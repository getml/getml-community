#include "decisiontrees/decisiontrees.hpp"

namespace autosql
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

DecisionTreeNode::DecisionTreeNode(
    bool _is_activated, AUTOSQL_INT _depth, const DecisionTreeImpl *_tree )
    : depth_( _depth ), is_activated_( _is_activated ), tree_( _tree ){};

// ----------------------------------------------------------------------------

void DecisionTreeNode::apply_by_categories_used(
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    if ( std::distance( _sample_container_begin, _sample_container_end ) == 0 )
        {
            return;
        }

    if ( apply_from_above() )
        {
            if ( is_activated_ )
                {
                    aggregation()->deactivate_samples_not_containing_categories(
                        categories_used_begin(),
                        categories_used_end(),
                        _sample_container_begin,
                        _sample_container_end );
                }
            else
                {
                    aggregation()->activate_samples_not_containing_categories(
                        categories_used_begin(),
                        categories_used_end(),
                        _sample_container_begin,
                        _sample_container_end );
                }
        }
    else
        {
            if ( is_activated_ )
                {
                    aggregation()->deactivate_samples_containing_categories(
                        categories_used_begin(),
                        categories_used_end(),
                        _sample_container_begin,
                        _sample_container_end );
                }
            else
                {
                    aggregation()->activate_samples_containing_categories(
                        categories_used_begin(),
                        categories_used_end(),
                        _sample_container_begin,
                        _sample_container_end );
                }
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::apply_by_categories_used_and_commit(
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    if ( std::distance( _sample_container_begin, _sample_container_end ) > 0 )
        {
            const auto index = containers::CategoryIndex(
                categories_used(),
                _sample_container_begin,
                _sample_container_end );

            if ( apply_from_above() )
                {
                    if ( is_activated_ )
                        {
                            aggregation()
                                ->deactivate_samples_not_containing_categories(
                                    categories_used_begin(),
                                    categories_used_end(),
                                    aggregations::Revert::not_at_all,
                                    index );
                        }
                    else
                        {
                            aggregation()
                                ->activate_samples_not_containing_categories(
                                    categories_used_begin(),
                                    categories_used_end(),
                                    aggregations::Revert::not_at_all,
                                    index );
                        }
                }
            else
                {
                    if ( is_activated_ )
                        {
                            aggregation()
                                ->deactivate_samples_containing_categories(
                                    categories_used_begin(),
                                    categories_used_end(),
                                    aggregations::Revert::not_at_all,
                                    index );
                        }
                    else
                        {
                            aggregation()
                                ->activate_samples_containing_categories(
                                    categories_used_begin(),
                                    categories_used_end(),
                                    aggregations::Revert::not_at_all,
                                    index );
                        }
                }
        }
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::vector<AUTOSQL_INT>>
DecisionTreeNode::calculate_categories(
    const AUTOSQL_SIZE _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    // ------------------------------------------------------------------------

    AUTOSQL_INT categories_begin = 0;
    AUTOSQL_INT categories_end = 0;

    // ------------------------------------------------------------------------
    // In distributed versions, it is possible that there are no sample sizes
    // left in this process rank. In that case we effectively pass plus infinity
    // to min and minus infinity to max, ensuring that they will not be the
    // chosen minimum or maximum.

    if ( std::distance( _sample_container_begin, _sample_container_end ) > 0 )
        {
            categories_begin =
                std::max( ( *_sample_container_begin )->categorical_value, 0 );
            categories_end = std::max(
                ( *( _sample_container_end - 1 ) )->categorical_value + 1, 0 );
        }
    else
        {
            categories_begin = std::numeric_limits<AUTOSQL_INT>::max();
            categories_end = 0;
        }

#ifdef AUTOSQL_PARALLEL

    reduce_min_max( categories_begin, categories_end );

#endif  // AUTOSQL_PARALLEL

    // ------------------------------------------------------------------------
    // There is a possibility that all critical values are NULL (signified by
    // -1) in all processes. This accounts for this edge case.

    if ( categories_begin >= categories_end )
        {
            return std::make_shared<std::vector<AUTOSQL_INT>>( 0 );
        }

    // ------------------------------------------------------------------------
    // Find unique categories (signified by a boolean vector). We cannot use the
    // actual boolean type, because bool is smaller than char and therefore the
    // all_reduce operator won't work. So we use std::int8_t instead.

    auto included =
        std::vector<std::int8_t>( categories_end - categories_begin, 0 );

    for ( auto it = _sample_container_begin; it != _sample_container_end; ++it )
        {
            if ( ( *it )->categorical_value < 0 )
                {
                    continue;
                }

            assert( ( *it )->categorical_value >= categories_begin );
            assert( ( *it )->categorical_value < categories_end );

            included[( *it )->categorical_value - categories_begin] = 1;
        }

#ifdef AUTOSQL_PARALLEL
    {
        auto global = std::vector<std::int8_t>( included.size() );

        AUTOSQL_PARALLEL_LIB::all_reduce(
            *comm(),                            // comm
            included.data(),                    // in_values
            categories_end - categories_begin,  // count
            global.data(),                      // out_values
            AUTOSQL_MAX_OP<std::int8_t>()        // op
        );

        comm()->barrier();

        included = std::move( global );
    }
#endif  // AUTOSQL_PARALLEL

    // ------------------------------------------------------------------------
    // Build vector.

    auto categories = std::make_shared<std::vector<AUTOSQL_INT>>( 0 );

    for ( AUTOSQL_INT i = 0; i < categories_end - categories_begin; ++i )
        {
            if ( included[i] == 1 )
                {
                    categories->push_back( categories_begin + i );
                }
        }

    // ------------------------------------------------------------------------

    return categories;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

containers::Matrix<AUTOSQL_FLOAT>
DecisionTreeNode::calculate_critical_values_discrete(
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    const AUTOSQL_SIZE _sample_size )
{
    // ---------------------------------------------------------------------------

    debug_message( "calculate_critical_values_discrete..." );

    AUTOSQL_FLOAT min = 0.0, max = 0.0;

    // ---------------------------------------------------------------------------
    // In distributed versions, it is possible that there are no sample sizes
    // left in this process rank. In that case we effectively pass plus infinity
    // to min and minus infinity to max, ensuring that they not will be the
    // chosen minimum or maximum.

    debug_message(
        "std::distance( ... ): " +
        std::to_string(
            std::distance( _sample_container_begin, _sample_container_end ) ) );

    if ( std::distance( _sample_container_begin, _sample_container_end ) > 0 )
        {
            min = std::floor( ( *_sample_container_begin )->numerical_value );
            max = std::ceil(
                ( *( _sample_container_end - 1 ) )->numerical_value );
        }
    else
        {
            min = std::numeric_limits<AUTOSQL_FLOAT>::max();
            max = std::numeric_limits<AUTOSQL_FLOAT>::lowest();
        }

#ifdef AUTOSQL_PARALLEL

    reduce_min_max( min, max );

#endif /* AUTOSQL_PARALLEL */

    // ---------------------------------------------------------------------------
    // There is a possibility that all critical values are NAN in all processes.
    // This accounts for this edge case.

    if ( min > max )
        {
            return containers::Matrix<AUTOSQL_FLOAT>( 0, 1 );
        }

    // ---------------------------------------------------------------------------

    AUTOSQL_INT num_critical_values = static_cast<AUTOSQL_INT>( max - min + 1 );

    debug_message(
        "num_critical_values: " + std::to_string( num_critical_values ) );

    containers::Matrix<AUTOSQL_FLOAT> critical_values( num_critical_values, 1 );

    for ( AUTOSQL_INT i = 0; i < num_critical_values; ++i )
        {
            critical_values[i] = min + static_cast<AUTOSQL_FLOAT>( i );
        }

    // ---------------------------------------------------------------------------

    debug_message( "calculate_critical_values_discrete...done" );

    return critical_values;

    // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

containers::Matrix<AUTOSQL_FLOAT>
DecisionTreeNode::calculate_critical_values_numerical(
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    const AUTOSQL_SIZE _sample_size )
{
    // ---------------------------------------------------------------------------

    debug_message( "calculate_critical_values_numerical..." );

    AUTOSQL_FLOAT min = 0.0, max = 0.0;

    // ---------------------------------------------------------------------------
    // In distributed versions, it is possible that there are no sample sizes
    // left in this process rank. In that case we effectively pass plus infinity
    // to min and minus infinity to max, ensuring that they not will be the
    // chosen minimum or maximum.

    if ( std::distance( _sample_container_begin, _sample_container_end ) > 0 )
        {
            min = ( *_sample_container_begin )->numerical_value;
            max = ( *( _sample_container_end - 1 ) )->numerical_value;
        }
    else
        {
            min = std::numeric_limits<AUTOSQL_FLOAT>::max();
            max = std::numeric_limits<AUTOSQL_FLOAT>::lowest();
        }

#ifdef AUTOSQL_PARALLEL

    reduce_min_max( min, max );

#endif /* AUTOSQL_PARALLEL */

    // ---------------------------------------------------------------------------
    // There is a possibility that all critical values are NAN in all processes.
    // This accounts for this edge case.

    if ( min > max )
        {
            debug_message(
                "calculate_critical_values_discrete...done (edge case)." );

            return containers::Matrix<AUTOSQL_FLOAT>( 0, 1 );
        }

    // ---------------------------------------------------------------------------

    AUTOSQL_INT num_critical_values =
        calculate_num_critical_values( _sample_size );

    AUTOSQL_FLOAT step_size =
        ( max - min ) / static_cast<AUTOSQL_FLOAT>( num_critical_values + 1 );

    containers::Matrix<AUTOSQL_FLOAT> critical_values( num_critical_values, 1 );

    for ( AUTOSQL_INT i = 0; i < num_critical_values; ++i )
        {
            critical_values[i] =
                min + static_cast<AUTOSQL_FLOAT>( i + 1 ) * step_size;
        }

    debug_message( "calculate_critical_values_discrete...done." );

    return critical_values;

    // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::commit(
    const descriptors::Split &_split,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    debug_message( "fit: Improvement possible..." );

    auto null_values_separator = identify_parameters(
        _split, _sample_container_begin, _sample_container_end );

    debug_message( "fit: Commit..." );

    aggregation()->commit();

    optimization_criterion()->commit();

    if ( depth_ < tree_->max_length_ )
        {
            debug_message( "fit: Max length not reached..." );

            spawn_child_nodes(
                _sample_container_begin,
                null_values_separator,
                _sample_container_end );
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::fit(
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    debug_message( "fit: Calculating sample size..." );

#ifdef AUTOSQL_PARALLEL

    const AUTOSQL_SIZE sample_size = reduce_sample_size(
        std::distance( _sample_container_begin, _sample_container_end ) );

#else  // AUTOSQL_PARALLEL

    const AUTOSQL_SIZE sample_size = static_cast<AUTOSQL_SIZE>(
        std::distance( _sample_container_begin, _sample_container_end ) );

#endif  // AUTOSQL_PARALLEL

    if ( sample_size == 0 ||
         static_cast<AUTOSQL_INT>( sample_size ) < tree_->min_num_samples_ * 2 )
        {
            return;
        }

    // The reason we add an additional 1 is that the apply_by_... functions
    // will add another line to the storage of the optimization_criterion,
    // because they reproduce all the steps undertaken by the maximum split. But
    // the split used in the end is ix_max.
    debug_message( "fit: Setting storage size..." );

    optimization_criterion()->set_storage_size( 1 );

    // ------------------------------------------------------------------------
    // Try imposing different conditions and measure the performance.

    std::vector<descriptors::Split> candidate_splits = {};

    try_conditions(
        sample_size,
        _sample_container_begin,
        _sample_container_end,
        candidate_splits );

    // ------------------------------------------------------------------------
    // Find maximum

    debug_message( "fit: Find maximum..." );

    // Find maximum
    AUTOSQL_INT ix_max = optimization_criterion()->find_maximum();

    const AUTOSQL_FLOAT max_value =
        optimization_criterion()->values_stored( ix_max );

    // ------------------------------------------------------------------------
    // DEBUG and parallel mode only: Make sure that the values_stored are
    // aligned!

#ifndef NDEBUG

#ifdef AUTOSQL_PARALLEL

    std::array<AUTOSQL_FLOAT, 2> values = {max_value,
                                          optimization_criterion()->value()};

    assert( std::get<0>( values ) == std::get<0>( values ) );

    assert( std::get<1>( values ) == std::get<1>( values ) );

    std::array<AUTOSQL_FLOAT, 2> global_values;

    AUTOSQL_PARALLEL_LIB::all_reduce(
        *comm(),                       // comm
        values.data(),                 // in_value
        2,                             // count
        global_values.data(),          // out_value
        AUTOSQL_MAX_OP<AUTOSQL_FLOAT>()  // op
    );

    comm()->barrier();

    assert( std::get<0>( values ) == std::get<0>( global_values ) );

    assert( std::get<1>( values ) == std::get<1>( global_values ) );

#endif  // AUTOSQL_PARALLEL

#endif  // NDEBUG

    // ------------------------------------------------------------------------
    // Imposing a condition is only necessary, if it actually improves the
    // optimization criterion

    if ( max_value >
         optimization_criterion()->value() + tree_->regularization_ + 1e-07 )
        {
            commit(
                candidate_splits[ix_max].deep_copy(),
                _sample_container_begin,
                _sample_container_end );
        }
    else
        {
            debug_message( "fit: No improvement possible..." );
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::fit_as_root(
    AUTOSQL_SAMPLE_CONTAINER::iterator _sample_container_begin,
    AUTOSQL_SAMPLE_CONTAINER::iterator _sample_container_end )
{
    debug_message( "fit_as_root..." );

    aggregation()->activate_all(
        true, _sample_container_begin, _sample_container_end );

    aggregation()->commit();

    optimization_criterion()->commit();

    if ( tree_->max_length_ > 0 )
        {
            fit( _sample_container_begin, _sample_container_end );
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::from_json_obj( const Poco::JSON::Object &_json_obj )
{
    is_activated_ = _json_obj.AUTOSQL_GET( "act_" );

    const bool imposes_condition = _json_obj.AUTOSQL_GET( "imp_" );

    if ( imposes_condition )
        {
            split_.reset( new descriptors::Split( _json_obj ) );

            if ( _json_obj.has( "sub1_" ) )
                {
                    if ( !_json_obj.has( "sub2_" ) )
                        {
                            std::invalid_argument(
                                "Error in JSON: Has 'sub1_', but not "
                                "'sub2_'!" );
                        }

                    child_node_greater_.reset(
                        new DecisionTreeNode( false, depth_ + 1, tree_ ) );

                    child_node_greater_->from_json_obj(
                        *_json_obj.AUTOSQL_GET_OBJECT( "sub1_" ) );

                    child_node_smaller_.reset(
                        new DecisionTreeNode( false, depth_ + 1, tree_ ) );

                    child_node_smaller_->from_json_obj(
                        *_json_obj.AUTOSQL_GET_OBJECT( "sub2_" ) );
                }
        }
}

// ----------------------------------------------------------------------------

std::string DecisionTreeNode::greater_or_not_equal_to(
    const std::string &_colname ) const
{
    if ( data_used() == DataUsed::same_unit_categorical )
        {
            return _colname;
        }

    std::stringstream sql;

    if ( categorical_data_used() )
        {
            sql << "( ";

            for ( auto it = categories_used_begin(); it < categories_used_end();
                  ++it )
                {
                    const auto category_used = *it;

                    assert( category_used >= 0 );
                    assert(
                        category_used <
                        static_cast<AUTOSQL_INT>( tree_->categories().size() ) );

                    if ( it != categories_used_begin() )
                        {
                            sql << " AND ";
                        }

                    sql << _colname;

                    sql << " != '";

                    sql << tree_->categories()[category_used];

                    sql << "'";
                }

            sql << " )";
        }
    else
        {
            sql << _colname;

            sql << " > ";

            sql << std::to_string( critical_value() );
        }

    return sql.str();
}

// ----------------------------------------------------------------------------

AUTOSQL_SAMPLE_ITERATOR DecisionTreeNode::identify_parameters(
    const descriptors::Split &_split,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    // --------------------------------------------------------------
    // Transfer parameters from split descriptor

    split_.reset( new descriptors::Split( _split ) );

    // --------------------------------------------------------------

    debug_message( "Identify parameters..." );

    // --------------------------------------------------------------
    // Restore the optimal split

    set_samples( _sample_container_begin, _sample_container_end );

    // --------------------------------------------------------------
    // Change stage of aggregation to optimal split

    auto null_values_separator = _sample_container_begin;

    if ( categorical_data_used() )
        {
            debug_message( "Identify_parameters: Sort.." );

            sort_by_categorical_value(
                _sample_container_begin, _sample_container_end );

            debug_message( "Identify_parameters: apply..." );

            apply_by_categories_used_and_commit(
                _sample_container_begin, _sample_container_end );
        }
    else
        {
            // --------------------------------------------------------------

            containers::Matrix<AUTOSQL_FLOAT> critical_values( 1, 1 );

            critical_values[0] = critical_value();

            const bool null_values_to_beginning =
                ( apply_from_above() != is_activated_ );

            // --------------------------------------------------------------

            debug_message( "Identify_parameters: Sort.." );

            null_values_separator = separate_null_values(
                _sample_container_begin,
                _sample_container_end,
                null_values_to_beginning );

            // --------------------------------------------------------------

            if ( null_values_to_beginning )
                {
                    sort_by_numerical_value(
                        null_values_separator, _sample_container_end );

                    debug_message( "Identify_parameters: apply..." );

                    if ( is_activated_ )
                        {
                            aggregation()->deactivate_samples_with_null_values(
                                _sample_container_begin,
                                null_values_separator );
                        }

                    apply_by_critical_value(
                        critical_values,
                        null_values_separator,
                        _sample_container_end );
                }
            else
                {
                    sort_by_numerical_value(
                        _sample_container_begin, null_values_separator );

                    debug_message( "Identify_parameters: apply..." );

                    if ( is_activated_ )
                        {
                            aggregation()->deactivate_samples_with_null_values(
                                null_values_separator, _sample_container_end );
                        }

                    apply_by_critical_value(
                        critical_values,
                        _sample_container_begin,
                        null_values_separator );
                }

            // --------------------------------------------------------------
        }

    // --------------------------------------------------------------

    return null_values_separator;
}

// ----------------------------------------------------------------------------

#ifdef AUTOSQL_PARALLEL

AUTOSQL_SIZE DecisionTreeNode::reduce_sample_size( AUTOSQL_SIZE _sample_size )
{
    AUTOSQL_SIZE global_sample_size = 0;

    AUTOSQL_PARALLEL_LIB::all_reduce(
        *comm(),                   // comm
        _sample_size,              // in_value
        global_sample_size,        // out_value
        std::plus<AUTOSQL_FLOAT>()  // op
    );

    comm()->barrier();

    return global_sample_size;
}

#endif  // AUTOSQL_PARALLEL

// ----------------------------------------------------------------------------

AUTOSQL_SAMPLE_ITERATOR DecisionTreeNode::separate_null_values(
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    bool _null_values_to_beginning )
{
    auto is_null = []( Sample *sample ) {
        return sample->numerical_value != sample->numerical_value;
    };

    auto is_not_null = []( Sample *sample ) {
        return sample->numerical_value == sample->numerical_value;
    };

    if ( _null_values_to_beginning )
        {
            if ( std::is_partitioned(
                     _sample_container_begin, _sample_container_end, is_null ) )

                {
                    return std::partition_point(
                        _sample_container_begin,
                        _sample_container_end,
                        is_null );
                }
            else
                {
                    return std::stable_partition(
                        _sample_container_begin,
                        _sample_container_end,
                        is_null );
                }
        }
    else
        {
            if ( std::is_partitioned(
                     _sample_container_begin,
                     _sample_container_end,
                     is_not_null ) )

                {
                    return std::partition_point(
                        _sample_container_begin,
                        _sample_container_end,
                        is_not_null );
                }
            else
                {
                    return std::stable_partition(
                        _sample_container_begin,
                        _sample_container_end,
                        is_not_null );
                }
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::set_samples(
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    switch ( data_used() )
        {
            case DataUsed::same_unit_categorical:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->categorical_value =
                            get_same_unit_categorical( *it, column_used() );
                    }

                break;

            case DataUsed::same_unit_discrete:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->numerical_value =
                            get_same_unit_discrete( *it, column_used() );
                    }

                break;

            case DataUsed::same_unit_numerical:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->numerical_value =
                            get_same_unit_numerical( *it, column_used() );
                    }

                break;

            case DataUsed::x_perip_categorical:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->categorical_value =
                            get_x_perip_categorical( *it, column_used() );
                    }

                break;

            case DataUsed::x_perip_numerical:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->numerical_value =
                            get_x_perip_numerical( *it, column_used() );
                    }

                break;

            case DataUsed::x_perip_discrete:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->numerical_value =
                            get_x_perip_discrete( *it, column_used() );
                    }

                break;

            case DataUsed::x_popul_categorical:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->categorical_value =
                            get_x_popul_categorical( *it, column_used() );
                    }

                break;

            case DataUsed::x_popul_numerical:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->numerical_value =
                            get_x_popul_numerical( *it, column_used() );
                    }

                break;

            case DataUsed::x_popul_discrete:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->numerical_value =
                            get_x_popul_discrete( *it, column_used() );
                    }

                break;

            case DataUsed::x_subfeature:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->numerical_value =
                            get_x_subfeature( *it, column_used() );
                    }

                break;

            case DataUsed::time_stamps_diff:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->numerical_value = get_time_stamps_diff( *it );
                    }

                break;

            default:

                assert( false && "Unknown DataUsed!" );
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::sort_by_categorical_value(
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    auto compare_op = []( const Sample *sample1, const Sample *sample2 ) {
        return sample1->categorical_value < sample2->categorical_value;
    };

    std::sort( _sample_container_begin, _sample_container_end, compare_op );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::sort_by_numerical_value(
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    auto compare_op = []( const Sample *sample1, const Sample *sample2 ) {
        return sample1->numerical_value < sample2->numerical_value;
    };

    std::sort( _sample_container_begin, _sample_container_end, compare_op );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::source_importances(
    const AUTOSQL_FLOAT _factor, descriptors::SourceImportances &_importances )
{
    if ( split_ )
        {
            tree_->source_importances(
                data_used(),
                column_used(),
                _factor,
                _importances.condition_imp_ );
        }

    if ( child_node_greater_ )
        {
            child_node_greater_->source_importances(
                _factor * 0.5, _importances );

            child_node_smaller_->source_importances(
                _factor * 0.5, _importances );
        }
}

// ----------------------------------------------------------------------------

std::string DecisionTreeNode::smaller_or_equal_to(
    const std::string &_colname ) const
{
    if ( data_used() == DataUsed::same_unit_categorical )
        {
            return _colname;
        }

    std::stringstream sql;

    if ( categorical_data_used() )
        {
            sql << "( ";

            for ( auto it = categories_used_begin(); it < categories_used_end();
                  ++it )
                {
                    const auto category_used = *it;

                    assert( category_used >= 0 );
                    assert(
                        category_used <
                        static_cast<AUTOSQL_INT>( tree_->categories().size() ) );

                    if ( it != categories_used_begin() )
                        {
                            sql << " OR ";
                        }

                    sql << _colname;

                    sql << " = '";

                    sql << tree_->categories()[category_used];

                    sql << "'";
                }

            sql << " )";
        }
    else
        {
            sql << _colname;

            sql << " <= ";

            sql << std::to_string( critical_value() );
        }

    return sql.str();
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::spawn_child_nodes(
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _null_values_separator,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    // -------------------------------------------------------------------------

    auto it = _sample_container_begin;

    const bool child_node_greater_is_activated =
        ( apply_from_above() != is_activated_ );

    // If child_node_greater_is_activated, then the NULL samples
    // are allocated to the beginning, since they must always be
    // deactivated.
    if ( child_node_greater_is_activated )
        {
            it = _null_values_separator;
        }

    // -------------------------------------------------------------------------

    if ( categorical_data_used() )
        {
            // The samples where the category equals any of categories_used()
            // are copied into samples_smaller. This makes sense, because
            // for the numerical values, samples_smaller contains all values
            // <= critical_value()

            const auto is_contained = [this]( const Sample *_sample ) {
                return std::any_of(
                    categories_used_begin(),
                    categories_used_end(),
                    [_sample]( AUTOSQL_INT cat ) {
                        return cat == _sample->categorical_value;
                    } );
            };

            it = std::partition(
                _sample_container_begin, _sample_container_end, is_contained );
        }
    else
        {
            while ( it < _sample_container_end )
                {
                    const AUTOSQL_FLOAT val = ( *it )->numerical_value;

                    // If val != val, then all samples but the NULL samples
                    // are activated. This is a corner case that can only
                    // happen when the user has defined a min_num_samples
                    // of 0.
                    if ( val > critical_value() || val != val )
                        {
                            break;
                        }

                    ++it;
                }
        }

    // -------------------------------------------------------------------------
    // Set up and fit child_node_greater_

    child_node_greater_.reset( new DecisionTreeNode(
        child_node_greater_is_activated,  // _is_activated
        depth_ + 1,                       // _depth
        tree_                             // _tree
        ) );

    child_node_greater_->fit( it, _sample_container_end );

    // -------------------------------------------------------------------------
    // Set up and fit child_node_smaller_

    child_node_smaller_.reset( new DecisionTreeNode(
        !( child_node_greater_is_activated ),  // _is_activated
        depth_ + 1,                            // _depth
        tree_                                  // _tree
        ) );

    child_node_smaller_->fit( _sample_container_begin, it );

    // -------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DecisionTreeNode::to_json_obj()
{
    Poco::JSON::Object obj;

    obj.set( "act_", is_activated_ );

    obj.set( "imp_", ( split_ && true ) );

    if ( split_ )
        {
            obj.set( "app_", apply_from_above() );

            obj.set(
                "categories_used_",
                JSON::vector_to_array( categories_used() ) );

            obj.set( "critical_value_", critical_value() );

            obj.set( "column_used_", column_used() );

            obj.set( "data_used_", JSON::data_used_to_int( data_used() ) );

            if ( child_node_greater_ )
                {
                    obj.set( "sub1_", child_node_greater_->to_json_obj() );

                    obj.set( "sub2_", child_node_smaller_->to_json_obj() );
                }
        }

    return obj;
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::to_sql(
    const std::string &_feature_num,
    std::vector<std::string> &_conditions,
    std::string _sql ) const
{
    if ( child_node_greater_ )
        {
            if ( _sql.size() > 0 )
                {
                    _sql.append( " AND " );
                }

            // Append conditions greater
            std::string sql_greater = _sql;

            // There is a difference between colname_greater and colname_smaller
            // because of the same units.
            const auto colname_greater = tree_->get_colname(
                _feature_num, data_used(), column_used(), false );

            sql_greater.append( greater_or_not_equal_to( colname_greater ) );

            child_node_greater_->to_sql(
                _feature_num, _conditions, sql_greater );

            // Append conditions smaller
            std::string sql_smaller = _sql;

            // There is a difference between colname_greater and colname_smaller
            // because of the same units.
            const auto colname_smaller = tree_->get_colname(
                _feature_num, data_used(), column_used(), true );

            sql_smaller.append( smaller_or_equal_to( colname_smaller ) );

            child_node_smaller_->to_sql(
                _feature_num, _conditions, sql_smaller );
        }
    else
        {
            if ( split_ )
                {
                    if ( _sql.size() > 0 )
                        {
                            _sql.append( " AND " );
                        }

                    const auto colname = tree_->get_colname(
                        _feature_num,
                        data_used(),
                        column_used(),
                        apply_from_above() == is_activated_ );

                    if ( apply_from_above() != is_activated_ )
                        {
                            _sql.append( greater_or_not_equal_to( colname ) );
                        }
                    else
                        {
                            _sql.append( smaller_or_equal_to( colname ) );
                        }

                    _conditions.push_back( _sql );
                }
            else
                {
                    if ( is_activated_ && _sql != "" )
                        {
                            _conditions.push_back( _sql );
                        }
                }
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::to_monitor(
    const std::string &_feature_num,
    Poco::JSON::Array _node,
    Poco::JSON::Array &_conditions ) const
{
    if ( child_node_greater_ )
        {
            // -----------------------------------------------
            // Append conditions greater

            {
                std::string condition = "";

                const auto colname = tree_->get_colname(
                    _feature_num, data_used(), column_used(), false );

                condition.append( greater_or_not_equal_to( colname ) );

                _node.add( condition );

                child_node_greater_.get()->to_monitor(
                    _feature_num, _node, _conditions );
            }

            // -----------------------------------------------
            // Append conditions smaller

            {
                std::string condition = "";

                const auto colname = tree_->get_colname(
                    _feature_num, data_used(), column_used(), false );

                condition.append( smaller_or_equal_to( colname ) );

                _node.add( condition );

                child_node_greater_.get()->to_monitor(
                    _feature_num, _node, _conditions );
            }

            // -----------------------------------------------
        }
    else
        {
            if ( split_ )
                {
                    std::string condition = "";

                    const auto colname = tree_->get_colname(
                        _feature_num,
                        data_used(),
                        column_used(),
                        apply_from_above() == is_activated_ );

                    if ( apply_from_above() != is_activated_ )
                        {
                            condition.append(
                                greater_or_not_equal_to( colname ) );
                        }
                    else
                        {
                            condition.append( smaller_or_equal_to( colname ) );
                        }

                    _node.add( condition );
                }

            _conditions.add( _node );
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::transform(
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    // -----------------------------------------------------------

    // Some nodes do not impose a condition at all. It that case they
    // cannot have any children either and there is nothing left to do.
    if ( !split_ )
        {
            debug_message( "transform: Does not impose condition..." );

            return;
        }

    // -----------------------------------------------------------

    debug_message( "transform: Setting samples..." );

    set_samples( _sample_container_begin, _sample_container_end );

    // -----------------------------------------------------------

    debug_message( "transform: Applying condition..." );

    if ( categorical_data_used() )
        {
            apply_by_categories_used(
                _sample_container_begin, _sample_container_end );
        }
    else
        {
            apply_by_critical_value(
                critical_value(),
                _sample_container_begin,
                _sample_container_end );
        }

    // -----------------------------------------------------------
    // If the node has child nodes, use them to transform as well

    auto it = _sample_container_begin;

    if ( child_node_greater_ )
        {
            debug_message( "transform: Has child..." );

            // ---------------------------------------------------------

            debug_message( "transform: Partitioning by value.." );

            if ( categorical_data_used() )
                {
                    const auto is_contained = [this]( const Sample *_sample ) {
                        return std::any_of(
                            categories_used_begin(),
                            categories_used_end(),
                            [_sample]( AUTOSQL_INT cat ) {
                                return cat == _sample->categorical_value;
                            } );
                    };

                    it = std::partition(
                        _sample_container_begin,
                        _sample_container_end,
                        is_contained );
                }
            else
                {
                    // ---------------------------------------------------------

                    debug_message( "transform: Separating null values..." );

                    const bool null_values_to_beginning =
                        ( apply_from_above() != is_activated_ );

                    auto null_values_separator = separate_null_values(
                        _sample_container_begin,
                        _sample_container_end,
                        null_values_to_beginning );

                    // ---------------------------------------------------------

                    debug_message(
                        "transform: Separating by critical values..." );

                    if ( null_values_to_beginning )
                        {
                            it = std::partition(
                                null_values_separator,
                                _sample_container_end,
                                [this]( const Sample *_sample ) {
                                    return _sample->numerical_value <=
                                           critical_value();
                                } );
                        }
                    else
                        {
                            it = std::partition(
                                _sample_container_begin,
                                null_values_separator,
                                [this]( const Sample *_sample ) {
                                    return _sample->numerical_value <=
                                           critical_value();
                                } );
                        }

                    // ---------------------------------------------------------
                }

            // ---------------------------------------------------------

            child_node_smaller_->transform( _sample_container_begin, it );

            child_node_greater_->transform( it, _sample_container_end );

            // ---------------------------------------------------------
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_categorical_peripheral(
    const AUTOSQL_SIZE _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> &_candidate_splits )
{
    debug_message( "try_categorical_peripheral..." );

    for ( AUTOSQL_INT col = 0; col < tree_->peripheral_.categorical().ncols();
          ++col )
        {
            if ( tree_->peripheral_.categorical().unit( col ).find(
                     "comparison only" ) != std::string::npos )
                {
                    continue;
                }

            if ( skip_condition() )
                {
                    continue;
                }

            for ( auto it = _sample_container_begin;
                  it != _sample_container_end;
                  ++it )
                {
                    ( *it )->categorical_value =
                        get_x_perip_categorical( *it, col );
                }

            try_categorical_values(
                col,
                DataUsed::x_perip_categorical,
                _sample_container_begin,
                _sample_container_end,
                _sample_size,
                _candidate_splits );
        }

    debug_message( "try_categorical_peripheral...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_categorical_population(
    const AUTOSQL_SIZE _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> &_candidate_splits )
{
    debug_message( "try_categorical_population..." );

    for ( AUTOSQL_INT col = 0;
          col < tree_->population_.df().categorical().ncols();
          ++col )
        {
            if ( tree_->population_.df().categorical().unit( col ).find(
                     "comparison only" ) != std::string::npos )
                {
                    continue;
                }

            if ( skip_condition() )
                {
                    continue;
                }

            for ( auto it = _sample_container_begin;
                  it != _sample_container_end;
                  ++it )
                {
                    ( *it )->categorical_value =
                        get_x_popul_categorical( *it, col );
                }

            try_categorical_values(
                col,
                DataUsed::x_popul_categorical,
                _sample_container_begin,
                _sample_container_end,
                _sample_size,
                _candidate_splits );
        }

    debug_message( "try_categorical_population...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_discrete_peripheral(
    const AUTOSQL_SIZE _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> &_candidate_splits )
{
    debug_message( "try_discrete_peripheral..." );

    for ( AUTOSQL_INT col = 0; col < tree_->peripheral_.discrete().ncols();
          ++col )
        {
            if ( tree_->peripheral_.discrete().unit( col ).find(
                     "comparison only" ) != std::string::npos )
                {
                    continue;
                }

            if ( skip_condition() )
                {
                    continue;
                }

            for ( auto it = _sample_container_begin;
                  it != _sample_container_end;
                  ++it )
                {
                    ( *it )->numerical_value = get_x_perip_discrete( *it, col );
                }

            try_discrete_values(
                col,
                DataUsed::x_perip_discrete,
                _sample_container_begin,
                _sample_container_end,
                _sample_size,
                _candidate_splits );
        }

    debug_message( "try_discrete_peripheral...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_discrete_population(
    const AUTOSQL_SIZE _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> &_candidate_splits )
{
    debug_message( "try_discrete_population..." );

    for ( AUTOSQL_INT col = 0; col < tree_->population_.df().discrete().ncols();
          ++col )
        {
            if ( tree_->population_.df().discrete().unit( col ).find(
                     "comparison only" ) != std::string::npos )
                {
                    continue;
                }

            if ( skip_condition() )
                {
                    continue;
                }

            for ( auto it = _sample_container_begin;
                  it != _sample_container_end;
                  ++it )
                {
                    ( *it )->numerical_value = get_x_popul_discrete( *it, col );
                }

            try_discrete_values(
                col,
                DataUsed::x_popul_discrete,
                _sample_container_begin,
                _sample_container_end,
                _sample_size,
                _candidate_splits );
        }

    debug_message( "try_discrete_population...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_numerical_peripheral(
    const AUTOSQL_SIZE _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> &_candidate_splits )
{
    debug_message( "try_numerical_peripheral..." );

    for ( AUTOSQL_INT col = 0; col < tree_->peripheral_.numerical().ncols();
          ++col )
        {
            if ( tree_->peripheral_.numerical().unit( col ).find(
                     "comparison only" ) != std::string::npos )
                {
                    continue;
                }

            if ( skip_condition() )
                {
                    continue;
                }

            for ( auto it = _sample_container_begin;
                  it != _sample_container_end;
                  ++it )
                {
                    ( *it )->numerical_value =
                        get_x_perip_numerical( *it, col );
                }

            try_numerical_values(
                col,
                DataUsed::x_perip_numerical,
                _sample_container_begin,
                _sample_container_end,
                _sample_size,
                _candidate_splits );
        }

    debug_message( "try_numerical_peripheral...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_numerical_population(
    const AUTOSQL_SIZE _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> &_candidate_splits )
{
    debug_message( "try_numerical_population..." );

    for ( AUTOSQL_INT col = 0; col < tree_->population_.df().numerical().ncols();
          ++col )
        {
            if ( tree_->population_.df().numerical().unit( col ).find(
                     "comparison only" ) != std::string::npos )
                {
                    continue;
                }

            if ( skip_condition() )
                {
                    continue;
                }

            for ( auto it = _sample_container_begin;
                  it != _sample_container_end;
                  ++it )
                {
                    ( *it )->numerical_value =
                        get_x_popul_numerical( *it, col );
                }

            try_numerical_values(
                col,
                DataUsed::x_popul_numerical,
                _sample_container_begin,
                _sample_container_end,
                _sample_size,
                _candidate_splits );
        }

    debug_message( "try_numerical_population...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_categorical_values(
    const AUTOSQL_INT _column_used,
    const DataUsed _data_used,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    const AUTOSQL_SIZE _sample_size,
    std::vector<descriptors::Split> &_candidate_splits )
{
    // -----------------------------------------------------------------------

    sort_by_categorical_value( _sample_container_begin, _sample_container_end );

    const auto categories = calculate_categories(
        _sample_size, _sample_container_begin, _sample_container_end );

    const auto index = containers::CategoryIndex(
        *categories, _sample_container_begin, _sample_container_end );

    const auto num_categories = static_cast<AUTOSQL_INT>( categories->size() );

    // -----------------------------------------------------------------------
    // Add new splits to the candidate splits
    // The samples where the category equals category_used_
    // are copied into samples_smaller. This makes sense, because
    // for the numerical values, samples_smaller contains all values
    // <= critical_value().
    // Because we first try the samples containing a category, that means
    // the change must be applied from below, so apply_from_above is
    // first false and then true.

    for ( auto cat = categories->cbegin(); cat < categories->cend(); ++cat )
        {
            _candidate_splits.push_back( descriptors::Split(
                false,         // _apply_from_above
                categories,    // _categories_used
                cat,           // _categories_used_begin
                cat + 1,       // _categories_used_end
                _column_used,  // _column_used
                _data_used     // _data_used
                ) );
        }

    for ( auto cat = categories->cbegin(); cat < categories->cend(); ++cat )
        {
            _candidate_splits.push_back( descriptors::Split(
                true,          // _apply_from_above
                categories,    // _categories_used
                cat,           // _categories_used_begin
                cat + 1,       // _categories_used_end
                _column_used,  // _column_used
                _data_used     // _data_used
                ) );
        }

    // -----------------------------------------------------------------------
    // Extend the storage size.

    optimization_criterion()->extend_storage_size( num_categories * 2 );

    // -----------------------------------------------------------------------
    // Try individual categories.
    //
    // It is possible that std::distance( _sample_container_begin,
    // _sample_container_end ) is zero, when we are using the distributed
    // version. In that case we want this process until this point, because
    // calculate_critical_values_numerical contains a barrier and we want to
    // avoid a deadlock.

    if ( std::distance( _sample_container_begin, _sample_container_end ) == 0 )
        {
            for ( size_t i = 0; i < categories->size() * 2; ++i )
                {
                    optimization_criterion()->store_current_stage( 0.0, 0.0 );
                }
        }
    else
        {
            // -----------------------------------------------------------------------
            // Try applying all aggregation to all samples that
            // contain a certain category.

            if ( is_activated_ )
                {
                    aggregation()->deactivate_samples_containing_categories(
                        categories->cbegin(),
                        categories->cend(),
                        aggregations::Revert::after_each_category,
                        index );
                }
            else
                {
                    aggregation()->activate_samples_containing_categories(
                        categories->cbegin(),
                        categories->cend(),
                        aggregations::Revert::after_each_category,
                        index );
                }

            // -----------------------------------------------------------------------
            // Try applying all aggregation to all samples that DO NOT
            // contain a certain category

            if ( is_activated_ )
                {
                    aggregation()->deactivate_samples_not_containing_categories(
                        categories->cbegin(),
                        categories->cend(),
                        aggregations::Revert::after_each_category,
                        index );
                }
            else
                {
                    aggregation()->activate_samples_not_containing_categories(
                        categories->cbegin(),
                        categories->cend(),
                        aggregations::Revert::after_each_category,
                        index );
                }
        }

    // -----------------------------------------------------------------------
    // If there are only two categories, trying combined categories does not
    // make any sense.

    if ( categories->size() < 3 || !tree_->allow_sets_ )
        {
            return;
        }

    // -----------------------------------------------------------------------
    // Produce sorted_by_containing.

    const auto storage_ix = optimization_criterion()->storage_ix();

    auto sorted_by_containing =
        std::make_shared<std::vector<AUTOSQL_INT>>( num_categories );

    {
        const auto indices = optimization_criterion()->argsort(
            storage_ix - num_categories * 2, storage_ix - num_categories );

        assert( indices.size() == categories->size() );

        for ( size_t i = 0; i < indices.size(); ++i )
            {
                assert( indices[i] >= 0 );
                assert( indices[i] < num_categories );

                ( *sorted_by_containing )[i] = ( *categories )[indices[i]];
            }
    }

    // -----------------------------------------------------------------------
    // Produce sorted_by_not_containing.

    auto sorted_by_not_containing =
        std::make_shared<std::vector<AUTOSQL_INT>>( num_categories );

    {
        const auto indices = optimization_criterion()->argsort(
            storage_ix - num_categories, storage_ix );

        assert( indices.size() == categories->size() );

        for ( size_t i = 0; i < indices.size(); ++i )
            {
                assert( indices[i] >= 0 );
                assert( indices[i] < num_categories );

                ( *sorted_by_not_containing )[i] = ( *categories )[indices[i]];
            }
    }

    // -----------------------------------------------------------------------
    // Add new splits to the candidate splits.

    for ( auto cat = sorted_by_containing->cbegin();
          cat < sorted_by_containing->cbegin() + num_categories / 2;
          ++cat )
        {
            _candidate_splits.push_back( descriptors::Split(
                false,                           // _apply_from_above
                sorted_by_containing,            // _categories_used
                sorted_by_containing->cbegin(),  // _categories_used_begin
                cat + 1,                         // _categories_used_end
                _column_used,                    // _column_used
                _data_used                       // _data_used
                ) );
        }

    for ( auto cat = sorted_by_containing->cbegin() + num_categories / 2;
          cat < sorted_by_containing->cend();
          ++cat )
        {
            _candidate_splits.push_back( descriptors::Split(
                true,                          // _apply_from_above
                sorted_by_containing,          // _categories_used
                cat + 1,                       // _categories_used_begin
                sorted_by_containing->cend(),  // _categories_used_end
                _column_used,                  // _column_used
                _data_used                     // _data_used
                ) );
        }

    for ( auto cat = sorted_by_not_containing->cbegin();
          cat < sorted_by_not_containing->cbegin() + num_categories / 2;
          ++cat )
        {
            _candidate_splits.push_back( descriptors::Split(
                true,                                // _apply_from_above
                sorted_by_not_containing,            // _categories_used
                sorted_by_not_containing->cbegin(),  // _categories_used_begin
                cat + 1,                             // _categories_used_end
                _column_used,                        // _column_used
                _data_used                           // _data_used
                ) );
        }

    for ( auto cat = sorted_by_not_containing->cbegin() + num_categories / 2;
          cat < sorted_by_not_containing->cend();
          ++cat )
        {
            _candidate_splits.push_back( descriptors::Split(
                false,                             // _apply_from_above
                sorted_by_not_containing,          // _categories_used
                cat + 1,                           // _categories_used_begin
                sorted_by_not_containing->cend(),  // _categories_used_end
                _column_used,                      // _column_used
                _data_used                         // _data_used
                ) );
        }

    // -----------------------------------------------------------------------
    // Extend the storage size.

    optimization_criterion()->extend_storage_size( categories->size() * 2 );

    // -----------------------------------------------------------------------
    // Try combined categories.

    if ( std::distance( _sample_container_begin, _sample_container_end ) == 0 )
        {
            for ( size_t i = 0; i < categories->size() * 2; ++i )
                {
                    optimization_criterion()->store_current_stage( 0.0, 0.0 );
                }
        }
    else
        {
            //-----------------------------------------------------------------
            // Try applying all aggregation to all samples that
            // contain a certain category.

            if ( is_activated_ )
                {
                    aggregation()->deactivate_samples_containing_categories(
                        sorted_by_containing->cbegin(),
                        sorted_by_containing->cend(),
                        aggregations::Revert::after_all_categories,
                        index );
                }
            else
                {
                    aggregation()->activate_samples_containing_categories(
                        sorted_by_containing->cbegin(),
                        sorted_by_containing->cend(),
                        aggregations::Revert::after_all_categories,
                        index );
                }

            // -------------------------------------------------------------------
            // Try applying all aggregation to all samples that DO NOT
            // contain a certain category

            if ( is_activated_ )
                {
                    aggregation()->deactivate_samples_not_containing_categories(
                        sorted_by_not_containing->cbegin(),
                        sorted_by_not_containing->cend(),
                        aggregations::Revert::after_all_categories,
                        index );
                }
            else
                {
                    aggregation()->activate_samples_not_containing_categories(
                        sorted_by_not_containing->cbegin(),
                        sorted_by_not_containing->cend(),
                        aggregations::Revert::after_all_categories,
                        index );
                }
        }

    // -----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_conditions(
    const AUTOSQL_SIZE _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> &_candidate_splits )
{
    try_same_units_categorical(
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_same_units_discrete(
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_same_units_numerical(
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_categorical_peripheral(
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_discrete_peripheral(
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_numerical_peripheral(
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_categorical_population(
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_discrete_population(
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_numerical_population(
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_subfeatures(
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_time_stamps_diff(
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_discrete_values(
    const AUTOSQL_INT _column_used,
    const DataUsed _data_used,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    const AUTOSQL_SIZE _sample_size,
    std::vector<descriptors::Split> &_candidate_splits )
{
    // -----------------------------------------------------------------------

    debug_message( "try_discrete_values..." );

    // -----------------------------------------------------------------------

    AUTOSQL_SAMPLE_ITERATOR null_values_separator =
        separate_null_values( _sample_container_begin, _sample_container_end );

    sort_by_numerical_value( null_values_separator, _sample_container_end );

    auto critical_values = calculate_critical_values_discrete(
        null_values_separator, _sample_container_end, _sample_size );

    // -----------------------------------------------------------------------

    try_non_categorical_values(
        _column_used,
        _data_used,
        _sample_container_begin,
        null_values_separator,
        _sample_container_end,
        _sample_size,
        critical_values,
        _candidate_splits );

    // -----------------------------------------------------------------------

    debug_message( "try_discrete_values...done." );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_non_categorical_values(
    const AUTOSQL_INT _column_used,
    const DataUsed _data_used,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _null_values_separator,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    const AUTOSQL_SIZE _sample_size,
    containers::Matrix<AUTOSQL_FLOAT> &_critical_values,
    std::vector<descriptors::Split> &_candidate_splits )
{
    // -----------------------------------------------------------------------

    debug_message( "try_non_categorical_values..." );

    // -----------------------------------------------------------------------
    // Extend the storage size

    debug_message( "try_non_categorical_values: Extend storage." );

    optimization_criterion()->extend_storage_size(
        _critical_values.nrows() * 2 );

    // -----------------------------------------------------------------------
    // Add new splits to the candidate splits

    debug_message( "try_non_categorical_values: Add new splits." );

    for ( AUTOSQL_INT i = 0; i < _critical_values.nrows(); ++i )
        {
            _candidate_splits.push_back( descriptors::Split(
                true,
                _critical_values.end()[-i - 1],
                _column_used,
                _data_used ) );
        }

    for ( auto &critical_value : _critical_values )
        {
            _candidate_splits.push_back( descriptors::Split(
                false, critical_value, _column_used, _data_used ) );
        }

    // -----------------------------------------------------------------------
    // If this is an activated node, we need to deactivate all samples for
    // which the numerical value is NULL

    debug_message( "try_non_categorical_values: Handle NULL." );

    if ( is_activated_ )
        {
            aggregation()->deactivate_samples_with_null_values(
                _sample_container_begin, _null_values_separator );
        }

    // -----------------------------------------------------------------------
    // It is possible that std::distance( _sample_container_begin,
    // _sample_container_end ) is zero, when we are using the distributed
    // version. In that case we want this process to continue until this point,
    // because calculate_critical_values_numerical and
    // calculate_critical_values_discrete contain a barriers and we want to
    // avoid a livelock.

    if ( std::distance( _null_values_separator, _sample_container_end ) == 0 )
        {
            for ( AUTOSQL_INT i = 0; i < _critical_values.nrows() * 2; ++i )
                {
                    aggregation()
                        ->update_optimization_criterion_and_clear_updates_current(
                            0.0,  // _num_samples_smaller
                            0.0   // _num_samples_greater
                        );
                }

            aggregation()->revert_to_commit();

            optimization_criterion()->revert_to_commit();

            return;
        }

    // -----------------------------------------------------------------------
    // Try applying from above

    debug_message( "try_non_categorical_values: Apply from above..." );

    // Apply changes and store resulting value of optimization criterion
    if ( is_activated_ )
        {
            aggregation()->deactivate_samples_from_above(
                _critical_values,
                _null_values_separator,
                _sample_container_end );
        }
    else
        {
            aggregation()->activate_samples_from_above(
                _critical_values,
                _null_values_separator,
                _sample_container_end );
        }

    // Revert to original situation
    aggregation()->revert_to_commit();

    optimization_criterion()->revert_to_commit();

    // -----------------------------------------------------------------------
    // If this is an activated node, we need to deactivate all samples for
    // which the numerical value is NULL. We need to do this again, because
    // all of the changes would have been reverted by revert_to_commit().

    if ( is_activated_ )
        {
            aggregation()->deactivate_samples_with_null_values(
                _sample_container_begin, _null_values_separator );
        }

    // -----------------------------------------------------------------------
    // Try applying from below

    debug_message( "try_non_categorical_values: Apply from below..." );

    // Apply changes and store resulting value of optimization criterion
    if ( is_activated_ )
        {
            aggregation()->deactivate_samples_from_below(
                _critical_values,
                _null_values_separator,
                _sample_container_end );
        }
    else
        {
            aggregation()->activate_samples_from_below(
                _critical_values,
                _null_values_separator,
                _sample_container_end );
        }

    // -----------------------------------------------------------------------
    // Revert to original situation

    debug_message( "try_non_categorical_values: Revert..." );

    aggregation()->revert_to_commit();

    optimization_criterion()->revert_to_commit();

    // -----------------------------------------------------------------------

    debug_message( "try_non_categorical_values...done." );

    // -----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_numerical_values(
    const AUTOSQL_INT _column_used,
    const DataUsed _data_used,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    const AUTOSQL_SIZE _sample_size,
    std::vector<descriptors::Split> &_candidate_splits )
{
    // -----------------------------------------------------------------------

    AUTOSQL_SAMPLE_ITERATOR null_values_separator =
        separate_null_values( _sample_container_begin, _sample_container_end );

    sort_by_numerical_value( null_values_separator, _sample_container_end );

    auto critical_values = calculate_critical_values_numerical(
        null_values_separator, _sample_container_end, _sample_size );

    // -----------------------------------------------------------------------

    try_non_categorical_values(
        _column_used,
        _data_used,
        _sample_container_begin,
        null_values_separator,
        _sample_container_end,
        _sample_size,
        critical_values,
        _candidate_splits );

    // -----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_same_units_categorical(
    const AUTOSQL_SIZE _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> &_candidate_splits )
{
    debug_message( "try_same_units_categorical..." );

    for ( AUTOSQL_INT col = 0; col < same_units_categorical().size(); ++col )
        {
            if ( skip_condition() )
                {
                    continue;
                }

            for ( auto it = _sample_container_begin;
                  it != _sample_container_end;
                  ++it )
                {
                    ( *it )->categorical_value =
                        get_same_unit_categorical( *it, col );
                }

            try_categorical_values(
                col,
                DataUsed::same_unit_categorical,
                _sample_container_begin,
                _sample_container_end,
                _sample_size,
                _candidate_splits );
        }

    debug_message( "try_same_units_categorical...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_same_units_discrete(
    const AUTOSQL_SIZE _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> &_candidate_splits )
{
    debug_message( "try_same_units_discrete..." );

    for ( AUTOSQL_INT col = 0; col < same_units_discrete().size(); ++col )
        {
            if ( skip_condition() )
                {
                    continue;
                }

            for ( auto it = _sample_container_begin;
                  it != _sample_container_end;
                  ++it )
                {
                    ( *it )->numerical_value =
                        get_same_unit_discrete( *it, col );
                }

            try_discrete_values(
                col,
                DataUsed::same_unit_discrete,
                _sample_container_begin,
                _sample_container_end,
                _sample_size,
                _candidate_splits );
        }

    debug_message( "try_same_units_discrete...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_same_units_numerical(
    const AUTOSQL_SIZE _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> &_candidate_splits )
{
    debug_message( "try_same_units_numerical..." );

    for ( AUTOSQL_INT col = 0; col < same_units_numerical().size(); ++col )
        {
            if ( skip_condition() )
                {
                    continue;
                }

            for ( auto it = _sample_container_begin;
                  it != _sample_container_end;
                  ++it )
                {
                    ( *it )->numerical_value =
                        get_same_unit_numerical( *it, col );
                }

            try_numerical_values(
                col,
                DataUsed::same_unit_numerical,
                _sample_container_begin,
                _sample_container_end,
                _sample_size,
                _candidate_splits );
        }

    debug_message( "try_same_units_numerical...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_subfeatures(
    const AUTOSQL_SIZE _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> &_candidate_splits )
{
    if ( !tree_->subfeatures() )
        {
            return;
        }

    debug_message( "try_subfeatures..." );

    for ( AUTOSQL_INT col = 0; col < tree_->subfeatures().ncols(); ++col )
        {
            if ( skip_condition() )
                {
                    continue;
                }

            for ( auto it = _sample_container_begin;
                  it != _sample_container_end;
                  ++it )
                {
                    ( *it )->numerical_value = get_x_subfeature( *it, col );
                }

            try_numerical_values(
                col,
                DataUsed::x_subfeature,
                _sample_container_begin,
                _sample_container_end,
                _sample_size,
                _candidate_splits );
        }

    debug_message( "try_subfeatures...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_time_stamps_diff(
    const AUTOSQL_SIZE _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> &_candidate_splits )
{
    debug_message( "try_time_stamps_diff..." );

    if ( skip_condition() )
        {
            return;
        }

    for ( auto it = _sample_container_begin; it != _sample_container_end; ++it )
        {
            ( *it )->numerical_value = get_time_stamps_diff( *it );
        }

    try_numerical_values(
        0,
        DataUsed::time_stamps_diff,
        _sample_container_begin,
        _sample_container_end,
        _sample_size,
        _candidate_splits );

    debug_message( "try_time_stamps_diff...done" );
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace autosql
