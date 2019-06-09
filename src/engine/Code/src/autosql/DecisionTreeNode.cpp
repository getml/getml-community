#include "autosql/decisiontrees/decisiontrees.hpp"

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
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    aggregations::AbstractAggregation *_aggregation ) const
{
    if ( std::distance( _sample_container_begin, _sample_container_end ) == 0 )
        {
            return;
        }

    if ( apply_from_above() )
        {
            if ( is_activated_ )
                {
                    _aggregation->deactivate_samples_not_containing_categories(
                        categories_used_begin(),
                        categories_used_end(),
                        _sample_container_begin,
                        _sample_container_end );
                }
            else
                {
                    _aggregation->activate_samples_not_containing_categories(
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
                    _aggregation->deactivate_samples_containing_categories(
                        categories_used_begin(),
                        categories_used_end(),
                        _sample_container_begin,
                        _sample_container_end );
                }
            else
                {
                    _aggregation->activate_samples_containing_categories(
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
    const size_t _sample_size,
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
            AUTOSQL_MAX_OP<std::int8_t>()       // op
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

std::vector<AUTOSQL_FLOAT> DecisionTreeNode::calculate_critical_values_discrete(
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    const size_t _sample_size )
{
    // ---------------------------------------------------------------------------

    debug_log( "calculate_critical_values_discrete..." );

    AUTOSQL_FLOAT min = 0.0, max = 0.0;

    // ---------------------------------------------------------------------------
    // In distributed versions, it is possible that there are no sample sizes
    // left in this process rank. In that case we effectively pass plus infinity
    // to min and minus infinity to max, ensuring that they not will be the
    // chosen minimum or maximum.

    debug_log(
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
            return std::vector<AUTOSQL_FLOAT>( 0, 1 );
        }

    // ---------------------------------------------------------------------------

    AUTOSQL_INT num_critical_values = static_cast<AUTOSQL_INT>( max - min + 1 );

    debug_log(
        "num_critical_values: " + std::to_string( num_critical_values ) );

    std::vector<AUTOSQL_FLOAT> critical_values( num_critical_values, 1 );

    for ( AUTOSQL_INT i = 0; i < num_critical_values; ++i )
        {
            critical_values[i] = min + static_cast<AUTOSQL_FLOAT>( i );
        }

    // ---------------------------------------------------------------------------

    debug_log( "calculate_critical_values_discrete...done" );

    return critical_values;

    // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<AUTOSQL_FLOAT>
DecisionTreeNode::calculate_critical_values_numerical(
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    const size_t _sample_size )
{
    // ---------------------------------------------------------------------------

    debug_log( "calculate_critical_values_numerical..." );

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
            debug_log(
                "calculate_critical_values_discrete...done (edge case)." );

            return std::vector<AUTOSQL_FLOAT>( 0, 1 );
        }

    // ---------------------------------------------------------------------------

    AUTOSQL_INT num_critical_values =
        calculate_num_critical_values( _sample_size );

    AUTOSQL_FLOAT step_size =
        ( max - min ) / static_cast<AUTOSQL_FLOAT>( num_critical_values + 1 );

    std::vector<AUTOSQL_FLOAT> critical_values( num_critical_values, 1 );

    for ( AUTOSQL_INT i = 0; i < num_critical_values; ++i )
        {
            critical_values[i] =
                min + static_cast<AUTOSQL_FLOAT>( i + 1 ) * step_size;
        }

    debug_log( "calculate_critical_values_discrete...done." );

    return critical_values;

    // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::commit(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const std::vector<containers::ColumnView<
        AUTOSQL_FLOAT,
        std::map<AUTOSQL_INT, AUTOSQL_INT>>> &_subfeatures,
    const descriptors::Split &_split,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    debug_log( "fit: Improvement possible..." );

    auto null_values_separator = identify_parameters(
        _population,
        _peripheral,
        _subfeatures,
        _split,
        _sample_container_begin,
        _sample_container_end );

    debug_log( "fit: Commit..." );

    aggregation()->commit();

    optimization_criterion()->commit();

    if ( depth_ < tree_->max_length() )
        {
            debug_log( "fit: Max length not reached..." );

            spawn_child_nodes(
                _population,
                _peripheral,
                _subfeatures,
                _sample_container_begin,
                null_values_separator,
                _sample_container_end );
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::fit(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const std::vector<containers::ColumnView<
        AUTOSQL_FLOAT,
        std::map<AUTOSQL_INT, AUTOSQL_INT>>> &_subfeatures,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    debug_log( "fit: Calculating sample size..." );

    const size_t sample_size = reduce_sample_size(
        std::distance( _sample_container_begin, _sample_container_end ) );

    if ( sample_size == 0 || static_cast<AUTOSQL_INT>( sample_size ) <
                                 tree_->min_num_samples() * 2 )
        {
            return;
        }

    // The reason we add an additional 1 is that the apply_by_... functions
    // will add another line to the storage of the optimization_criterion,
    // because they reproduce all the steps undertaken by the maximum split. But
    // the split used in the end is ix_max.
    debug_log( "fit: Setting storage size..." );

    // ------------------------------------------------------------------------
    // Try imposing different conditions and measure the performance.

    std::vector<descriptors::Split> candidate_splits = {};

    try_conditions(
        _population,
        _peripheral,
        _subfeatures,
        sample_size,
        _sample_container_begin,
        _sample_container_end,
        &candidate_splits );

    // ------------------------------------------------------------------------
    // Find maximum

    debug_log( "fit: Find maximum..." );

    // Find maximum
    AUTOSQL_INT ix_max = optimization_criterion()->find_maximum();

    const AUTOSQL_FLOAT max_value =
        optimization_criterion()->values_stored( ix_max );

    // ------------------------------------------------------------------------
    // DEBUG and parallel mode only: Make sure that the values_stored are
    // aligned!

#ifndef NDEBUG

    std::array<AUTOSQL_FLOAT, 2> values = {max_value,
                                           optimization_criterion()->value()};

    assert( std::get<0>( values ) == std::get<0>( values ) );

    assert( std::get<1>( values ) == std::get<1>( values ) );

    std::array<AUTOSQL_FLOAT, 2> global_values;

    multithreading::all_reduce(
        *comm(),                                  // comm
        values.data(),                            // in_value
        2,                                        // count
        global_values.data(),                     // out_value
        multithreading::maximum<AUTOSQL_FLOAT>()  // op
    );

    comm()->barrier();

    assert( std::get<0>( values ) == std::get<0>( global_values ) );

    assert( std::get<1>( values ) == std::get<1>( global_values ) );

#endif  // NDEBUG

    // ------------------------------------------------------------------------
    // Imposing a condition is only necessary, if it actually improves the
    // optimization criterion

    if ( max_value >
         optimization_criterion()->value() + tree_->regularization() + 1e-07 )
        {
            commit(
                _population,
                _peripheral,
                _subfeatures,
                candidate_splits[ix_max].deep_copy(),
                _sample_container_begin,
                _sample_container_end );
        }
    else
        {
            debug_log( "fit: No improvement possible..." );
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::fit_as_root(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const std::vector<containers::ColumnView<
        AUTOSQL_FLOAT,
        std::map<AUTOSQL_INT, AUTOSQL_INT>>> &_subfeatures,
    AUTOSQL_SAMPLE_CONTAINER::iterator _sample_container_begin,
    AUTOSQL_SAMPLE_CONTAINER::iterator _sample_container_end )
{
    debug_log( "fit_as_root..." );

    aggregation()->activate_all(
        true, _sample_container_begin, _sample_container_end );

    aggregation()->commit();

    optimization_criterion()->commit();

    if ( tree_->max_length() > 0 )
        {
            fit( _population,
                 _peripheral,
                 _subfeatures,
                 _sample_container_begin,
                 _sample_container_end );
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::from_json_obj( const Poco::JSON::Object &_json_obj )
{
    is_activated_ = JSON::get_value<bool>( _json_obj, "act_" );

    const bool imposes_condition = JSON::get_value<bool>( _json_obj, "imp_" );

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
                        *JSON::get_object( _json_obj, "sub1_" ) );

                    child_node_smaller_.reset(
                        new DecisionTreeNode( false, depth_ + 1, tree_ ) );

                    child_node_smaller_->from_json_obj(
                        *JSON::get_object( _json_obj, "sub2_" ) );
                }
        }
}

// ----------------------------------------------------------------------------

std::string DecisionTreeNode::greater_or_not_equal_to(
    const std::string &_colname ) const
{
    if ( data_used() == enums::DataUsed::same_unit_categorical )
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
                        category_used < static_cast<AUTOSQL_INT>(
                                            tree_->categories().size() ) );

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
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const std::vector<containers::ColumnView<
        AUTOSQL_FLOAT,
        std::map<AUTOSQL_INT, AUTOSQL_INT>>> &_subfeatures,
    const descriptors::Split &_split,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    // --------------------------------------------------------------
    // Transfer parameters from split descriptor

    split_.reset( new descriptors::Split( _split ) );

    // --------------------------------------------------------------

    debug_log( "Identify parameters..." );

    // --------------------------------------------------------------
    // Restore the optimal split

    set_samples(
        _population,
        _peripheral,
        _subfeatures,
        _sample_container_begin,
        _sample_container_end );

    // --------------------------------------------------------------
    // Change stage of aggregation to optimal split

    auto null_values_separator = _sample_container_begin;

    if ( categorical_data_used() )
        {
            debug_log( "Identify_parameters: Sort.." );

            sort_by_categorical_value(
                _sample_container_begin, _sample_container_end );

            debug_log( "Identify_parameters: apply..." );

            apply_by_categories_used_and_commit(
                _sample_container_begin, _sample_container_end );
        }
    else
        {
            // --------------------------------------------------------------

            std::vector<AUTOSQL_FLOAT> critical_values( 1, 1 );

            critical_values[0] = critical_value();

            const bool null_values_to_beginning =
                ( apply_from_above() != is_activated_ );

            // --------------------------------------------------------------

            debug_log( "Identify_parameters: Sort.." );

            null_values_separator = separate_null_values(
                _sample_container_begin,
                _sample_container_end,
                null_values_to_beginning );

            // --------------------------------------------------------------

            if ( null_values_to_beginning )
                {
                    sort_by_numerical_value(
                        null_values_separator, _sample_container_end );

                    debug_log( "Identify_parameters: apply..." );

                    if ( is_activated_ )
                        {
                            aggregation()->deactivate_samples_with_null_values(
                                _sample_container_begin,
                                null_values_separator );
                        }

                    apply_by_critical_value(
                        critical_values,
                        null_values_separator,
                        _sample_container_end,
                        aggregation() );
                }
            else
                {
                    sort_by_numerical_value(
                        _sample_container_begin, null_values_separator );

                    debug_log( "Identify_parameters: apply..." );

                    if ( is_activated_ )
                        {
                            aggregation()->deactivate_samples_with_null_values(
                                null_values_separator, _sample_container_end );
                        }

                    apply_by_critical_value(
                        critical_values,
                        _sample_container_begin,
                        null_values_separator,
                        aggregation() );
                }

            // --------------------------------------------------------------
        }

    // --------------------------------------------------------------

    return null_values_separator;
}

// ----------------------------------------------------------------------------

size_t DecisionTreeNode::reduce_sample_size( const size_t _sample_size )
{
    size_t global_sample_size = 0;

    multithreading::all_reduce(
        *comm(),                    // comm
        _sample_size,               // in_value
        global_sample_size,         // out_value
        std::plus<AUTOSQL_FLOAT>()  // op
    );

    comm()->barrier();

    return global_sample_size;
}

// ----------------------------------------------------------------------------

AUTOSQL_SAMPLE_ITERATOR DecisionTreeNode::separate_null_values(
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    bool _null_values_to_beginning ) const
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
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const std::vector<containers::ColumnView<
        AUTOSQL_FLOAT,
        std::map<AUTOSQL_INT, AUTOSQL_INT>>> &_subfeatures,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end ) const
{
    switch ( data_used() )
        {
            case enums::DataUsed::same_unit_categorical:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->categorical_value = get_same_unit_categorical(
                            _population, _peripheral, *it, column_used() );
                    }

                break;

            case enums::DataUsed::same_unit_discrete:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->numerical_value = get_same_unit_discrete(
                            _population, _peripheral, *it, column_used() );
                    }

                break;

            case enums::DataUsed::same_unit_numerical:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->numerical_value = get_same_unit_numerical(
                            _population, _peripheral, *it, column_used() );
                    }

                break;

            case enums::DataUsed::x_perip_categorical:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->categorical_value = get_x_perip_categorical(
                            _peripheral, *it, column_used() );
                    }

                break;

            case enums::DataUsed::x_perip_numerical:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->numerical_value = get_x_perip_numerical(
                            _peripheral, *it, column_used() );
                    }

                break;

            case enums::DataUsed::x_perip_discrete:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->numerical_value = get_x_perip_discrete(
                            _peripheral, *it, column_used() );
                    }

                break;

            case enums::DataUsed::x_popul_categorical:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->categorical_value = get_x_popul_categorical(
                            _population, *it, column_used() );
                    }

                break;

            case enums::DataUsed::x_popul_numerical:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->numerical_value = get_x_popul_numerical(
                            _population, *it, column_used() );
                    }

                break;

            case enums::DataUsed::x_popul_discrete:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->numerical_value = get_x_popul_discrete(
                            _population, *it, column_used() );
                    }

                break;

            case enums::DataUsed::x_subfeature:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->numerical_value = get_x_subfeature(
                            _subfeatures, *it, column_used() );
                    }

                break;

            case enums::DataUsed::time_stamps_diff:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->numerical_value = get_time_stamps_diff(
                            _population, _peripheral, *it );
                    }

                break;

            default:

                assert( false && "Unknown enums::DataUsed!" );
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

std::string DecisionTreeNode::smaller_or_equal_to(
    const std::string &_colname ) const
{
    if ( data_used() == enums::DataUsed::same_unit_categorical )
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
                        category_used < static_cast<AUTOSQL_INT>(
                                            tree_->categories().size() ) );

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
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const std::vector<containers::ColumnView<
        AUTOSQL_FLOAT,
        std::map<AUTOSQL_INT, AUTOSQL_INT>>> &_subfeatures,
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

    child_node_greater_->fit(
        _population, _peripheral, _subfeatures, it, _sample_container_end );

    // -------------------------------------------------------------------------
    // Set up and fit child_node_smaller_

    child_node_smaller_.reset( new DecisionTreeNode(
        !( child_node_greater_is_activated ),  // _is_activated
        depth_ + 1,                            // _depth
        tree_                                  // _tree
        ) );

    child_node_smaller_->fit(
        _population, _peripheral, _subfeatures, _sample_container_begin, it );

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
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const std::vector<containers::ColumnView<
        AUTOSQL_FLOAT,
        std::map<AUTOSQL_INT, AUTOSQL_INT>>> &_subfeatures,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    aggregations::AbstractAggregation *_aggregation ) const
{
    // -----------------------------------------------------------

    // Some nodes do not impose a condition at all. It that case they
    // cannot have any children either and there is nothing left to do.
    if ( !split_ )
        {
            debug_log( "transform: Does not impose condition..." );

            return;
        }

    // -----------------------------------------------------------

    debug_log( "transform: Setting samples..." );

    set_samples(
        _population,
        _peripheral,
        _subfeatures,
        _sample_container_begin,
        _sample_container_end );

    // -----------------------------------------------------------

    debug_log( "transform: Applying condition..." );

    if ( categorical_data_used() )
        {
            apply_by_categories_used(
                _sample_container_begin, _sample_container_end, _aggregation );
        }
    else
        {
            apply_by_critical_value(
                critical_value(),
                _sample_container_begin,
                _sample_container_end,
                _aggregation );
        }

    // -----------------------------------------------------------
    // If the node has child nodes, use them to transform as well

    auto it = _sample_container_begin;

    if ( child_node_greater_ )
        {
            debug_log( "transform: Has child..." );

            // ---------------------------------------------------------

            debug_log( "transform: Partitioning by value.." );

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

                    debug_log( "transform: Separating null values..." );

                    const bool null_values_to_beginning =
                        ( apply_from_above() != is_activated_ );

                    auto null_values_separator = separate_null_values(
                        _sample_container_begin,
                        _sample_container_end,
                        null_values_to_beginning );

                    // ---------------------------------------------------------

                    debug_log( "transform: Separating by critical values..." );

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

            child_node_smaller_->transform(
                _population,
                _peripheral,
                _subfeatures,
                _sample_container_begin,
                it,
                _aggregation );

            child_node_greater_->transform(
                _population,
                _peripheral,
                _subfeatures,
                it,
                _sample_container_end,
                _aggregation );

            // ---------------------------------------------------------
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_categorical_peripheral(
    const containers::DataFrame &_peripheral,
    const size_t _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
{
    debug_log( "try_categorical_peripheral..." );

    for ( size_t col = 0; col < _peripheral.num_categoricals(); ++col )
        {
            if ( _peripheral.categorical_unit( col ).find(
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
                        get_x_perip_categorical( _peripheral, *it, col );
                }

            try_categorical_values(
                col,
                enums::DataUsed::x_perip_categorical,
                _sample_size,
                _sample_container_begin,
                _sample_container_end,
                _candidate_splits );
        }

    debug_log( "try_categorical_peripheral...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_categorical_population(
    const containers::DataFrameView &_population,
    const size_t _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
{
    debug_log( "try_categorical_population..." );

    for ( size_t col = 0; col < _population.num_categoricals(); ++col )
        {
            if ( _population.categorical_unit( col ).find(
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
                        get_x_popul_categorical( _population, *it, col );
                }

            try_categorical_values(
                col,
                enums::DataUsed::x_popul_categorical,
                _sample_size,
                _sample_container_begin,
                _sample_container_end,
                _candidate_splits );
        }

    debug_log( "try_categorical_population...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_discrete_peripheral(
    const containers::DataFrame &_peripheral,
    const size_t _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
{
    debug_log( "try_discrete_peripheral..." );

    for ( size_t col = 0; col < _peripheral.num_discretes(); ++col )
        {
            if ( _peripheral.discrete_unit( col ).find( "comparison only" ) !=
                 std::string::npos )
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
                        get_x_perip_discrete( _peripheral, *it, col );
                }

            try_discrete_values(
                col,
                enums::DataUsed::x_perip_discrete,
                _sample_size,
                _sample_container_begin,
                _sample_container_end,
                _candidate_splits );
        }

    debug_log( "try_discrete_peripheral...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_discrete_population(
    const containers::DataFrameView &_population,
    const size_t _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
{
    debug_log( "try_discrete_population..." );

    for ( size_t col = 0; col < _population.num_discretes(); ++col )
        {
            if ( _population.discrete_unit( col ).find( "comparison only" ) !=
                 std::string::npos )
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
                        get_x_popul_discrete( _population, *it, col );
                }

            try_discrete_values(
                col,
                enums::DataUsed::x_popul_discrete,
                _sample_size,
                _sample_container_begin,
                _sample_container_end,
                _candidate_splits );
        }

    debug_log( "try_discrete_population...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_numerical_peripheral(
    const containers::DataFrame &_peripheral,
    const size_t _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
{
    debug_log( "try_numerical_peripheral..." );

    for ( size_t col = 0; col < _peripheral.num_numericals(); ++col )
        {
            if ( _peripheral.numerical_unit( col ).find( "comparison only" ) !=
                 std::string::npos )
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
                        get_x_perip_numerical( _peripheral, *it, col );
                }

            try_numerical_values(
                col,
                enums::DataUsed::x_perip_numerical,
                _sample_size,
                _sample_container_begin,
                _sample_container_end,
                _candidate_splits );
        }

    debug_log( "try_numerical_peripheral...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_numerical_population(
    const containers::DataFrameView &_population,
    const size_t _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
{
    debug_log( "try_numerical_population..." );

    for ( size_t col = 0; col < _population.num_numericals(); ++col )
        {
            if ( _population.numerical_unit( col ).find( "comparison only" ) !=
                 std::string::npos )
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
                        get_x_popul_numerical( _population, *it, col );
                }

            try_numerical_values(
                col,
                enums::DataUsed::x_popul_numerical,
                _sample_size,
                _sample_container_begin,
                _sample_container_end,
                _candidate_splits );
        }

    debug_log( "try_numerical_population...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_categorical_values(
    const AUTOSQL_INT _column_used,
    const enums::DataUsed _data_used,
    const size_t _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
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
            _candidate_splits->push_back( descriptors::Split(
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
            _candidate_splits->push_back( descriptors::Split(
                true,          // _apply_from_above
                categories,    // _categories_used
                cat,           // _categories_used_begin
                cat + 1,       // _categories_used_end
                _column_used,  // _column_used
                _data_used     // _data_used
                ) );
        }

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

    if ( categories->size() < 3 || !tree_->allow_sets() )
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
            _candidate_splits->push_back( descriptors::Split(
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
            _candidate_splits->push_back( descriptors::Split(
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
            _candidate_splits->push_back( descriptors::Split(
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
            _candidate_splits->push_back( descriptors::Split(
                false,                             // _apply_from_above
                sorted_by_not_containing,          // _categories_used
                cat + 1,                           // _categories_used_begin
                sorted_by_not_containing->cend(),  // _categories_used_end
                _column_used,                      // _column_used
                _data_used                         // _data_used
                ) );
        }

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
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const std::vector<containers::ColumnView<
        AUTOSQL_FLOAT,
        std::map<AUTOSQL_INT, AUTOSQL_INT>>> &_subfeatures,
    const size_t _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
{
    try_same_units_categorical(
        _population,
        _peripheral,
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_same_units_discrete(
        _population,
        _peripheral,
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_same_units_numerical(
        _population,
        _peripheral,
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_categorical_peripheral(
        _peripheral,
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_discrete_peripheral(
        _peripheral,
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_numerical_peripheral(
        _peripheral,
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_categorical_population(
        _population,
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_discrete_population(
        _population,
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_numerical_population(
        _population,
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_subfeatures(
        _subfeatures,
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    try_time_stamps_diff(
        _population,
        _peripheral,
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_discrete_values(
    const AUTOSQL_INT _column_used,
    const enums::DataUsed _data_used,
    const size_t _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
{
    // -----------------------------------------------------------------------

    debug_log( "try_discrete_values..." );

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
        _sample_size,
        critical_values,
        _sample_container_begin,
        null_values_separator,
        _sample_container_end,
        _candidate_splits );

    // -----------------------------------------------------------------------

    debug_log( "try_discrete_values...done." );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_non_categorical_values(
    const AUTOSQL_INT _column_used,
    const enums::DataUsed _data_used,
    const size_t _sample_size,
    const std::vector<AUTOSQL_FLOAT> _critical_values,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _null_values_separator,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
{
    // -----------------------------------------------------------------------

    debug_log( "try_non_categorical_values..." );

    // -----------------------------------------------------------------------
    // Add new splits to the candidate splits

    debug_log( "try_non_categorical_values: Add new splits." );

    for ( size_t i = 0; i < _critical_values.size(); ++i )
        {
            _candidate_splits->push_back( descriptors::Split(
                true,
                _critical_values.end()[-i - 1],
                _column_used,
                _data_used ) );
        }

    for ( auto &critical_value : _critical_values )
        {
            _candidate_splits->push_back( descriptors::Split(
                false, critical_value, _column_used, _data_used ) );
        }

    // -----------------------------------------------------------------------
    // If this is an activated node, we need to deactivate all samples for
    // which the numerical value is NULL

    debug_log( "try_non_categorical_values: Handle NULL." );

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
            for ( size_t i = 0; i < _critical_values.size() * 2; ++i )
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

    debug_log( "try_non_categorical_values: Apply from above..." );

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

    debug_log( "try_non_categorical_values: Apply from below..." );

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

    debug_log( "try_non_categorical_values: Revert..." );

    aggregation()->revert_to_commit();

    optimization_criterion()->revert_to_commit();

    // -----------------------------------------------------------------------

    debug_log( "try_non_categorical_values...done." );

    // -----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_numerical_values(
    const AUTOSQL_INT _column_used,
    const enums::DataUsed _data_used,
    const size_t _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
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
        _sample_size,
        critical_values,
        _sample_container_begin,
        null_values_separator,
        _sample_container_end,
        _candidate_splits );

    // -----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_same_units_categorical(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const size_t _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
{
    debug_log( "try_same_units_categorical..." );

    for ( size_t col = 0; col < same_units_categorical().size(); ++col )
        {
            if ( skip_condition() )
                {
                    continue;
                }

            for ( auto it = _sample_container_begin;
                  it != _sample_container_end;
                  ++it )
                {
                    ( *it )->categorical_value = get_same_unit_categorical(
                        _population, _peripheral, *it, col );
                }

            try_categorical_values(
                col,
                enums::DataUsed::same_unit_categorical,
                _sample_size,
                _sample_container_begin,
                _sample_container_end,
                _candidate_splits );
        }

    debug_log( "try_same_units_categorical...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_same_units_discrete(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const size_t _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
{
    debug_log( "try_same_units_discrete..." );

    for ( size_t col = 0; col < same_units_discrete().size(); ++col )
        {
            if ( skip_condition() )
                {
                    continue;
                }

            for ( auto it = _sample_container_begin;
                  it != _sample_container_end;
                  ++it )
                {
                    ( *it )->numerical_value = get_same_unit_discrete(
                        _population, _peripheral, *it, col );
                }

            try_discrete_values(
                col,
                enums::DataUsed::same_unit_discrete,
                _sample_size,
                _sample_container_begin,
                _sample_container_end,
                _candidate_splits );
        }

    debug_log( "try_same_units_discrete...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_same_units_numerical(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const size_t _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
{
    debug_log( "try_same_units_numerical..." );

    for ( size_t col = 0; col < same_units_numerical().size(); ++col )
        {
            if ( skip_condition() )
                {
                    continue;
                }

            for ( auto it = _sample_container_begin;
                  it != _sample_container_end;
                  ++it )
                {
                    ( *it )->numerical_value = get_same_unit_numerical(
                        _population, _peripheral, *it, col );
                }

            try_numerical_values(
                col,
                enums::DataUsed::same_unit_numerical,
                _sample_size,
                _sample_container_begin,
                _sample_container_end,
                _candidate_splits );
        }

    debug_log( "try_same_units_numerical...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_subfeatures(
    const std::vector<containers::ColumnView<
        AUTOSQL_FLOAT,
        std::map<AUTOSQL_INT, AUTOSQL_INT>>> &_subfeatures,
    const size_t _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
{
    debug_log( "try_subfeatures..." );

    for ( size_t col = 0; col < _subfeatures.size(); ++col )
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
                        get_x_subfeature( _subfeatures, *it, col );
                }

            try_numerical_values(
                col,
                enums::DataUsed::x_subfeature,
                _sample_size,
                _sample_container_begin,
                _sample_container_end,
                _candidate_splits );
        }

    debug_log( "try_subfeatures...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_time_stamps_diff(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const size_t _sample_size,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
{
    debug_log( "try_time_stamps_diff..." );

    if ( skip_condition() )
        {
            return;
        }

    for ( auto it = _sample_container_begin; it != _sample_container_end; ++it )
        {
            ( *it )->numerical_value =
                get_time_stamps_diff( _population, _peripheral, *it );
        }

    try_numerical_values(
        0,
        enums::DataUsed::time_stamps_diff,
        _sample_size,
        _sample_container_begin,
        _sample_container_end,
        _candidate_splits );

    debug_log( "try_time_stamps_diff...done" );
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace autosql
