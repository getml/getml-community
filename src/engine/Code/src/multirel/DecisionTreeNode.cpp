#include "multirel/decisiontrees/decisiontrees.hpp"

namespace multirel
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

DecisionTreeNode::DecisionTreeNode(
    bool _is_activated, Int _depth, const DecisionTreeImpl *_tree )
    : depth_( _depth ), is_activated_( _is_activated ), tree_( _tree ){};

// ----------------------------------------------------------------------------

void DecisionTreeNode::apply_by_categories_used(
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
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
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end )
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

std::shared_ptr<const std::vector<Int>> DecisionTreeNode::calculate_categories(
    const size_t _sample_size,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end )
{
    // ------------------------------------------------------------------------

    Int categories_begin = 0;
    Int categories_end = 0;

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
            categories_begin = std::numeric_limits<Int>::max();
            categories_end = 0;
        }

    utils::Reducer::reduce(
        multithreading::minimum<Int>(), &categories_begin, comm() );

    utils::Reducer::reduce(
        multithreading::maximum<Int>(), &categories_end, comm() );

    // ------------------------------------------------------------------------
    // There is a possibility that all critical values are NULL (signified by
    // -1) in all processes. This accounts for this edge case.

    if ( categories_begin >= categories_end )
        {
            return std::make_shared<std::vector<Int>>( 0 );
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

            assert_true( ( *it )->categorical_value >= categories_begin );
            assert_true( ( *it )->categorical_value < categories_end );

            included[( *it )->categorical_value - categories_begin] = 1;
        }

    utils::Reducer::reduce(
        multithreading::maximum<std::int8_t>(), &included, comm() );

    // ------------------------------------------------------------------------
    // Build vector.

    auto categories = std::make_shared<std::vector<Int>>( 0 );

    for ( Int i = 0; i < categories_end - categories_begin; ++i )
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

std::vector<Float> DecisionTreeNode::calculate_critical_values_discrete(
    const size_t _sample_size,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end )
{
    // ---------------------------------------------------------------------------

    debug_log( "calculate_critical_values_discrete..." );

    Float min = 0.0, max = 0.0;

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
            min = std::numeric_limits<Float>::max();
            max = std::numeric_limits<Float>::lowest();
        }

    utils::Reducer::reduce( multithreading::minimum<Float>(), &min, comm() );

    utils::Reducer::reduce( multithreading::maximum<Float>(), &max, comm() );

    // ---------------------------------------------------------------------------
    // There is a possibility that all critical values are NAN in all processes.
    // This accounts for this edge case.

    if ( min > max )
        {
            return std::vector<Float>( 0.0, 1 );
        }

    // ---------------------------------------------------------------------------
    // If the number of critical values is too large, then use the numerical
    // algorithm instead.

    const auto num_critical_values = static_cast<Int>( max - min + 1 );

    const auto num_critical_values_numerical =
        calculate_num_critical_values( _sample_size );

    if ( num_critical_values_numerical < num_critical_values )
        {
            auto critical_values = calculate_critical_values_numerical(
                num_critical_values_numerical, min, max );

            for ( auto &c : critical_values )
                {
                    c = std::floor( c );
                }

            debug_log(
                "calculate_critical_values_discrete (using numerical "
                "approach)...done" );

            return critical_values;
        }

    // ---------------------------------------------------------------------------

    debug_log(
        "num_critical_values: " + std::to_string( num_critical_values ) );

    std::vector<Float> critical_values( num_critical_values, 1 );

    for ( Int i = 0; i < num_critical_values; ++i )
        {
            critical_values[i] = min + static_cast<Float>( i );
        }

    // ---------------------------------------------------------------------------

    debug_log( "calculate_critical_values_discrete...done" );

    return critical_values;

    // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<Float> DecisionTreeNode::calculate_critical_values_numerical(
    const size_t _sample_size,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end )
{
    // ---------------------------------------------------------------------------

    debug_log( "calculate_critical_values_numerical..." );

    Float min = 0.0, max = 0.0;

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
            min = std::numeric_limits<Float>::max();
            max = std::numeric_limits<Float>::lowest();
        }

    utils::Reducer::reduce( multithreading::minimum<Float>(), &min, comm() );

    utils::Reducer::reduce( multithreading::maximum<Float>(), &max, comm() );

    // ---------------------------------------------------------------------------
    // There is a possibility that all critical values are NAN in all processes.
    // This accounts for this edge case.

    if ( min > max )
        {
            debug_log(
                "calculate_critical_values_numerical.distance..done (edge "
                "case)." );

            return std::vector<Float>( 0 );
        }

    // ---------------------------------------------------------------------------

    Int num_critical_values = calculate_num_critical_values( _sample_size );

    return calculate_critical_values_numerical( num_critical_values, min, max );

    // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<Float> DecisionTreeNode::calculate_critical_values_numerical(
    const Int _num_critical_values, const Float _min, const Float _max )
{
    Float step_size =
        ( _max - _min ) / static_cast<Float>( _num_critical_values + 1 );

    std::vector<Float> critical_values( _num_critical_values );

    for ( Int i = 0; i < _num_critical_values; ++i )
        {
            critical_values[i] = _min + static_cast<Float>( i + 1 ) * step_size;
        }

    return critical_values;
}

// ----------------------------------------------------------------------------

std::vector<Float> DecisionTreeNode::calculate_critical_values_window(
    const Float _delta_t,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end )
{
    // ---------------------------------------------------------------------------

    debug_log( "calculate_critical_values_window..." );

    Float min = 0.0, max = 0.0;

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
            min = std::numeric_limits<Float>::max();
            max = std::numeric_limits<Float>::lowest();
        }

    utils::Reducer::reduce( multithreading::minimum<Float>(), &min, comm() );

    utils::Reducer::reduce( multithreading::maximum<Float>(), &max, comm() );

    // ---------------------------------------------------------------------------
    // There is a possibility that all critical values are NAN in all processes.
    // This accounts for this edge case.

    if ( min > max )
        {
            debug_log( "calculate_critical_values_window...done (edge case)." );

            return std::vector<Float>( 0, 1 );
        }

    // ---------------------------------------------------------------------------
    // The input value for delta_t could be stupid...we want to avoid memory
    // overflow.

    assert_true( _delta_t > 0.0 );

    const auto num_critical_values =
        static_cast<size_t>( ( max - min ) / _delta_t ) + 1;

    if ( num_critical_values > 100000 )
        {
            debug_log(
                "calculate_critical_values_window...done (delta_t too "
                "small)." );

            return std::vector<Float>( 0, 1 );
        }

    // ---------------------------------------------------------------------------

    std::vector<Float> critical_values( num_critical_values );

    for ( size_t i = 0; i < num_critical_values; ++i )
        {
            critical_values[i] = min + static_cast<Float>( i + 1 ) * _delta_t;
        }

    debug_log( "calculate_critical_values_window...done." );

    return critical_values;

    // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::commit(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const containers::Subfeatures &_subfeatures,
    const descriptors::Split &_split,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end )
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

    debug_log(
        "commit: optimization_criterion()->value(): " +
        std::to_string( optimization_criterion()->value() ) );

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
    const containers::Subfeatures &_subfeatures,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end )
{
    debug_log( "fit: Calculating sample size..." );

    const size_t sample_size = reduce_sample_size(
        std::distance( _sample_container_begin, _sample_container_end ) );

    if ( sample_size == 0 || sample_size < tree_->min_num_samples() * 2 )
        {
            return;
        }

    optimization_criterion()->reset_storage_size();

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
    Int ix_max = optimization_criterion()->find_maximum();

    const Float max_value = optimization_criterion()->values_stored( ix_max );

    debug_log( "max_value:" + std::to_string( max_value ) );

    // ------------------------------------------------------------------------
    // DEBUG and parallel mode only: Make sure that the values_stored are
    // aligned!

#ifndef NDEBUG

    auto global_storage_ix = optimization_criterion()->storage_ix();

    utils::Reducer::reduce<size_t>(
        multithreading::maximum<size_t>(), &global_storage_ix, comm() );

    assert_true( global_storage_ix == optimization_criterion()->storage_ix() );

    auto global_value = optimization_criterion()->value();

    utils::Reducer::reduce<Float>(
        multithreading::maximum<Float>(), &global_value, comm() );

    assert_true( global_value == optimization_criterion()->value() );

    auto global_max_value = max_value;

    utils::Reducer::reduce<Float>(
        multithreading::maximum<Float>(), &global_max_value, comm() );

    assert_true( global_max_value == max_value );

#endif  // NDEBUG

    // ------------------------------------------------------------------------
    // Imposing a condition is only necessary, if it actually improves the
    // optimization criterion

    if ( max_value >
         optimization_criterion()->value() + tree_->regularization() + 1e-07 )
        {
            debug_log( "fit: Committing..." );

            assert_true( ix_max < candidate_splits.size() );

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
    const containers::Subfeatures &_subfeatures,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end )
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
            sql << _colname << " NOT IN ( ";

            for ( auto it = categories_used_begin(); it < categories_used_end();
                  ++it )
                {
                    const auto category_used = *it;

                    assert_true( category_used >= 0 );
                    assert_true(
                        category_used <
                        static_cast<Int>( tree_->categories().size() ) );

                    sql << "'";

                    sql << tree_->categories()[category_used];

                    sql << "'";

                    if ( std::next( it, 1 ) != categories_used_end() )
                        {
                            sql << ", ";
                        }
                }

            sql << " )";
        }
    else
        {
            if ( lag_used() )
                {
                    sql << "( ";

                    sql << _colname;

                    sql << " <= ";

                    sql << std::to_string(
                        critical_value() - tree_->delta_t() );

                    sql << " OR ";
                }

            sql << _colname;

            sql << " > ";

            sql << std::to_string( critical_value() );

            if ( lag_used() )
                {
                    sql << " )";
                }
        }

    return sql.str();
}

// ----------------------------------------------------------------------------

containers::MatchPtrs::iterator DecisionTreeNode::identify_parameters(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const containers::Subfeatures &_subfeatures,
    const descriptors::Split &_split,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end )
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

    debug_log(
        "Data used: " +
        std::to_string( JSON::data_used_to_int( split_->data_used ) ) );

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

            std::vector<Float> critical_values( 1 );

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

containers::MatchPtrs::iterator DecisionTreeNode::partition_by_categories_used(
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end ) const
{
    const auto is_contained = [this]( const containers::Match *_sample ) {
        return std::any_of(
            categories_used_begin(),
            categories_used_end(),
            [_sample]( Int cat ) {
                return cat == _sample->categorical_value;
            } );
    };

    return std::partition(
        _sample_container_begin, _sample_container_end, is_contained );
}

// ----------------------------------------------------------------------------

containers::MatchPtrs::iterator DecisionTreeNode::partition_by_critical_value(
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end ) const
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

    if ( lag_used() )
        {
            return std::partition(
                _sample_container_begin,
                _sample_container_end,
                [this]( const containers::Match *_sample ) {
                    return (
                        _sample->numerical_value <= critical_value() &&
                        _sample->numerical_value >
                            critical_value() - tree_->delta_t() );
                } );
        }
    else
        {
            if ( null_values_to_beginning )
                {
                    return std::partition(
                        null_values_separator,
                        _sample_container_end,
                        [this]( const containers::Match *_sample ) {
                            return _sample->numerical_value <= critical_value();
                        } );
                }
            else
                {
                    return std::partition(
                        _sample_container_begin,
                        null_values_separator,
                        [this]( const containers::Match *_sample ) {
                            return _sample->numerical_value <= critical_value();
                        } );
                }
        }

    // ---------------------------------------------------------
}

// ----------------------------------------------------------------------------

size_t DecisionTreeNode::reduce_sample_size( const size_t _sample_size )
{
    size_t global_sample_size = _sample_size;

    utils::Reducer::reduce( std::plus<size_t>(), &global_sample_size, comm() );

    return global_sample_size;
}

// ----------------------------------------------------------------------------

containers::MatchPtrs::iterator DecisionTreeNode::separate_null_values(
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
    bool _null_values_to_beginning ) const
{
    auto is_null = []( containers::Match *sample ) {
        return (
            std::isnan( sample->numerical_value ) ||
            std::isinf( sample->numerical_value ) );
    };

    auto is_not_null = []( containers::Match *sample ) {
        return (
            !std::isnan( sample->numerical_value ) &&
            !std::isinf( sample->numerical_value ) );
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
    const containers::Subfeatures &_subfeatures,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end ) const
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

            case enums::DataUsed::time_stamps_window:

                for ( auto it = _sample_container_begin;
                      it != _sample_container_end;
                      ++it )
                    {
                        ( *it )->numerical_value = get_time_stamps_diff(
                            _population, _peripheral, *it );
                    }

                break;

            default:

                assert_true( false && "Unknown enums::DataUsed!" );
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::sort_by_categorical_value(
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end )
{
    auto compare_op = []( const containers::Match *sample1,
                          const containers::Match *sample2 ) {
        return sample1->categorical_value < sample2->categorical_value;
    };

    std::sort( _sample_container_begin, _sample_container_end, compare_op );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::sort_by_numerical_value(
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end )
{
    auto compare_op = []( const containers::Match *sample1,
                          const containers::Match *sample2 ) {
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
            sql << _colname << " IN ( ";

            for ( auto it = categories_used_begin(); it < categories_used_end();
                  ++it )
                {
                    const auto category_used = *it;

                    assert_true( category_used >= 0 );
                    assert_true(
                        category_used <
                        static_cast<Int>( tree_->categories().size() ) );

                    sql << "'";

                    sql << tree_->categories()[category_used];

                    sql << "'";

                    if ( std::next( it, 1 ) != categories_used_end() )
                        {
                            sql << ", ";
                        }
                }

            sql << " )";
        }
    else
        {
            if ( lag_used() )
                {
                    sql << "( ";

                    sql << _colname;

                    sql << " > ";

                    sql << std::to_string(
                        critical_value() - tree_->delta_t() );

                    sql << " AND ";
                }

            sql << _colname;

            sql << " <= ";

            sql << std::to_string( critical_value() );

            if ( lag_used() )
                {
                    sql << " )";
                }
        }

    return sql.str();
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::spawn_child_nodes(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const containers::Subfeatures &_subfeatures,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _null_values_separator,
    containers::MatchPtrs::iterator _sample_container_end )
{
    // -------------------------------------------------------------------------

    auto it = _sample_container_begin;

    const bool child_node_greater_is_activated =
        ( apply_from_above() != is_activated_ );

    // If child_node_greater_is_activated, then the NULL samples
    // are allocated to the beginning, since they must always be
    // deactivated.
    /*  if ( child_node_greater_is_activated )
          {
              it = _null_values_separator;
          }*/

    // -------------------------------------------------------------------------

    if ( categorical_data_used() )
        {
            it = partition_by_categories_used(
                _sample_container_begin, _sample_container_end );

            // The samples where the category equals any of categories_used()
            // are copied into samples_smaller. This makes sense, because
            // for the numerical values, samples_smaller contains all values
            // <= critical_value()

            /* const auto is_contained =
                 [this]( const containers::Match *_sample ) {
                     return std::any_of(
                         categories_used_begin(),
                         categories_used_end(),
                         [_sample]( Int cat ) {
                             return cat == _sample->categorical_value;
                         } );
                 };

             it = std::partition(
                 _sample_container_begin, _sample_container_end, is_contained
             );*/
        }
    else
        {
            it = partition_by_critical_value(
                _sample_container_begin, _sample_container_end );

            /*  while ( it < _sample_container_end )
                  {
                      const Float val = ( *it )->numerical_value;

                      // If val != val, then all samples but the NULL samples
                      // are activated. This is a corner case that can only
                      // happen when the user has defined a min_num_samples
                      // of 0.
                      if ( val > critical_value() || val != val )
                          {
                              break;
                          }

                      ++it;
                  }*/
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

Poco::JSON::Object DecisionTreeNode::to_json_obj() const
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
    const containers::Subfeatures &_subfeatures,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
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
                    it = partition_by_categories_used(
                        _sample_container_begin, _sample_container_end );
                }
            else
                {
                    it = partition_by_critical_value(
                        _sample_container_begin, _sample_container_end );
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
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
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
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
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
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
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
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
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
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
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
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
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
    const size_t _column_used,
    const enums::DataUsed _data_used,
    const size_t _sample_size,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
{
    // -----------------------------------------------------------------------

    sort_by_categorical_value( _sample_container_begin, _sample_container_end );

    const auto categories = calculate_categories(
        _sample_size, _sample_container_begin, _sample_container_end );

    const auto index = containers::CategoryIndex(
        *categories, _sample_container_begin, _sample_container_end );

    const auto num_categories = categories->size();

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
        std::make_shared<std::vector<Int>>( num_categories );

    {
        const auto indices = optimization_criterion()->argsort(
            storage_ix - num_categories * 2, storage_ix - num_categories );

        assert_true( indices.size() == categories->size() );

        for ( size_t i = 0; i < indices.size(); ++i )
            {
                assert_true( indices[i] >= 0 );
                assert_true( indices[i] < num_categories );

                ( *sorted_by_containing )[i] = ( *categories )[indices[i]];
            }
    }

    // -----------------------------------------------------------------------
    // Produce sorted_by_not_containing.

    auto sorted_by_not_containing =
        std::make_shared<std::vector<Int>>( num_categories );

    {
        const auto indices = optimization_criterion()->argsort(
            storage_ix - num_categories, storage_ix );

        assert_true( indices.size() == categories->size() );

        for ( size_t i = 0; i < indices.size(); ++i )
            {
                assert_true( indices[i] >= 0 );
                assert_true( indices[i] < num_categories );

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
    const containers::Subfeatures &_subfeatures,
    const size_t _sample_size,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
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
    const size_t _column_used,
    const enums::DataUsed _data_used,
    const size_t _sample_size,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
{
    // -----------------------------------------------------------------------

    debug_log( "try_discrete_values..." );

    // -----------------------------------------------------------------------

    const auto nan_begin = separate_null_values(
        _sample_container_begin, _sample_container_end, false );

    auto bins =
        containers::MatchPtrs( _sample_container_begin, _sample_container_end );

    const auto [min, max] = utils::MinMaxFinder<Float>::find_min_max(
        _sample_container_begin, nan_begin, comm() );

    const auto num_bins = calc_num_bins( _sample_container_begin, nan_begin );

    // Note that this bins in ASCENDING order.
    const auto [indptr, step_size] = utils::NumericalBinner::bin(
        min,
        max,
        num_bins,
        _sample_container_begin,
        nan_begin,
        _sample_container_end,
        &bins );

    // -----------------------------------------------------------------------

    try_non_categorical_values(
        _column_used,
        _data_used,
        _sample_size,
        min,
        step_size,
        indptr,
        bins,
        _candidate_splits );

    // -----------------------------------------------------------------------

    debug_log( "try_discrete_values...done." );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_non_categorical_values(
    const size_t _column_used,
    const enums::DataUsed _data_used,
    const size_t _sample_size,
    const Float _min,
    const Float _step_size,
    const std::vector<size_t> &_indptr,
    containers::MatchPtrs &_bins,  // TODO: Make const.
    std::vector<descriptors::Split> *_candidate_splits )
{
    // -----------------------------------------------------------------------

    debug_log( "try_non_categorical_values..." );

    // -----------------------------------------------------------------------

    if ( _indptr.size() == 0 )
        {
            return;
        }

    // -----------------------------------------------------------------------
    // Temporary solution.

    auto critical_values = std::vector<Float>( _indptr.size() - 1 );

    for ( size_t i = 1; i < _indptr.size(); ++i )
        {
            critical_values[i - 1] =
                _min + static_cast<Float>( i ) * _step_size;
        }

    // -----------------------------------------------------------------------
    // Add new splits to the candidate splits

    debug_log( "try_non_categorical_values: Add new splits." );

    for ( auto it = critical_values.rbegin(); it != critical_values.rend();
          ++it )
        {
            _candidate_splits->push_back(
                descriptors::Split( true, *it, _column_used, _data_used ) );
        }

    for ( auto &cv : critical_values )
        {
            _candidate_splits->push_back(
                descriptors::Split( false, cv, _column_used, _data_used ) );
        }

    // -----------------------------------------------------------------------
    // If this is an activated node, we need to deactivate all samples for
    // which the numerical value is NULL

    debug_log( "try_non_categorical_values: Handle NULL." );

    if ( is_activated_ )
        {
            aggregation()->deactivate_samples_with_null_values(
                _bins.begin() + _indptr.back(), _bins.end() );
        }

    // -----------------------------------------------------------------------
    // It is possible that std::distance( _sample_container_begin,
    // _sample_container_end ) is zero, when we are using the distributed
    // version. In that case we want this process to continue until this point,
    // because calculate_critical_values_numerical and
    // calculate_critical_values_discrete contains barriers and we want to
    // avoid a livelock.

    if ( _indptr.back() == 0 )
        {
            for ( size_t i = 0; i < critical_values.size() * 2; ++i )
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
            debug_log( "Deactivate..." );

            aggregation()->deactivate_samples_from_above(
                critical_values,
                _bins.begin(),
                _bins.begin() + _indptr.back() );
        }
    else
        {
            debug_log( "Activate..." );

            aggregation()->activate_samples_from_above(
                critical_values,
                _bins.begin(),
                _bins.begin() + _indptr.back() );
        }

    debug_log( "Revert to commit..." );

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
                _bins.begin() + _indptr.back(), _bins.end() );
        }

    // -----------------------------------------------------------------------
    // Try applying from below

    debug_log( "try_non_categorical_values: Apply from below..." );

    // Apply changes and store resulting value of optimization criterion
    if ( is_activated_ )
        {
            aggregation()->deactivate_samples_from_below(
                critical_values,
                _bins.begin(),
                _bins.begin() + _indptr.back() );
        }
    else
        {
            aggregation()->activate_samples_from_below(
                critical_values,
                _bins.begin(),
                _bins.begin() + _indptr.back() );
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
    const size_t _column_used,
    const enums::DataUsed _data_used,
    const size_t _sample_size,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
{
    // -----------------------------------------------------------------------

    const auto nan_begin = separate_null_values(
        _sample_container_begin, _sample_container_end, false );

    auto bins =
        containers::MatchPtrs( _sample_container_begin, _sample_container_end );

    const auto [min, max] = utils::MinMaxFinder<Float>::find_min_max(
        _sample_container_begin, nan_begin, comm() );

    const auto num_bins = calc_num_bins( _sample_container_begin, nan_begin );

    // Note that this bins in ASCENDING order.
    const auto [indptr, step_size] = utils::NumericalBinner::bin(
        min,
        max,
        num_bins,
        _sample_container_begin,
        nan_begin,
        _sample_container_end,
        &bins );

    // -----------------------------------------------------------------------

    try_non_categorical_values(
        _column_used,
        _data_used,
        _sample_size,
        min,
        step_size,
        indptr,
        bins,
        _candidate_splits );

    // -----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_same_units_categorical(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const size_t _sample_size,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
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
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
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
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
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
    const containers::Subfeatures &_subfeatures,
    const size_t _sample_size,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
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
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
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

    if ( tree_->delta_t() > 0.0 )
        {
            debug_log( "try time_stamps_window..." );

            try_window(
                0,
                enums::DataUsed::time_stamps_window,
                _sample_size,
                tree_->delta_t(),
                _sample_container_begin,
                _sample_container_end,
                _candidate_splits );
        }

    debug_log( "try_time_stamps_diff...done" );
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_window(
    const size_t _column_used,
    const enums::DataUsed _data_used,
    const size_t _sample_size,
    const Float _delta_t,
    containers::MatchPtrs::iterator _sample_container_begin,
    containers::MatchPtrs::iterator _sample_container_end,
    std::vector<descriptors::Split> *_candidate_splits )
{
    // -----------------------------------------------------------------------

    debug_log( "try_window..." );

    // -----------------------------------------------------------------------

    containers::MatchPtrs::iterator null_values_separator =
        separate_null_values( _sample_container_begin, _sample_container_end );

    assert_true(
        std::distance( _sample_container_begin, null_values_separator ) == 0 );

    sort_by_numerical_value( null_values_separator, _sample_container_end );

    auto critical_values = calculate_critical_values_window(
        _delta_t, null_values_separator, _sample_container_end );

    // -----------------------------------------------------------------------
    // Add new splits to the candidate splits

    debug_log( "try_window: Add new splits." );

    for ( auto &critical_value : critical_values )
        {
            _candidate_splits->push_back( descriptors::Split(
                true, critical_value, _column_used, _data_used ) );
        }

    for ( auto &critical_value : critical_values )
        {
            _candidate_splits->push_back( descriptors::Split(
                false, critical_value, _column_used, _data_used ) );
        }

    // -----------------------------------------------------------------------
    // It is possible that std::distance( _sample_container_begin,
    // _sample_container_end ) is zero, when we are using the distributed
    // version. In that case we want this process to continue until this point,
    // because calculate_critical_values_numerical and
    // calculate_critical_values_discrete contains barriers and we want to
    // avoid a livelock.

    if ( std::distance( null_values_separator, _sample_container_end ) == 0 )
        {
            for ( size_t i = 0; i < critical_values.size() * 2; ++i )
                {
                    aggregation()
                        ->update_optimization_criterion_and_clear_updates_current(
                            0.0,  // _num_samples_smaller
                            0.0   // _num_samples_greater
                        );
                }

            return;
        }

    // -----------------------------------------------------------------------
    // Try applying outside the window

    debug_log( "try_window: Apply from above..." );

    // Apply changes and store resulting value of optimization criterion
    if ( is_activated_ )
        {
            debug_log( "Deactivate..." );

            aggregation()->deactivate_samples_outside_window(
                critical_values,
                _delta_t,
                aggregations::Revert::after_each_category,
                null_values_separator,
                _sample_container_end );
        }
    else
        {
            debug_log( "Activate..." );

            aggregation()->activate_samples_outside_window(
                critical_values,
                _delta_t,
                aggregations::Revert::after_each_category,
                null_values_separator,
                _sample_container_end );
        }

    // -----------------------------------------------------------------------
    // Try applying inside the window

    debug_log( "try_window: Apply from below..." );

    // Apply changes and store resulting value of optimization criterion
    if ( is_activated_ )
        {
            debug_log( "try_window: Deactivate..." );

            aggregation()->deactivate_samples_in_window(
                critical_values,
                _delta_t,
                aggregations::Revert::after_each_category,
                null_values_separator,
                _sample_container_end );
        }
    else
        {
            debug_log( "try_window: Activate..." );

            aggregation()->activate_samples_in_window(
                critical_values,
                _delta_t,
                aggregations::Revert::after_each_category,
                null_values_separator,
                _sample_container_end );
        }

    // -----------------------------------------------------------------------

    debug_log( "try_window...done." );

    // -----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace multirel
