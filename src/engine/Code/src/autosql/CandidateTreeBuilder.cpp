#include "decisiontrees/decisiontrees.hpp"

namespace autosql
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

void CandidateTreeBuilder::add_counts(
    const TableHolder &_table_holder,
    const std::vector<descriptors::SameUnits> &_same_units,
    const descriptors::Hyperparameters &_hyperparameters,
    const SQLNET_INT _ix_perip_used,
    std::mt19937 &_random_number_generator,
    containers::Optional<aggregations::AggregationImpl> &_aggregation_impl,
    SQLNET_COMMUNICATOR *_comm,
    std::list<DecisionTree> &_candidate_trees )
{
    for ( auto &agg : _hyperparameters.aggregations )
        {
            if ( agg != "COUNT" )
                {
                    continue;
                }

            _candidate_trees.push_back( DecisionTree(
                agg,
                -1,  // ix_column_used - not important
                DataUsed::not_applicable,  // data_used
                _ix_perip_used,
                _same_units[_ix_perip_used],
                _random_number_generator,
                _aggregation_impl ) );

#ifdef SQLNET_PARALLEL
            _candidate_trees.back().set_comm( _comm );
#endif  // SQLNET_PARALLEL
        }
}

// ----------------------------------------------------------------------------

void CandidateTreeBuilder::add_count_distincts(
    const TableHolder &_table_holder,
    const std::vector<descriptors::SameUnits> &_same_units,
    const descriptors::Hyperparameters &_hyperparameters,
    const SQLNET_INT _ix_perip_used,
    std::mt19937 &_random_number_generator,
    containers::Optional<aggregations::AggregationImpl> &_aggregation_impl,
    SQLNET_COMMUNICATOR *_comm,
    std::list<DecisionTree> &_candidate_trees )
{
    for ( auto &agg : _hyperparameters.aggregations )
        {
            if ( agg != "COUNT DISTINCT" &&
                 agg != "COUNT MINUS COUNT DISTINCT" )
                {
                    continue;
                }

            for ( auto data_used : {DataUsed::x_perip_categorical,
                                    DataUsed::x_perip_discrete,
                                    DataUsed::time_stamps_diff} )
                {
                    SQLNET_INT ncols = get_ncols(
                        _table_holder.peripheral_tables,
                        _same_units,
                        _ix_perip_used,
                        data_used );

                    for ( SQLNET_INT ix_column_used = 0; ix_column_used < ncols;
                          ++ix_column_used )
                        {
                            _candidate_trees.push_back( DecisionTree(
                                agg,
                                ix_column_used,
                                data_used,
                                _ix_perip_used,
                                _same_units[_ix_perip_used],
                                _random_number_generator,
                                _aggregation_impl ) );

#ifdef SQLNET_PARALLEL
                            _candidate_trees.back().set_comm( _comm );
#endif  // SQLNET_PARALLEL
                        }
                }
        }
}

// ----------------------------------------------------------------------------

void CandidateTreeBuilder::add_other_aggs(
    const TableHolder &_table_holder,
    const std::vector<descriptors::SameUnits> &_same_units,
    const descriptors::Hyperparameters &_hyperparameters,
    const SQLNET_INT _ix_perip_used,
    std::mt19937 &_random_number_generator,
    containers::Optional<aggregations::AggregationImpl> &_aggregation_impl,
    SQLNET_COMMUNICATOR *_comm,
    std::list<DecisionTree> &_candidate_trees )
{
    for ( auto &agg : _hyperparameters.aggregations )
        {
            if ( agg == "COUNT" || agg == "COUNT DISTINCT" ||
                 agg == "COUNT MINUS COUNT DISTINCT" )
                {
                    continue;
                }

            for ( auto data_used : {DataUsed::x_perip_numerical,
                                    DataUsed::x_perip_discrete,
                                    DataUsed::time_stamps_diff,
                                    DataUsed::same_unit_numerical,
                                    DataUsed::same_unit_discrete} )
                {
                    SQLNET_INT ncols = get_ncols(
                        _table_holder.peripheral_tables,
                        _same_units,
                        _ix_perip_used,
                        data_used );

                    for ( SQLNET_INT ix_column_used = 0; ix_column_used < ncols;
                          ++ix_column_used )
                        {
                            // -----------------------------------------------------

                            const bool comparison_only = is_comparison_only(
                                _table_holder,
                                data_used,
                                _ix_perip_used,
                                ix_column_used );

                            if ( comparison_only )
                                {
                                    continue;
                                }

                            // -----------------------------------------------------

                            _candidate_trees.push_back( DecisionTree(
                                agg,
                                ix_column_used,
                                data_used,
                                _ix_perip_used,
                                _same_units[_ix_perip_used],
                                _random_number_generator,
                                _aggregation_impl ) );

#ifdef SQLNET_PARALLEL
                            _candidate_trees.back().set_comm( _comm );
#endif  // SQLNET_PARALLEL
                        }
                }
        }
}

// ----------------------------------------------------------------------------

void CandidateTreeBuilder::add_subfeature_aggs(
    const TableHolder &_table_holder,
    const std::vector<descriptors::SameUnits> &_same_units,
    const descriptors::Hyperparameters &_hyperparameters,
    const SQLNET_INT _ix_perip_used,
    std::mt19937 &_random_number_generator,
    containers::Optional<aggregations::AggregationImpl> &_aggregation_impl,
    SQLNET_COMMUNICATOR *_comm,
    std::list<DecisionTree> &_candidate_trees )
{
    assert( _table_holder.subtables[_ix_perip_used] );

    for ( auto &agg : _hyperparameters.aggregations )
        {
            if ( agg == "COUNT" || agg == "COUNT DISTINCT" ||
                 agg == "COUNT MINUS COUNT DISTINCT" )
                {
                    continue;
                }

            for ( SQLNET_INT ix_column_used = 0;
                  ix_column_used < _hyperparameters.num_subfeatures;
                  ++ix_column_used )
                {
                    _candidate_trees.push_back( DecisionTree(
                        agg,
                        ix_column_used,
                        DataUsed::x_subfeature,
                        _ix_perip_used,
                        _same_units[_ix_perip_used],
                        _random_number_generator,
                        _aggregation_impl ) );

#ifdef SQLNET_PARALLEL
                    _candidate_trees.back().set_comm( _comm );
#endif  // SQLNET_PARALLEL
                }
        }
}

// ----------------------------------------------------------------------------

std::list<DecisionTree> CandidateTreeBuilder::build_candidates(
    const TableHolder &_table_holder,
    const std::vector<descriptors::SameUnits> &_same_units,
    const SQLNET_INT _ix_feature,
    descriptors::Hyperparameters _hyperparameters,
    containers::Optional<aggregations::AggregationImpl> &_aggregation_impl,
    std::mt19937 &_random_number_generator,
    SQLNET_COMMUNICATOR *_comm )
{
    // ----------------------------------------------------------------

    debug_message( "build_candidates..." );

    // ----------------------------------------------------------------

#ifdef SQLNET_PARALLEL

    auto candidate_trees = build_candidate_trees(
        _table_holder,
        _same_units,
        _hyperparameters,
        _random_number_generator,
        _aggregation_impl,
        _comm );

#else  // SQLNET_PARALLEL

    auto candidate_trees = build_candidate_trees(
        _table_holder,
        _same_units,
        _hyperparameters,
        _random_number_generator,
        _aggregation_impl );

#endif  // SQLNET_PARALLEL

    // ----------------------------------------------------------------

    // If _ix_feature is -1, then these are subtrees for which round_robin does
    // not make sense.
    if ( _hyperparameters.round_robin && _ix_feature != -1 )
        {
            debug_message( "fit: Applying round robin..." );

            CandidateTreeBuilder::round_robin( _ix_feature, candidate_trees );
        }
    else if ( _hyperparameters.share_aggregations >= 0.0 )
        {
            debug_message( "fit: Removing candidates..." );

#ifdef SQLNET_PARALLEL

            // This will remove all but share_aggregations of trees
            randomly_remove_candidate_trees(
                _hyperparameters.share_aggregations,
                _random_number_generator,
                candidate_trees,
                _comm );

#else  // SQLNET_PARALLEL

            // This will remove all but share_aggregations of trees
            randomly_remove_candidate_trees(
                _hyperparameters.share_aggregations,
                _random_number_generator,
                candidate_trees );

#endif  // SQLNET_PARALLEL
        }

    // ----------------------------------------------------------------

    debug_message( "build_candidates...done." );

    // ----------------------------------------------------------------

    return candidate_trees;

    // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::list<DecisionTree> CandidateTreeBuilder::build_candidate_trees(
    const TableHolder &_table_holder,
    const std::vector<descriptors::SameUnits> &_same_units,
    const descriptors::Hyperparameters _hyperparameters,
    std::mt19937 &_random_number_generator,
    containers::Optional<aggregations::AggregationImpl> &_aggregation_impl,
    SQLNET_COMMUNICATOR *_comm )
{
    const SQLNET_INT num_perips =
        static_cast<SQLNET_INT>( _table_holder.peripheral_tables.size() );

    std::list<DecisionTree> candidate_trees;

    for ( SQLNET_INT ix_perip_used = 0; ix_perip_used < num_perips;
          ++ix_perip_used )
        {
            // -----------------------------------------------------
            // COUNT is special, because the values it aggregates
            // do not matter. The only thing that matters is
            // which of the input tables are to be aggregated

            add_counts(
                _table_holder,
                _same_units,
                _hyperparameters,
                ix_perip_used,
                _random_number_generator,
                _aggregation_impl,
                _comm,
                candidate_trees );

            // ------------------------------------------------------------------
            // COUNT DISTINCT and COUNT MINUS COUNT DISTINCT are special,
            // because they are applied to
            // DataUsed::x_perip_categorical instead of
            // DataUsed::x_perip_numerical.

            add_count_distincts(
                _table_holder,
                _same_units,
                _hyperparameters,
                ix_perip_used,
                _random_number_generator,
                _aggregation_impl,
                _comm,
                candidate_trees );

            // ------------------------------------------------------------------
            // Now we apply all of the aggregations that are not COUNT or COUNT
            // DISTINCT

            add_other_aggs(
                _table_holder,
                _same_units,
                _hyperparameters,
                ix_perip_used,
                _random_number_generator,
                _aggregation_impl,
                _comm,
                candidate_trees );

            // ------------------------------------------------------------------
            // If applicable, add aggregations over the subfeatures

            if ( _table_holder.subtables[ix_perip_used] )
                {
                    add_subfeature_aggs(
                        _table_holder,
                        _same_units,
                        _hyperparameters,
                        ix_perip_used,
                        _random_number_generator,
                        _aggregation_impl,
                        _comm,
                        candidate_trees );
                }

            // ------------------------------------------------------------------
        }

    return candidate_trees;
}

// ----------------------------------------------------------------------------

SQLNET_INT CandidateTreeBuilder::get_ncols(
    const std::vector<containers::DataFrame> &_peripheral_tables,
    const std::vector<descriptors::SameUnits> &_same_units,
    const SQLNET_INT _ix_perip_used,
    const DataUsed _data_used )
{
    assert( _peripheral_tables.size() == _same_units.size() );

    assert( _ix_perip_used >= 0 );

    assert( _ix_perip_used < static_cast<SQLNET_INT>( _same_units.size() ) );

    switch ( _data_used )
        {
            case DataUsed::x_perip_numerical:
                return _peripheral_tables[_ix_perip_used].numerical().ncols();

            case DataUsed::x_perip_discrete:
                return _peripheral_tables[_ix_perip_used].discrete().ncols();

            case DataUsed::x_perip_categorical:
                return _peripheral_tables[_ix_perip_used].categorical().ncols();

            case DataUsed::time_stamps_diff:
                return 1;

            case DataUsed::same_unit_discrete:
                assert( _same_units[_ix_perip_used].same_units_discrete );
                return static_cast<SQLNET_INT>(
                    _same_units[_ix_perip_used].same_units_discrete->size() );

            case DataUsed::same_unit_numerical:
                assert( _same_units[_ix_perip_used].same_units_numerical );
                return static_cast<SQLNET_INT>(
                    _same_units[_ix_perip_used].same_units_numerical->size() );

            default:
                assert( !"Unknown DataUsed in get_ncols!" );
                return 0;
        }
}

// ----------------------------------------------------------------------------

bool CandidateTreeBuilder::is_comparison_only(
    const TableHolder &_table_holder,
    const DataUsed _data_used,
    const SQLNET_INT _ix_perip_used,
    const SQLNET_INT _ix_column_used )
{
    if ( _data_used == DataUsed::x_perip_numerical )
        {
            if ( _table_holder.peripheral_tables[_ix_perip_used]
                     .numerical()
                     .unit( _ix_column_used )
                     .find( "comparison "
                            "only" ) != std::string::npos )
                {
                    return true;
                }
        }
    else if ( _data_used == DataUsed::x_perip_discrete )
        {
            if ( _table_holder.peripheral_tables[_ix_perip_used]
                     .discrete()
                     .unit( _ix_column_used )
                     .find( "comparison "
                            "only" ) != std::string::npos )
                {
                    return true;
                }
        }

    return false;
}

// ----------------------------------------------------------------------------

void CandidateTreeBuilder::randomly_remove_candidate_trees(
    const SQLNET_FLOAT _share_aggregations,
    std::mt19937 &_random_number_generator,
    std::list<DecisionTree> &_candidate_trees,
    SQLNET_COMMUNICATOR *_comm )
{
    // ------------------------------------------------

    const SQLNET_SIZE num_candidates = std::max(
        static_cast<SQLNET_SIZE>(
            _candidate_trees.size() * _share_aggregations ),
        static_cast<SQLNET_SIZE>( 1 ) );

    RandomNumberGenerator rng( &_random_number_generator, _comm );

    // ------------------------------------------------

    while ( _candidate_trees.size() > num_candidates )
        {
            const auto max =
                static_cast<SQLNET_INT>( _candidate_trees.size() ) - 1;

            auto ix_remove = rng.random_int( 0, max );

            auto tree_to_be_removed = _candidate_trees.begin();

            std::advance( tree_to_be_removed, ix_remove );

            _candidate_trees.erase( tree_to_be_removed );
        }

    // ------------------------------------------------
}

// ----------------------------------------------------------------------------

/// For the round_robin approach, we remove all features but
/// one - the remaining one is a different one every time.
void CandidateTreeBuilder::round_robin(
    const SQLNET_INT _ix_feature, std::list<DecisionTree> &_candidate_trees )
{
    const SQLNET_SIZE ix_feature_size = static_cast<SQLNET_SIZE>( _ix_feature );

    auto it = _candidate_trees.begin();

    std::advance( it, ix_feature_size % _candidate_trees.size() );

    std::list<DecisionTree> candidate_trees;

    candidate_trees.emplace_back( std::move( *it ) );

    _candidate_trees = std::move( candidate_trees );
}

// ----------------------------------------------------------------------------
}
}
