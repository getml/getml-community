#include "multirel/ensemble/ensemble.hpp"

namespace multirel
{
namespace ensemble
{
// ----------------------------------------------------------------------------

void CandidateTreeBuilder::add_counts(
    const decisiontrees::TableHolder &_table_holder,
    const std::vector<descriptors::SameUnits> &_same_units,
    const descriptors::Hyperparameters &_hyperparameters,
    const Int _ix_perip_used,
    std::mt19937 *_random_number_generator,
    multithreading::Communicator *_comm,
    std::list<decisiontrees::DecisionTree> *_candidate_trees )
{
    for ( auto &agg : _hyperparameters.aggregations_ )
        {
            if ( agg != "COUNT" )
                {
                    continue;
                }

            _candidate_trees->push_back( decisiontrees::DecisionTree(
                agg,
                _hyperparameters.tree_hyperparameters_,
                _ix_perip_used,
                enums::DataUsed::not_applicable,  // data_used
                0,  // _ix_column_used - not important
                _same_units[_ix_perip_used],
                _random_number_generator,
                _comm ) );
        }
}

// ----------------------------------------------------------------------------

void CandidateTreeBuilder::add_count_distincts(
    const decisiontrees::TableHolder &_table_holder,
    const std::vector<descriptors::SameUnits> &_same_units,
    const descriptors::Hyperparameters &_hyperparameters,
    const Int _ix_perip_used,
    std::mt19937 *_random_number_generator,
    multithreading::Communicator *_comm,
    std::list<decisiontrees::DecisionTree> *_candidate_trees )
{
    for ( auto &agg : _hyperparameters.aggregations_ )
        {
            if ( agg != "COUNT DISTINCT" &&
                 agg != "COUNT MINUS COUNT DISTINCT" )
                {
                    continue;
                }

            for ( auto data_used : {enums::DataUsed::x_perip_categorical,
                                    enums::DataUsed::x_perip_discrete} )
                {
                    size_t ncols = get_ncols(
                        _table_holder.peripheral_tables_,
                        _same_units,
                        _ix_perip_used,
                        data_used );

                    for ( size_t ix_column_used = 0; ix_column_used < ncols;
                          ++ix_column_used )
                        {
                            _candidate_trees->push_back(
                                decisiontrees::DecisionTree(
                                    agg,
                                    _hyperparameters.tree_hyperparameters_,
                                    _ix_perip_used,
                                    data_used,
                                    ix_column_used,
                                    _same_units[_ix_perip_used],
                                    _random_number_generator,
                                    _comm ) );
                        }
                }
        }
}

// ----------------------------------------------------------------------------

void CandidateTreeBuilder::add_other_aggs(
    const decisiontrees::TableHolder &_table_holder,
    const std::vector<descriptors::SameUnits> &_same_units,
    const descriptors::Hyperparameters &_hyperparameters,
    const Int _ix_perip_used,
    std::mt19937 *_random_number_generator,
    multithreading::Communicator *_comm,
    std::list<decisiontrees::DecisionTree> *_candidate_trees )
{
    assert_true( _ix_perip_used >= 0 );

    assert_true(
        static_cast<size_t>( _ix_perip_used ) <
        _table_holder.peripheral_tables_.size() );

    for ( auto &agg : _hyperparameters.aggregations_ )
        {
            if ( agg == "COUNT" || agg == "COUNT DISTINCT" ||
                 agg == "COUNT MINUS COUNT DISTINCT" )
                {
                    continue;
                }

            if ( skip_first_last(
                     agg,
                     _table_holder.peripheral_tables_.at( _ix_perip_used ) ) )
                {
                    continue;
                }

            for ( auto data_used : {enums::DataUsed::x_perip_numerical,
                                    enums::DataUsed::x_perip_discrete,
                                    enums::DataUsed::same_unit_numerical,
                                    enums::DataUsed::same_unit_discrete} )
                {
                    size_t ncols = get_ncols(
                        _table_holder.peripheral_tables_,
                        _same_units,
                        _ix_perip_used,
                        data_used );

                    for ( size_t ix_column_used = 0; ix_column_used < ncols;
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

                            data_used = to_ts(
                                _table_holder,
                                _same_units,
                                data_used,
                                _ix_perip_used,
                                ix_column_used );

                            // -----------------------------------------------------

                            _candidate_trees->push_back(
                                decisiontrees::DecisionTree(
                                    agg,
                                    _hyperparameters.tree_hyperparameters_,
                                    _ix_perip_used,
                                    data_used,
                                    ix_column_used,
                                    _same_units[_ix_perip_used],
                                    _random_number_generator,
                                    _comm ) );

                            // -----------------------------------------------------
                        }
                }
        }
}

// ----------------------------------------------------------------------------

void CandidateTreeBuilder::add_subfeature_aggs(
    const decisiontrees::TableHolder &_table_holder,
    const std::vector<descriptors::SameUnits> &_same_units,
    const descriptors::Hyperparameters &_hyperparameters,
    const Int _ix_perip_used,
    std::mt19937 *_random_number_generator,
    multithreading::Communicator *_comm,
    std::list<decisiontrees::DecisionTree> *_candidate_trees )
{
    assert_true( _table_holder.subtables_[_ix_perip_used] );

    for ( auto &agg : _hyperparameters.aggregations_ )
        {
            if ( agg == "COUNT" || agg == "COUNT DISTINCT" ||
                 agg == "COUNT MINUS COUNT DISTINCT" )
                {
                    continue;
                }

            for ( Int ix_column_used = 0;
                  ix_column_used < _hyperparameters.num_subfeatures_ * 2;
                  ++ix_column_used )
                {
                    _candidate_trees->push_back( decisiontrees::DecisionTree(
                        agg,
                        _hyperparameters.tree_hyperparameters_,
                        _ix_perip_used,
                        enums::DataUsed::x_subfeature,
                        ix_column_used,
                        _same_units[_ix_perip_used],
                        _random_number_generator,
                        _comm ) );
                }
        }
}

// ----------------------------------------------------------------------------

std::list<decisiontrees::DecisionTree> CandidateTreeBuilder::build_candidates(
    const decisiontrees::TableHolder &_table_holder,
    const std::vector<descriptors::SameUnits> &_same_units,
    const size_t _ix_feature,
    const descriptors::Hyperparameters &_hyperparameters,
    std::mt19937 *_random_number_generator,
    multithreading::Communicator *_comm )
{
    // ----------------------------------------------------------------

    auto candidate_trees = build_candidate_trees(
        _table_holder,
        _same_units,
        _hyperparameters,
        _random_number_generator,
        _comm );

    // ----------------------------------------------------------------

    // If _ix_feature is -1, then these are subtrees for which round_robin does
    // not make sense.
    if ( _hyperparameters.round_robin_ && _ix_feature != -1 )
        {
            CandidateTreeBuilder::round_robin( _ix_feature, &candidate_trees );
        }
    else if ( _hyperparameters.share_aggregations_ >= 0.0 )
        {
            randomly_remove_candidate_trees(
                _hyperparameters.share_aggregations_,
                _random_number_generator,
                _comm,
                &candidate_trees );
        }

    // ----------------------------------------------------------------

    return candidate_trees;

    // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::list<decisiontrees::DecisionTree>
CandidateTreeBuilder::build_candidate_trees(
    const decisiontrees::TableHolder &_table_holder,
    const std::vector<descriptors::SameUnits> &_same_units,
    const descriptors::Hyperparameters _hyperparameters,
    std::mt19937 *_random_number_generator,
    multithreading::Communicator *_comm )
{
    const auto num_perips = _table_holder.peripheral_tables_.size();

    std::list<decisiontrees::DecisionTree> candidate_trees;

    for ( size_t ix_perip_used = 0; ix_perip_used < num_perips;
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
                static_cast<Int>( ix_perip_used ),
                _random_number_generator,
                _comm,
                &candidate_trees );

            // ------------------------------------------------------------------
            // COUNT DISTINCT and COUNT MINUS COUNT DISTINCT are special,
            // because they are applied to
            // enums::DataUsed::x_perip_categorical instead of
            // enums::DataUsed::x_perip_numerical.

            add_count_distincts(
                _table_holder,
                _same_units,
                _hyperparameters,
                static_cast<Int>( ix_perip_used ),
                _random_number_generator,
                _comm,
                &candidate_trees );

            // ------------------------------------------------------------------
            // Now we apply all of the aggregations.

            add_other_aggs(
                _table_holder,
                _same_units,
                _hyperparameters,
                static_cast<Int>( ix_perip_used ),
                _random_number_generator,
                _comm,
                &candidate_trees );

            // ------------------------------------------------------------------
            // If applicable, add aggregations over the subfeatures

            if ( _table_holder.subtables_[ix_perip_used] )
                {
                    add_subfeature_aggs(
                        _table_holder,
                        _same_units,
                        _hyperparameters,
                        static_cast<Int>( ix_perip_used ),
                        _random_number_generator,
                        _comm,
                        &candidate_trees );
                }

            // ------------------------------------------------------------------
        }

    return candidate_trees;
}

// ----------------------------------------------------------------------------

size_t CandidateTreeBuilder::get_ncols(
    const std::vector<containers::DataFrame> &_peripheral_tables,
    const std::vector<descriptors::SameUnits> &_same_units,
    const size_t _ix_perip_used,
    const enums::DataUsed _data_used )
{
    assert_true( _peripheral_tables.size() == _same_units.size() );

    assert_true( _ix_perip_used < _same_units.size() );

    switch ( _data_used )
        {
            case enums::DataUsed::x_perip_numerical:
                return _peripheral_tables[_ix_perip_used].num_numericals();

            case enums::DataUsed::x_perip_discrete:
                return _peripheral_tables[_ix_perip_used].num_discretes();

            case enums::DataUsed::x_perip_categorical:
                return _peripheral_tables[_ix_perip_used].num_categoricals();

            case enums::DataUsed::same_unit_discrete:
                assert_true( _same_units[_ix_perip_used].same_units_discrete_ );
                return static_cast<Int>(
                    _same_units[_ix_perip_used].same_units_discrete_->size() );

            case enums::DataUsed::same_unit_numerical:
                assert_true(
                    _same_units[_ix_perip_used].same_units_numerical_ );
                return static_cast<Int>(
                    _same_units[_ix_perip_used].same_units_numerical_->size() );

            default:
                assert_true( !"Unknown enums::DataUsed in get_ncols!" );
                return 0;
        }
}

// ----------------------------------------------------------------------------

bool CandidateTreeBuilder::is_comparison_only(
    const decisiontrees::TableHolder &_table_holder,
    const enums::DataUsed _data_used,
    const size_t _ix_perip_used,
    const size_t _ix_column_used )
{
    switch ( _data_used )
        {
            case enums::DataUsed::x_perip_numerical:
                {
                    const bool contains_comparison_only =
                        _table_holder.peripheral_tables_[_ix_perip_used]
                            .numerical_unit( _ix_column_used )
                            .find(
                                "comparison "
                                "only" ) != std::string::npos;

                    return contains_comparison_only;
                }

            case enums::DataUsed::x_perip_discrete:
                {
                    const bool contains_comparison_only =
                        _table_holder.peripheral_tables_[_ix_perip_used]
                            .discrete_unit( _ix_column_used )
                            .find(
                                "comparison "
                                "only" ) != std::string::npos;

                    return contains_comparison_only;
                }

            default:
                return false;
        }
}

// ----------------------------------------------------------------------------

void CandidateTreeBuilder::randomly_remove_candidate_trees(
    const Float _share_aggregations,
    std::mt19937 *_random_number_generator,
    multithreading::Communicator *_comm,
    std::list<decisiontrees::DecisionTree> *_candidate_trees )
{
    // ------------------------------------------------

    const size_t num_candidates = std::max(
        static_cast<size_t>( _candidate_trees->size() * _share_aggregations ),
        static_cast<size_t>( 1 ) );

    utils::RandomNumberGenerator rng( _random_number_generator, _comm );

    // ------------------------------------------------

    assert_true( _candidate_trees->size() > 0 );

    if ( num_candidates == 1 )
        {
            const auto max = static_cast<Int>( _candidate_trees->size() ) - 1;

            const auto ix_keep = rng.random_int( 0, max );

            auto it = _candidate_trees->begin();

            std::advance( it, ix_keep );

            *_candidate_trees = std::list<decisiontrees::DecisionTree>( {*it} );

            return;
        }

    // ------------------------------------------------

    while ( _candidate_trees->size() > num_candidates )
        {
            const auto max = static_cast<Int>( _candidate_trees->size() ) - 1;

            auto ix_remove = rng.random_int( 0, max );

            auto tree_to_be_removed = _candidate_trees->begin();

            std::advance( tree_to_be_removed, ix_remove );

            _candidate_trees->erase( tree_to_be_removed );
        }

    // ------------------------------------------------
}

// ----------------------------------------------------------------------------

/// For the round_robin approach, we remove all features but
/// one - the remaining one is a different one every time.
void CandidateTreeBuilder::round_robin(
    const size_t _ix_feature,
    std::list<decisiontrees::DecisionTree> *_candidate_trees )
{
    const size_t ix_feature_size = static_cast<size_t>( _ix_feature );

    auto it = _candidate_trees->begin();

    std::advance( it, ix_feature_size % _candidate_trees->size() );

    std::list<decisiontrees::DecisionTree> candidate_trees;

    candidate_trees.emplace_back( std::move( *it ) );

    *_candidate_trees = std::move( candidate_trees );
}

// ----------------------------------------------------------------------------

bool CandidateTreeBuilder::skip_first_last(
    const std::string &_agg, const containers::DataFrame &_peripheral )
{
    if ( _agg != "FIRST" && _agg != "LAST" )
        {
            return false;
        }

    return ( _peripheral.num_time_stamps() == 0 );
}

// ----------------------------------------------------------------------------

enums::DataUsed CandidateTreeBuilder::to_ts(
    const decisiontrees::TableHolder &_table_holder,
    const std::vector<descriptors::SameUnits> &_same_units,
    const enums::DataUsed _data_used,
    const size_t _ix_perip_used,
    const size_t _ix_column_used )
{
    // ------------------------------------------------------------------

    assert_true(
        _table_holder.main_tables_.size() ==
        _table_holder.peripheral_tables_.size() );

    assert_true( _table_holder.main_tables_.size() == _same_units.size() );

    assert_true( _ix_perip_used < _table_holder.peripheral_tables_.size() );

    const auto population = _table_holder.main_tables_.at( _ix_perip_used );

    const auto peripheral =
        _table_holder.peripheral_tables_.at( _ix_perip_used );

    // ------------------------------------------------------------------

    switch ( _data_used )
        {
            case enums::DataUsed::same_unit_numerical:
                {
                    const auto same_unit =
                        _same_units.at( _ix_perip_used ).same_units_numerical_;

                    assert_true( same_unit );

                    const auto is_ts = _same_units.at( _ix_perip_used )
                                           .is_ts(
                                               population,
                                               peripheral,
                                               *same_unit,
                                               _ix_column_used );

                    if ( is_ts )
                        {
                            return enums::DataUsed::same_unit_numerical_ts;
                        }

                    return _data_used;
                }

            case enums::DataUsed::same_unit_discrete:
                {
                    const auto same_unit =
                        _same_units.at( _ix_perip_used ).same_units_discrete_;

                    assert_true( same_unit );

                    const auto is_ts = _same_units.at( _ix_perip_used )
                                           .is_ts(
                                               population,
                                               peripheral,
                                               *same_unit,
                                               _ix_column_used );

                    if ( is_ts )
                        {
                            return enums::DataUsed::same_unit_discrete_ts;
                        }

                    return _data_used;
                }

            default:
                return _data_used;
        }

    // ------------------------------------------------------------------

    return _data_used;

    // ------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace multirel
