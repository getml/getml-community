#include "autosql/ensemble/ensemble.hpp"

namespace autosql
{
namespace ensemble
{
// ------------------------------------------------------------------------

void TreeFitter::find_best_trees(
    const size_t _num_trees,
    const decisiontrees::TableHolder &_table_holder,
    const std::vector<containers::Subfeatures> &_subfeatures,
    const std::vector<AUTOSQL_FLOAT> &_values,
    std::vector<AUTOSQL_SAMPLES> *_samples,
    std::vector<AUTOSQL_SAMPLE_CONTAINER> *_sample_containers,
    optimizationcriteria::OptimizationCriterion *_optimization_criterion,
    std::list<decisiontrees::DecisionTree> *_candidate_trees,
    std::vector<decisiontrees::DecisionTree> *_trees )
{
    // -------------------------------------------------------------------
    // Identify and store best tree

    assert( _candidate_trees->size() == _values.size() );

    debug_log( "Identifying best feature..." );

    std::vector<std::tuple<size_t, AUTOSQL_FLOAT>> tuples;

    for ( size_t ix = 0; ix < _candidate_trees->size(); ++ix )
        {
            tuples.push_back( std::make_tuple( ix, _values[ix] ) );
        }

    std::sort(
        tuples.begin(),
        tuples.end(),
        []( const std::tuple<size_t, AUTOSQL_FLOAT> &t1,
            const std::tuple<size_t, AUTOSQL_FLOAT> &t2 ) {
            return std::get<1>( t1 ) > std::get<1>( t2 );
        } );

    for ( size_t i = 0; i < std::min( _num_trees, tuples.size() ); ++i )
        {
            if ( i > 0 && std::get<1>( tuples[i] ) <
                              tree_hyperparameters().regularization_ )
                {
                    break;
                }

            auto it = _candidate_trees->begin();

            std::advance( it, std::get<0>( tuples[i] ) );

            _trees->emplace_back( std::move( *it ) );
        }

    _candidate_trees->clear();

    // -------------------------------------------------------------------
    // Refit best tree, if necessary

    if ( tree_hyperparameters().max_length_probe_ <
         tree_hyperparameters().max_length_ )
        {
            /* fit_tree(
                 tree_hyperparameters().max_length_,
                 _samples,
                 _sample_containers,
                 _table_holder,
                 _optimization_criterion,
                 *_trees->last() );

                 */
        }

    // -------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void TreeFitter::fit(
    const decisiontrees::TableHolder &_table_holder,
    const std::vector<containers::Subfeatures> &_subfeatures,
    std::vector<AUTOSQL_SAMPLES> *_samples,
    std::vector<AUTOSQL_SAMPLE_CONTAINER> *_sample_containers,
    optimizationcriteria::OptimizationCriterion *_optimization_criterion,
    std::list<decisiontrees::DecisionTree> *_candidate_trees,
    std::vector<decisiontrees::DecisionTree> *_trees )
{
    // ----------------------------------------------------------------
    // In this section we just "probe" - we don't allow the tree to
    // grow to its full depth,  instead we just get an idea of what
    // might work best.

    std::vector<AUTOSQL_FLOAT> values;

    debug_log( "Fitter: Probing.." );

    probe(
        _table_holder,
        _subfeatures,
        _samples,
        _sample_containers,
        _optimization_criterion,
        _candidate_trees,
        &values );

    // -------------------------------------------------------------
    // Now that we have done the "probe", we identify which tree was
    // best and store the maximizing tree.

    debug_log( "Fitter: Storing best feature.." );

    find_best_trees(
        1,
        _table_holder,
        _subfeatures,
        values,
        _samples,
        _sample_containers,
        _optimization_criterion,
        _candidate_trees,
        _trees );

    // -------------------------------------------------------------
}

// ------------------------------------------------------------------------

void TreeFitter::fit_tree(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const containers::Subfeatures &_subfeatures,
    std::vector<AUTOSQL_SAMPLES> *_samples,
    std::vector<AUTOSQL_SAMPLE_CONTAINER> *_sample_containers,
    optimizationcriteria::OptimizationCriterion *_optimization_criterion,
    decisiontrees::DecisionTree *_tree )
{
    assert( _sample_containers->size() == _samples->size() );

    const auto ix_perip_used = _tree->column_to_be_aggregated().ix_perip_used;

    auto &samples = ( *_samples )[ix_perip_used];

    auto &sample_container = ( *_sample_containers )[ix_perip_used];

    assert(
        ix_perip_used <
        static_cast<AUTOSQL_INT>( _sample_containers->size() ) );

    auto null_values_dist = std::distance( samples.begin(), samples.begin() );

    if ( _tree->aggregation_type() != "COUNT" )
        {
            debug_log( "fit: Creating value to be aggregated.." );

            _tree->create_value_to_be_aggregated(
                _population, _peripheral, _subfeatures, sample_container );

            auto null_value_separator = _tree->separate_null_values( samples );

            null_values_dist =
                std::distance( samples.begin(), null_value_separator );

            debug_log(
                "null_values_dist: " + std::to_string( null_values_dist ) );

            _tree->set_samples_begin_end(
                samples.data() + null_values_dist,
                samples.data() + samples.size() );

            if ( _tree->aggregation_needs_sorting() )
                {
                    _tree->sort_samples( null_value_separator, samples.end() );
                }

            // NOTE: Samples are the actual samples, whereas
            // _sample_containers are pointers to samples!
            _tree->separate_null_values( sample_container );

            assert(
                null_values_dist ==
                std::distance(
                    sample_container.begin(),
                    _tree->separate_null_values( sample_container ) ) );
        }
    else
        {
            _tree->set_samples_begin_end(
                ( *_samples )[ix_perip_used].data(),
                ( *_samples )[ix_perip_used].data() +
                    ( *_samples )[ix_perip_used].size() );
        }

    // ---------------------------------------------------------

    debug_log( "fit: Fitting new candidate.." );

    _tree->fit(
        _population,
        _peripheral,
        _subfeatures,
        sample_container.begin(),
        sample_container.end(),
        _optimization_criterion );

    // ---------------------------------------------------------
}

// ------------------------------------------------------------------------

void TreeFitter::probe(
    const decisiontrees::TableHolder &_table_holder,
    const std::vector<containers::Subfeatures> &_subfeatures,
    std::vector<AUTOSQL_SAMPLES> *_samples,
    std::vector<AUTOSQL_SAMPLE_CONTAINER> *_sample_containers,
    optimizationcriteria::OptimizationCriterion *_optimization_criterion,
    std::list<decisiontrees::DecisionTree> *_candidate_trees,
    std::vector<AUTOSQL_FLOAT> *_values )
{
    for ( auto &tree : *_candidate_trees )
        {
            const auto ix = tree.ix_perip_used();

            assert( ix < _table_holder.main_tables_.size() );
            assert( _subfeatures.size() == _table_holder.main_tables_.size() );
            assert(
                _subfeatures.size() ==
                _table_holder.peripheral_tables_.size() );

            fit_tree(
                _table_holder.main_tables_[ix],
                _table_holder.peripheral_tables_[ix],
                _subfeatures[ix],
                _samples,
                _sample_containers,
                _optimization_criterion,
                &tree );

            _values->push_back( _optimization_criterion->value() );

            _optimization_criterion->reset();
        }
}

// ------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace autosql
