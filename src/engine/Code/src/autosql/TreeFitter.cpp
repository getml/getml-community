#include "decisiontrees/decisiontrees.hpp"

namespace autosql
{
namespace decisiontrees
{
// ------------------------------------------------------------------------

void TreeFitter::find_best_trees(
    const size_t _num_trees,
    const std::vector<AUTOSQL_FLOAT> &_values,
    std::vector<AUTOSQL_SAMPLES> &_samples,
    std::vector<AUTOSQL_SAMPLE_CONTAINER> &_sample_containers,
    TableHolder &_table_holder,
    optimizationcriteria::OptimizationCriterion *_optimization_criterion,
    std::list<DecisionTree> &_candidate_trees,
    std::vector<DecisionTree> &_trees )
{
    // -------------------------------------------------------------------
    // Identify and store best tree

    assert( _candidate_trees.size() == _values.size() );

    debug_message( "Identifying best feature..." );

    const auto ix_begin = _trees.size();

    std::vector<std::tuple<size_t, AUTOSQL_FLOAT>> tuples;

    for ( size_t ix = 0; ix < _candidate_trees.size(); ++ix )
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
                              tree_hyperparameters().regularization )
                {
                    break;
                }

            auto it = _candidate_trees.begin();

            std::advance( it, std::get<0>( tuples[i] ) );

            _trees.emplace_back( std::move( *it ) );
        }

    _candidate_trees.clear();

    for ( auto it = _trees.begin() + ix_begin; it != _trees.end(); ++it )
        {
            it->set_categories( categories() );
        }

    // ---------------î----------------------------------------------------
    // Refit best tree, if necessary

    if ( tree_hyperparameters().max_length_probe <
         tree_hyperparameters().max_length )
        {
            for ( auto it = _trees.begin() + ix_begin; it != _trees.end();
                  ++it )
                {
                    fit_tree(
                        tree_hyperparameters().max_length,
                        _samples,
                        _sample_containers,
                        _table_holder,
                        _optimization_criterion,
                        *it );
                }
        }

    // -------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void TreeFitter::fit(
    TableHolder &_table_holder,
    std::vector<AUTOSQL_SAMPLES> &_samples,
    std::vector<AUTOSQL_SAMPLE_CONTAINER> &_sample_containers,
    optimizationcriteria::OptimizationCriterion *_optimization_criterion,
    std::list<DecisionTree> &_candidate_trees,
    std::vector<DecisionTree> &_trees )
{
    // ----------------------------------------------------------------
    // Before can fit this tree, we must fit any existing subtrees.

    debug_message( "Fitter: Fitting subfeatures..." );

    fit_subtrees(
        _table_holder,
        _sample_containers,
        _optimization_criterion,
        _candidate_trees );

    // ----------------------------------------------------------------
    // In this section we just "probe" - we don't allow the tree to
    // grow to its full depth,  instead we just get an idea of what
    // might work best.

    std::vector<AUTOSQL_FLOAT> values;

    debug_message( "Fitter: Probing..." );

    probe(
        _samples,
        _sample_containers,
        _table_holder,
        _optimization_criterion,
        values,
        _candidate_trees );

    // -------------------------------------------------------------
    // Now that we have done the "probe", we identify which tree was
    // best and store the maximizing tree

    debug_message( "Fitter: Storing best feature..." );

    find_best_trees(
        1,
        values,
        _samples,
        _sample_containers,
        _table_holder,
        _optimization_criterion,
        _candidate_trees,
        _trees );

    // -------------------------------------------------------------
}

// ------------------------------------------------------------------------

void TreeFitter::fit_subtrees(
    TableHolder &_table_holder,
    const std::vector<AUTOSQL_SAMPLE_CONTAINER> &_sample_containers,
    optimizationcriteria::OptimizationCriterion *_opt,
    std::list<DecisionTree> &_candidate_trees )
{
    // ---------------------------------------------------------------

    assert( _sample_containers.size() == _table_holder.subtables.size() );

    // ---------------------------------------------------------------

    for ( size_t ix_subtable = 0; ix_subtable < _table_holder.subtables.size();
          ++ix_subtable )
        {
            if ( _table_holder.subtables[ix_subtable] )
                {
                    // ---------------------------------------------------------------
                    // For convenience

                    auto &subtable = *_table_holder.subtables[ix_subtable];

                    const auto num_peripheral =
                        subtable.peripheral_tables.size();

                    // ---------------------------------------------------------------
                    // Identify same units between the main table and the
                    // peripheral_tables

                    const auto same_units =
                        SameUnitIdentifier::identify_same_units(
                            subtable.peripheral_tables,
                            subtable.main_table.df() );

                    // ---------------------------------------------------------------
                    // Create the population indices

                    auto population_indices =
                        SampleContainer::create_population_indices(
                            subtable.main_table.df().nrows(),
                            _sample_containers[ix_subtable] );

                    subtable.main_table.set_indices( population_indices );

                    // ---------------------------------------------------------------
                    // The population indices map reverses the population
                    // indices

                    auto output_map = SampleContainer::create_output_map(
                        _table_holder.main_table.get_indices() );

                    // ---------------------------------------------------------------
                    // Create the new samples and sample containers

                    std::vector<AUTOSQL_SAMPLES> samples( num_peripheral );

                    std::vector<AUTOSQL_SAMPLE_CONTAINER> sample_containers(
                        num_peripheral );

                    SampleContainer::create_samples_and_sample_containers(
                        hyperparameters(),
                        subtable.peripheral_tables,
                        subtable.main_table,
                        samples,
                        sample_containers );

                    // ---------------------------------------------------------------

                    _table_holder.main_table.df().set_join_key_used(
                        ix_subtable );

                    _table_holder.main_table.df().set_time_stamps_used(
                        ix_subtable );

                    // ---------------------------------------------------------------
                    // The impl struct is to avoid having to reallocate the same
                    // data multiple times. Not that we still need to set the
                    // index and the optimizer.

                    const auto subview = containers::DataFrameView(
                        _table_holder.peripheral_tables[ix_subtable],
                        subtable.main_table.get_indices() );

                    const auto aggregation_index =
                        aggregations::AggregationIndex(
                            subview,
                            _table_holder.main_table,
                            output_map,
                            use_timestamps() );

                    auto opt_impl = std::make_shared<
                        aggregations::IntermediateAggregationImpl>(
                        _table_holder.main_table, aggregation_index, _opt );

                    auto aggregation_impl =
                        containers::Optional<aggregations::AggregationImpl>(
                            new aggregations::AggregationImpl(
                                subtable.main_table.nrows() ) );

                    // ---------------------------------------------------------------
                    // Fit appropriate subtrees for each of the candidates.

                    fit_subtrees_for_candidates(
                        static_cast<AUTOSQL_INT>( ix_subtable ),
                        subtable,
                        samples,
                        sample_containers,
                        same_units,
                        opt_impl,
                        aggregation_impl,
                        _candidate_trees );

                    // ---------------------------------------------------------------

                    _table_holder.main_table.df().set_join_key_used( -1 );

                    _table_holder.main_table.df().set_time_stamps_used( -1 );

                    // ---------------------------------------------------------------
                }
        }
}

// ------------------------------------------------------------------------

void TreeFitter::fit_subtrees_for_candidates(
    const AUTOSQL_INT _ix_subtable,
    TableHolder &_subtable,
    std::vector<AUTOSQL_SAMPLES> &_samples,
    std::vector<AUTOSQL_SAMPLE_CONTAINER> &_sample_containers,
    const std::vector<descriptors::SameUnits> &_same_units,
    std::shared_ptr<aggregations::IntermediateAggregationImpl> &_opt_impl,
    containers::Optional<aggregations::AggregationImpl> &_aggregation_impl,
    std::list<DecisionTree> &_candidate_trees )
{
    // ---------------------------------------------------------------

    debug_message( "fit_subtrees_for_candidates..." );

    // ---------------------------------------------------------------

    for ( auto it = _candidate_trees.begin(); it != _candidate_trees.end();
          ++it )
        {
            // ---------------------------------------------------------------

            if ( it->has_subtrees() )
                {
                    continue;
                }

            // ---------------------------------------------------------------

            if ( it->column_to_be_aggregated().ix_perip_used != _ix_subtable )
                {
                    continue;
                }

            // ---------------------------------------------------------------

            if ( it->intermediate_type() == "none" )
                {
                    continue;
                }

            // ---------------------------------------------------------------

            std::shared_ptr<optimizationcriteria::OptimizationCriterion>
                optimization_criterion = it->make_intermediate( _opt_impl );

            // ---------------------------------------------------------------

            std::vector<DecisionTree> subtrees;

            // ---------------------------------------------------------------

            while ( subtrees.size() <
                    static_cast<size_t>( hyperparameters().num_subfeatures ) )
                {
                    // ---------------------------------------------------------------
                    // Build candidates.

                    auto candidate_subtrees =
                        CandidateTreeBuilder::build_candidates(
                            _subtable,
                            _same_units,
                            -1,  // ix_features = -1 signals we do not want
                                 // round_robin.
                            hyperparameters(),
                            _aggregation_impl,
                            random_number_generator(),
                            comm() );

                    // ----------------------------------------------------------------
                    // Before can fit this tree, we must fit any existing
                    // subtrees.

                    debug_message( "Subfitter: Fitting subfeatures..." );

                    fit_subtrees(
                        _subtable,
                        _sample_containers,
                        optimization_criterion.get(),
                        candidate_subtrees );

                    // ----------------------------------------------------------------
                    // In this section we just "probe" - we don't allow the tree
                    // to grow to its full depth,  instead we just get an idea
                    // of what might work best.

                    std::vector<AUTOSQL_FLOAT> values;

                    debug_message( "Subfitter: Probing..." );

                    probe(
                        _samples,
                        _sample_containers,
                        _subtable,
                        optimization_criterion.get(),
                        values,
                        candidate_subtrees );

                    // ----------------------------------------------------------------
                    // Now, we identify the best trees

                    find_best_trees(
                        static_cast<size_t>(
                            hyperparameters().num_subfeatures ) -
                            subtrees.size(),
                        values,
                        _samples,
                        _sample_containers,
                        _subtable,
                        optimization_criterion.get(),
                        candidate_subtrees,
                        subtrees );

                    // ---------------------------------------------------------------
                }

            // ---------------------------------------------------------------
            // Under some circumstances, we do not need to retrain subtrees.

            for ( auto it2 = it; it2 != _candidate_trees.end(); ++it2 )
                {
                    const bool same_subtrees =
                        it2->column_to_be_aggregated().ix_perip_used ==
                            it->column_to_be_aggregated().ix_perip_used &&
                        it2->intermediate_type() == it->intermediate_type();

                    if ( same_subtrees )
                        {
                            assert( it2->has_subtrees() == false );
                            it2->set_subtrees( subtrees );
                        }
                }

            // ---------------------------------------------------------------
        }

    // ---------------------------------------------------------------

    debug_message( "fit_subtrees_for_candidates...done." );

    // ---------------------------------------------------------------
}

// ------------------------------------------------------------------------

void TreeFitter::fit_tree(
    const AUTOSQL_INT _max_length,
    std::vector<AUTOSQL_SAMPLES> &_samples,
    std::vector<AUTOSQL_SAMPLE_CONTAINER> &_sample_containers,
    TableHolder &_table_holder,
    optimizationcriteria::OptimizationCriterion *_optimization_criterion,
    DecisionTree &_tree )
{
    assert( _sample_containers.size() == _samples.size() );

    AUTOSQL_INT ix_perip_used = _tree.column_to_be_aggregated().ix_perip_used;

    assert(
        ix_perip_used < static_cast<AUTOSQL_INT>( _sample_containers.size() ) );

    auto null_values_dist = std::distance(
        _samples[ix_perip_used].begin(), _samples[ix_perip_used].begin() );

    if ( _tree.aggregation_type() != "COUNT" )
        {
            debug_message( "fit: Creating value to be aggregated..." );

            _tree.create_value_to_be_aggregated(
                _table_holder, _sample_containers[ix_perip_used] );

            auto null_value_separator =
                _tree.separate_null_values( _samples[ix_perip_used] );

            null_values_dist = std::distance(
                _samples[ix_perip_used].begin(), null_value_separator );

            debug_message(
                "null_values_dist: " + std::to_string( null_values_dist ) );

            _tree.set_samples_begin_end(
                _samples[ix_perip_used].data() + null_values_dist,
                _samples[ix_perip_used].data() +
                    _samples[ix_perip_used].size() );

            if ( _tree.aggregation_needs_sorting() )
                {
                    _tree.sort_samples(
                        null_value_separator, _samples[ix_perip_used].end() );
                }

            // NOTE: Samples are the actual samples, whereas
            // _sample_containers are pointers to samples!
            _tree.separate_null_values( _sample_containers[ix_perip_used] );

            assert(
                null_values_dist ==
                std::distance(
                    _sample_containers[ix_perip_used].begin(),
                    _tree.separate_null_values(
                        _sample_containers[ix_perip_used] ) ) );
        }
    else
        {
            _tree.set_samples_begin_end(
                _samples[ix_perip_used].data(),
                _samples[ix_perip_used].data() +
                    _samples[ix_perip_used].size() );
        }

    // ---------------------------------------------------------

    debug_message( "fit: Fitting new candidate..." );

    _tree.fit(
        _sample_containers[ix_perip_used].begin() + null_values_dist,
        _sample_containers[ix_perip_used].end(),
        _table_holder,
        _optimization_criterion,
        tree_hyperparameters().allow_sets,
        _max_length,
        tree_hyperparameters().min_num_samples,
        tree_hyperparameters().grid_factor,
        tree_hyperparameters().regularization,
        tree_hyperparameters().share_conditions,
        hyperparameters().use_timestamps );
}

// ------------------------------------------------------------------------

void TreeFitter::probe(
    std::vector<AUTOSQL_SAMPLES> &_samples,
    std::vector<AUTOSQL_SAMPLE_CONTAINER> &_sample_containers,
    TableHolder &_table_holder,
    optimizationcriteria::OptimizationCriterion *_optimization_criterion,
    std::vector<AUTOSQL_FLOAT> &_values,
    std::list<DecisionTree> &_candidate_trees )
{
    for ( auto &tree : _candidate_trees )
        {
            fit_tree(
                tree_hyperparameters().max_length_probe,
                _samples,
                _sample_containers,
                _table_holder,
                _optimization_criterion,
                tree );

            _values.push_back( _optimization_criterion->value() );

            _optimization_criterion->reset();
        }
}

// ------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace autosql
