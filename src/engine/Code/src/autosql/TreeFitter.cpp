#include "autosql/ensemble/ensemble.hpp"

namespace autosql
{
namespace ensemble
{
// ------------------------------------------------------------------------

void TreeFitter::find_best_trees(
    const size_t _num_trees,
    const decisiontrees::TableHolder &_table_holder,
    const std::vector<containers::ColumnView<
        AUTOSQL_FLOAT,
        std::map<AUTOSQL_INT, AUTOSQL_INT>>> &_subfeatures,
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

    const auto ix_begin = _trees->size();

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

    for ( auto it = _trees->begin() + ix_begin; it != _trees->end(); ++it )
        {
            it->set_categories( categories() );
        }

    // ---------------î----------------------------------------------------
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
                 *_trees->last() );*/
        }

    // -------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void TreeFitter::fit(
    const decisiontrees::TableHolder &_table_holder,
    std::vector<AUTOSQL_SAMPLES> *_samples,
    std::vector<AUTOSQL_SAMPLE_CONTAINER> *_sample_containers,
    optimizationcriteria::OptimizationCriterion *_optimization_criterion,
    std::list<decisiontrees::DecisionTree> *_candidate_trees,
    std::vector<decisiontrees::DecisionTree> *_trees )
{
    // ----------------------------------------------------------------
    // Before can fit this tree, we must fit any existing subtrees.

    debug_log( "Fitter: Fitting subfeatures.." );

    /*fit_subtrees(
        _table_holder,
        _sample_containers,
        _optimization_criterion,
        _candidate_trees );*/

    // ----------------------------------------------------------------
    // Build subfeatures

    const std::vector<containers::ColumnView<
        AUTOSQL_FLOAT,
        std::map<AUTOSQL_INT, AUTOSQL_INT>>>
        subfeatures;

    // ----------------------------------------------------------------
    // In this section we just "probe" - we don't allow the tree to
    // grow to its full depth,  instead we just get an idea of what
    // might work best.

    std::vector<AUTOSQL_FLOAT> values;

    debug_log( "Fitter: Probing.." );

    probe(
        _table_holder,
        subfeatures,
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
        subfeatures,
        values,
        _samples,
        _sample_containers,
        _optimization_criterion,
        _candidate_trees,
        _trees );

    // -------------------------------------------------------------
}

// ------------------------------------------------------------------------

void TreeFitter::fit_subtrees(
    decisiontrees::TableHolder &_table_holder,
    const std::vector<AUTOSQL_SAMPLE_CONTAINER> &_sample_containers,
    optimizationcriteria::OptimizationCriterion *_opt,
    std::list<decisiontrees::DecisionTree> &_candidate_trees )
{
    // ---------------------------------------------------------------

    assert( _sample_containers.size() == _table_holder.subtables_.size() );

    // ---------------------------------------------------------------

    for ( size_t ix_subtable = 0; ix_subtable < _table_holder.subtables_.size();
          ++ix_subtable )
        {
            if ( _table_holder.subtables_[ix_subtable] )
                {
                    // ---------------------------------------------------------------
                    // For convenience

                    auto &subtable = *_table_holder.subtables_[ix_subtable];

                    // const auto num_peripheral =
                    //    subtable.peripheral_tables_.size();

                    // ---------------------------------------------------------------
                    // Identify same units between the main table and the
                    // peripheral_tables

                    assert( subtable.main_tables_.size() > 0 );

                    const auto same_units =
                        SameUnitIdentifier::identify_same_units(
                            subtable.peripheral_tables_,
                            subtable.main_tables_[0].df() );

                    // ---------------------------------------------------------------
                    // The population indices map reverses the population
                    // indices.

                    assert( false && "ToDo" );

                    /* auto output_map = SampleContainer::create_output_map(
                         _table_holder.main_tables_.get_indices() );

                     //
                     ---------------------------------------------------------------
                     // Create the new samples and sample containers

                     std::vector<AUTOSQL_SAMPLES> samples( num_peripheral );

                     std::vector<AUTOSQL_SAMPLE_CONTAINER> sample_containers(
                         num_peripheral );

                     SampleContainer::create_samples_and_sample_containers(
                         hyperparameters(),
                         subtable.peripheral_tables_,
                         subtable.main_tables_,
                         samples,
                         sample_containers );

                     //
                     ---------------------------------------------------------------
                     // The impl struct is to avoid having to reallocate the
                     same
                     // data multiple times. Not that we still need to set the
                     // index and the optimizer.

                     const auto subview = containers::DataFrameView(
                         _table_holder.peripheral_tables_[ix_subtable],
                         subtable.main_tables_.get_indices() );

                     const auto aggregation_index =
                         aggregations::AggregationIndex(
                             subview,
                             _table_holder.main_tables_,
                             output_map,
                             use_timestamps() );

                     auto opt_impl = std::make_shared<
                         aggregations::IntermediateAggregationImpl>(
                         _table_holder.main_tables_, aggregation_index, _opt );

                     auto aggregation_impl =
                         containers::Optional<aggregations::AggregationImpl>(
                             new aggregations::AggregationImpl(
                                 subtable.main_tables_.nrows() ) );

                     //
                     ---------------------------------------------------------------
                     // Fit appropriate subtrees for each of the candidates.

                     fit_subtrees_for_candidates(
                         static_cast<AUTOSQL_INT>( ix_subtable ),
                         subtable,
                         samples,
                         sample_containers,
                         same_units,
                         opt_impl,
                         aggregation_impl,
                         _candidate_trees );*/

                    // ---------------------------------------------------------------
                }
        }
}

// ------------------------------------------------------------------------

void TreeFitter::fit_subtrees_for_candidates(
    const AUTOSQL_INT _ix_subtable,
    decisiontrees::TableHolder &_subtable,
    std::vector<AUTOSQL_SAMPLES> &_samples,
    std::vector<AUTOSQL_SAMPLE_CONTAINER> &_sample_containers,
    const std::vector<descriptors::SameUnits> &_same_units,
    std::shared_ptr<aggregations::IntermediateAggregationImpl> &_opt_impl,
    containers::Optional<aggregations::AggregationImpl> &_aggregation_impl,
    std::list<decisiontrees::DecisionTree> &_candidate_trees )
{
    // ---------------------------------------------------------------

    debug_log( "fit_subtrees_for_candidates.." );

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

            std::vector<decisiontrees::DecisionTree> subtrees;

            // ---------------------------------------------------------------

            while ( subtrees.size() <
                    static_cast<size_t>( hyperparameters().num_subfeatures_ ) )
                {
                    // ---------------------------------------------------------------
                    // Build candidates.

                    auto candidate_subtrees =
                        CandidateTreeBuilder::build_candidates(
                            _subtable,
                            _same_units,
                            -1,  // ix_features = -1 signals we do not want
                                 // round_robin
                            hyperparameters(),
                            &_aggregation_impl,
                            &random_number_generator(),
                            comm() );

                    // ----------------------------------------------------------------
                    // Before can fit this tree, we must fit any existing
                    // subtrees.

                    debug_log( "Subfitter: Fitting subfeatures.." );

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

                    debug_log( "Subfitter: Probing.." );

                    /*probe(
                        _samples,
                        _sample_containers,
                        _subtable,
                        optimization_criterion.get(),
                        values,
                        candidate_subtrees );*/

                    // ----------------------------------------------------------------
                    // Now, we identify the best trees

                    /* find_best_trees(
                         static_cast<size_t>(
                             hyperparameters().num_subfeatures_ ) -
                             subtrees.size(),
                         values,
                         _samples,
                         _sample_containers,
                         _subtable,
                         optimization_criterion.get(),
                         candidate_subtrees,
                         subtrees );*/

                    // ---------------------------------------------------------------
                }

            // ---------------------------------------------------------------
            // Under some circumstances, we can simply copy existing subtrees.

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

    debug_log( "fit_subtrees_for_candidates..done." );

    // ---------------------------------------------------------------
}

// ------------------------------------------------------------------------

void TreeFitter::fit_tree(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const std::vector<containers::ColumnView<
        AUTOSQL_FLOAT,
        std::map<AUTOSQL_INT, AUTOSQL_INT>>> &_subfeatures,
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
    const std::vector<containers::ColumnView<
        AUTOSQL_FLOAT,
        std::map<AUTOSQL_INT, AUTOSQL_INT>>> &_subfeatures,
    std::vector<AUTOSQL_SAMPLES> *_samples,
    std::vector<AUTOSQL_SAMPLE_CONTAINER> *_sample_containers,
    optimizationcriteria::OptimizationCriterion *_optimization_criterion,
    std::list<decisiontrees::DecisionTree> *_candidate_trees,
    std::vector<AUTOSQL_FLOAT> *_values )
{
    for ( auto &tree : *_candidate_trees )
        {
            const auto ix = tree.ix_perip_used();

            fit_tree(
                _table_holder.main_tables_[ix],
                _table_holder.peripheral_tables_[ix],
                _subfeatures,
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
