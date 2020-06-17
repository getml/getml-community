#ifndef MULTIREL_DECISIONTREES_SUBTREEHELPER_HPP_
#define MULTIREL_DECISIONTREES_SUBTREEHELPER_HPP_

// ----------------------------------------------------------------------------

namespace multirel
{
namespace ensemble
{
// ------------------------------------------------------------------------

class SubtreeHelper
{
   public:
    /// Fits the subensembles passed by the ensemble itself
    static void fit_subensembles(
        const std::shared_ptr<const decisiontrees::TableHolder>& _table_holder,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const DecisionTreeEnsemble& _ensemble,
        optimizationcriteria::OptimizationCriterion* _opt,
        multithreading::Communicator* _comm,
        std::vector<containers::Optional<DecisionTreeEnsemble>>*
            _subensembles_avg,
        std::vector<containers::Optional<DecisionTreeEnsemble>>*
            _subensembles_sum );

    /// Calls transform on the subfeatures, returning predictions.
    static std::vector<containers::Predictions> make_predictions(
        const decisiontrees::TableHolder& _table_holder,
        const std::vector<containers::Optional<DecisionTreeEnsemble>>&
            _subensembles_avg,
        const std::vector<containers::Optional<DecisionTreeEnsemble>>&
            _subensembles_sum,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        multithreading::Communicator* _comm );

    /// Builds appropriate views on the features. The purpose of the ColumnView
    /// is to reverse the effect of the row indices in the DataFrameView.
    static std::vector<containers::Subfeatures> make_subfeatures(
        const decisiontrees::TableHolder& _table_holder,
        const std::vector<containers::Predictions>& _predictions );

   private:
    /// Fits a subensemble for a single peripheral table, for a single
    /// IntermediateAggregation.
    template <typename AggType>
    static void fit_subensemble(
        const std::shared_ptr<const decisiontrees::TableHolder>& _table_holder,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const std::shared_ptr<const std::map<Int, Int>>& _output_map,
        const descriptors::Hyperparameters& _hyperparameters,
        const size_t _ix_perip_used,
        optimizationcriteria::OptimizationCriterion* _opt,
        multithreading::Communicator* _comm,
        DecisionTreeEnsemble* _subfeature );
};

// ------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace multirel

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

namespace multirel
{
namespace ensemble
{
// ----------------------------------------------------------------------------

template <typename AggType>
void SubtreeHelper::fit_subensemble(
    const std::shared_ptr<const decisiontrees::TableHolder>& _table_holder,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::shared_ptr<const std::map<Int, Int>>& _output_map,
    const descriptors::Hyperparameters& _hyperparameters,
    const size_t _ix_perip_used,
    optimizationcriteria::OptimizationCriterion* _opt,
    multithreading::Communicator* _comm,
    DecisionTreeEnsemble* _subensemble )
{
    const auto subtable_holder =
        std::make_shared<const decisiontrees::TableHolder>(
            *_table_holder->subtables_[_ix_perip_used] );

    assert_true( subtable_holder->main_tables_.size() > 0 );

    const auto input_table = containers::DataFrameView(
        _table_holder->peripheral_tables_[_ix_perip_used],
        subtable_holder->main_tables_[0].rows_ptr() );

    // The input map is needed for propagating sampling.
    const auto input_map =
        utils::Mapper::create_rows_map( input_table.rows_ptr() );

    const auto aggregation_index = aggregations::AggregationIndex(
        input_table,
        _table_holder->main_tables_[_ix_perip_used],
        input_map,
        _output_map,
        _hyperparameters.use_timestamps_ );

    const auto opt_impl =
        std::make_shared<aggregations::IntermediateAggregationImpl>(
            _table_holder->main_tables_[0].nrows(), aggregation_index, _opt );

    const auto intermediate_agg =
        std::unique_ptr<optimizationcriteria::OptimizationCriterion>(
            new aggregations::IntermediateAggregation<AggType>( opt_impl ) );

    const auto silent = _hyperparameters.silent_;

    _subensemble->fit(
        subtable_holder,
        _logger,
        _hyperparameters.num_subfeatures_,
        intermediate_agg.get(),
        _comm );

    _opt->reset_yhat_old();
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace multirel

#endif  // MULTIREL_DECISIONTREES_SUBTREEHELPER_HPP_
