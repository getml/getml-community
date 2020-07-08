#ifndef RELBOOST_ENSEMBLE_SUBTREEHELPER_HPP_
#define RELBOOST_ENSEMBLE_SUBTREEHELPER_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace ensemble
{
// ------------------------------------------------------------------------

class SubtreeHelper
{
   public:
    /// Fits the subensembles passed by the ensemble itself
    static void fit_subensembles(
        const std::shared_ptr<const TableHolder>& _table_holder,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const DecisionTreeEnsemble& _ensemble,
        const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
        multithreading::Communicator* _comm,
        std::vector<std::optional<DecisionTreeEnsemble>>* _subensembles_avg,
        std::vector<std::optional<DecisionTreeEnsemble>>* _subensembles_sum );

    /// Calls transform on the subfeatures, returning predictions.
    static std::vector<containers::Predictions> make_predictions(
        const TableHolder& _table_holder,
        const std::vector<std::optional<DecisionTreeEnsemble>>&
            _subensembles_avg,
        const std::vector<std::optional<DecisionTreeEnsemble>>&
            _subensembles_sum,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        multithreading::Communicator* _comm );

    /// Builds appropriate views on the features. The purpose of the ColumnView
    /// is to reverse the effect of the row indices in the DataFrameView.
    static std::vector<containers::Subfeatures> make_subfeatures(
        const TableHolder& _table_holder,
        const std::vector<containers::Predictions>& _predictions );

   private:
    /// Fits a subensemble for a single peripheral table, for a single
    /// intermediate aggregation.
    static void fit_subensemble(
        const std::string& _agg_type,
        const std::shared_ptr<const TableHolder>& _table_holder,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const std::shared_ptr<const std::map<Int, Int>>& _output_map,
        const Hyperparameters& _hyperparameters,
        const size_t _ix_perip_used,
        const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
        multithreading::Communicator* _comm,
        std::optional<DecisionTreeEnsemble>* _subensemble );

    /// Makes the predictions for a single subensemble
    static void make_predictions_for_one_subensemble(
        const std::optional<DecisionTreeEnsemble>& _subensemble,
        const TableHolder& _subtable_holder,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        multithreading::Communicator* _comm,
        containers::Predictions* _predictions );
};

// ------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relboost

#endif  // RELBOOST_ENSEMBLE_SUBTREEHELPER_HPP_

