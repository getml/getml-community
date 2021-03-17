#ifndef RELMT_ENSEMBLE_THREADUTILS_HPP_
#define RELMT_ENSEMBLE_THREADUTILS_HPP_

namespace relmt
{
namespace ensemble
{
// ----------------------------------------------------------------------------

class Threadutils
{
    // ------------------------------------------------------------------------

   public:
    /// Fits an ensemble.
    static void fit_ensemble(
        const size_t _this_thread_num,
        const std::vector<size_t> _thread_nums,
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const helpers::RowIndexContainer& _row_indices,
        const helpers::WordIndexContainer& _word_indices,
        const std::optional<const helpers::MappedContainer>& _mapped,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        multithreading::Communicator* _comm,
        ensemble::DecisionTreeEnsemble* _ensemble );

    /// Number of threads.
    static Int get_num_threads( const Int _num_threads );

    /// Generates features or predictions.
    static void transform_ensemble(
        const size_t _this_thread_num,
        const std::vector<size_t> _thread_nums,
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::optional<helpers::WordIndexContainer>& _word_indices,
        const std::optional<const helpers::MappedContainer>& _mapped,
        const std::vector<size_t>& _index,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const ensemble::DecisionTreeEnsemble& _ensemble,
        multithreading::Communicator* _comm,
        containers::Features* _features );

    // ------------------------------------------------------------------------

   private:
    /// Copies to the features.
    static void copy(
        const std::vector<size_t> _rows,
        const std::vector<Float>& _local_feature,
        std::vector<Float>* _global_feature );

    /// Fits the relmt ensemble as a feature learner.
    static void fit_as_feature_learner(
        const size_t _this_thread_num,
        const std::vector<size_t>& _thread_nums,
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const helpers::RowIndexContainer& _row_indices,
        const helpers::WordIndexContainer& _word_indices,
        const std::optional<const helpers::MappedContainer>& _mapped,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        multithreading::Communicator* _comm,
        ensemble::DecisionTreeEnsemble* _ensemble );

    /// Generates features.
    static void transform_as_feature_learner(
        const size_t _this_thread_num,
        const std::vector<size_t> _thread_nums,
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::optional<helpers::WordIndexContainer>& _word_indices,
        const std::optional<const helpers::MappedContainer>& _mapped,
        const std::vector<size_t>& _index,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const ensemble::DecisionTreeEnsemble& _ensemble,
        multithreading::Communicator* _comm,
        containers::Features* _features );

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relmt

// ----------------------------------------------------------------------------

#endif  // RELMT_ENSEMBLE_THREADUTILS_HPP_
