#ifndef ENGINE_PIPELINES_FITPARAMS_HPP_
#define ENGINE_PIPELINES_FITPARAMS_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace pipelines
{
// ----------------------------------------------------------------------------

struct FitParams
{
    /// The categorical encoding.
    const std::shared_ptr<containers::Encoding> categories_;

    /// The command used.
    const Poco::JSON::Object cmd_;

    /// Contains all of the data frames - we need this, because it might be
    /// possible that the features are retrieved.
    const std::map<std::string, containers::DataFrame> data_frames_;

    /// Keeps track of the data frames and their fingerprints.
    const dependency::DataFrameTracker data_frame_tracker_;

    /// The dependency tracker for the feature learners.
    const std::shared_ptr<dependency::FETracker> fe_tracker_;

    /// Logs the progress.
    const std::shared_ptr<const communication::Logger> logger_;

    /// The peripheral tables.
    const std::vector<containers::DataFrame> peripheral_dfs_;

    /// The population table.
    const containers::DataFrame population_df_;

    /// The dependency tracker for the predictors.
    const std::shared_ptr<dependency::PredTracker> pred_tracker_;

    /// The dependency tracker for the preprocessors.
    const std::shared_ptr<dependency::PreprocessorTracker>
        preprocessor_tracker_;

    /// The population table used for validation (only relevant for
    /// early stopping).
    const std::optional<containers::DataFrame> validation_df_;

    /// Output: The socket with which we communicate.
    Poco::Net::StreamSocket* const socket_ = nullptr;
};

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_FITPARAMS_HPP_
