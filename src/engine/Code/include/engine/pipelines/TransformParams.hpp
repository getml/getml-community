#ifndef ENGINE_PIPELINES_TRANSFORMPARAMS_HPP_
#define ENGINE_PIPELINES_TRANSFORMPARAMS_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace pipelines
{
// ----------------------------------------------------------------------------

struct TransformParams
{
    static constexpr const char* FEATURE_SELECTOR = "feature selector";
    static constexpr const char* PREDICTOR = "predictor";

    /// The categorical encoding.
    const std::shared_ptr<containers::Encoding> categories_;

    /// The command used.
    const Poco::JSON::Object& cmd_;

    /// Contains all of the data frames - we need this, because it might be
    /// possible that the features are retrieved.
    const std::map<std::string, containers::DataFrame>& data_frames_;

    /// Keeps track of the data frames and their fingerprints.
    const dependency::DataFrameTracker& data_frame_tracker_;

    /// The depedencies of the predictors.
    const std::vector<Poco::JSON::Object::Ptr>& dependencies_;

    /// Logs the progress.
    const std::shared_ptr<const communication::Logger>& logger_;

    /// The peripheral tables, without staging, as they were passed.
    const std::optional<std::vector<containers::DataFrame>>
        original_peripheral_dfs_;

    /// The population table, without staging, as it was passed.
    const std::optional<containers::DataFrame> original_population_df_;

    /// The peripheral tables.
    const std::vector<containers::DataFrame>& peripheral_dfs_;

    /// The population table.
    const containers::DataFrame& population_df_;

    /// Impl for the predictors.
    const predictors::PredictorImpl& predictor_impl_;

    /// The dependency tracker for the predictors.
    const std::shared_ptr<dependency::PredTracker> pred_tracker_;

    /// Purpose: FEATURE_SELECTOR or PREDICTOR
    const std::string purpose_;

    /// The population table used for validation (only relevant for
    /// early stopping).
    const std::optional<containers::DataFrame> validation_df_;

    /// Output: The autofeatures to be generated.
    containers::Features* const autofeatures_;

    /// Output: The predictors to be fitted.
    std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>* const
        predictors_;

    /// Output: The socket with which we communicate.
    Poco::Net::StreamSocket* const socket_;
};

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_TRANSFORMPARAMS_HPP_
