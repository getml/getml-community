#ifndef ENGINE_PIPELINES_CHECKPARAMS_HPP_
#define ENGINE_PIPELINES_CHECKPARAMS_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace pipelines
{
// ----------------------------------------------------------------------------

struct CheckParams
{
    /// The Encoding used for the categories.
    const std::shared_ptr<containers::Encoding> categories_;

    /// The command used.
    const Poco::JSON::Object cmd_;

    /// Logs the progress.
    const std::shared_ptr<const communication::Logger> logger_;

    /// The peripheral tables.
    const std::vector<containers::DataFrame> peripheral_dfs_;

    /// The population table.
    const containers::DataFrame population_df_;

    /// The dependency tracker for the preprocessors.
    const std::shared_ptr<dependency::PreprocessorTracker>
        preprocessor_tracker_;

    /// Tracks the warnings to be shown in the Python API.
    const std::shared_ptr<dependency::WarningTracker> warning_tracker_;

    /// Output: The socket with which we communicate.
    Poco::Net::StreamSocket* const socket_;
};

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_CHECKPARAMS_HPP_
