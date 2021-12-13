#ifndef HELPERS_FITPARAMS_HPP_
#define HELPERS_FITPARAMS_HPP_

namespace helpers
{
// ----------------------------------------------------------------------------

struct FitParams
{
    /// Contains the features trained by the propositionalization.
    const std::optional<const FeatureContainer> feature_container_;

    /// Uses to log progress.
    const std::shared_ptr<const logging::AbstractLogger> logger_;

    /// The peripheral tables used.
    const std::vector<DataFrame> peripheral_;

    /// The population table used.
    const DataFrame population_;

    /// Contains the row indices of the text fields.
    const RowIndexContainer row_indices_;

    /// The temporary directory to be used to place the memory mapping (when set
    /// to nullopt, everything will be done in memory)
    const std::optional<std::string> temp_dir_;

    /// Contains the word indices of the text fields.
    const WordIndexContainer word_indices_;
};

// ----------------------------------------------------------------------------
}  // namespace helpers

// ----------------------------------------------------------------------------
#endif  // HELPERS_FITPARAMS_HPP_

