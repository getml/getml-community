#ifndef HELPERS_FITPARAMS_HPP_
#define HELPERS_FITPARAMS_HPP_

namespace helpers
{
// ----------------------------------------------------------------------------

struct FitParams
{
    /// Contains the features trained by the propositionalization.
    const std::optional<const FeatureContainer> &feature_container_;

    /// Uses to log progress.
    const std::shared_ptr<const logging::AbstractLogger> &logger_;

    /// Contains the mapping columns.
    const std::optional<const MappedContainer> &mapped_;

    /// The peripheral tables used.
    const std::vector<DataFrame> &peripheral_;

    /// The population table used.
    const DataFrame &population_;

    /// Contains the row indices of the text fields.
    const RowIndexContainer &row_indices_;

    /// Contains the word indices of the text fields.
    const WordIndexContainer &word_indices_;
};

// ----------------------------------------------------------------------------
}  // namespace helpers

// ----------------------------------------------------------------------------
#endif  // HELPERS_FITPARAMS_HPP_

