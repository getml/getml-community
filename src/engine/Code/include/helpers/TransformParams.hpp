#ifndef HELPERS_TRANSFORMPARAMS_HPP_
#define HELPERS_TRANSFORMPARAMS_HPP_

namespace helpers
{
// ----------------------------------------------------------------------------

struct TransformParams
{
    /// Contains the features trained by the propositionalization.
    const std::optional<const FeatureContainer> &feature_container_;

    /// Indicates the features we want to extract.
    const std::vector<size_t> &index_;

    /// Uses to log progress.
    const std::shared_ptr<const logging::AbstractLogger> &logger_;

    /// Contains the mapping columns.
    const std::optional<const MappedContainer> &mapped_;

    /// The peripheral tables used.
    const std::vector<DataFrame> &peripheral_;

    /// The population table used.
    const DataFrame &population_;

    /// Contains the word indices of the text fields.
    const WordIndexContainer &word_indices_;
};

// ----------------------------------------------------------------------------
}  // namespace helpers

// ----------------------------------------------------------------------------
#endif  // HELPERS_TRANSFORMPARAMS_HPP_

