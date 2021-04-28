#ifndef FASTPROP_SUBFEATURES_MAKERPARAMS_HPP_
#define FASTPROP_SUBFEATURES_MAKERPARAMS_HPP_

// ----------------------------------------------------------------------------

namespace fastprop
{
namespace subfeatures
{
// ------------------------------------------------------------------------

struct MakerParams
{
    /// The container for the fast prop algorithm - needed for transformation
    /// only.
    const std::shared_ptr<const FastPropContainer> fast_prop_container_ =
        nullptr;

    /// The hyperparameters used to construct the FastProp algorithm.
    const std::shared_ptr<const Hyperparameters>& hyperparameters_;

    /// The logger used to log the progress.
    const std::shared_ptr<const logging::AbstractLogger>& logger_;

    /// The mapped categorical, discrete and text columns.
    const std::optional<const helpers::MappedContainer> mapped_;

    /// The peripheral tables used for training.
    const std::vector<containers::DataFrame>& peripheral_;

    /// Names of the peripheral tables, as referenced by the placeholder.
    const std::shared_ptr<const std::vector<std::string>>& peripheral_names_;

    /// The placeholder used.
    const containers::Placeholder placeholder_;

    /// The population table used for training.
    const containers::DataFrame population_;

    /// The row index container used to fit the text columns.
    const std::optional<helpers::RowIndexContainer> row_index_container_ =
        std::nullopt;

    /// The word index container used to transform the text columns.
    const helpers::WordIndexContainer word_index_container_;
};

// ------------------------------------------------------------------------
}  // namespace subfeatures
}  // namespace fastprop

#endif  // FASTPROP_SUBFEATURES_MAKERPARAMS_HPP_
