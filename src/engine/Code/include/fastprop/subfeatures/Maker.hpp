#ifndef FASTPROP_SUBFEATURES_MAKER_HPP_
#define FASTPROP_SUBFEATURES_MAKER_HPP_

// ----------------------------------------------------------------------------

namespace fastprop
{
namespace subfeatures
{
// ------------------------------------------------------------------------

class Maker
{
   public:
    /// Fits any and all FastProp algorithms whereever
    /// propositionalization flag is activated.
    static std::pair<
        std::shared_ptr<const FastPropContainer>,
        helpers::FeatureContainer>
    fit( const MakerParams& _params );

    /// Generates a new table holder by transforming the input data.
    static helpers::FeatureContainer transform( const MakerParams& _params );

   private:
    /// Finds the correct index matching the joined table to peripheral_names.
    static size_t find_peripheral_ix(
        const MakerParams& _params, const size_t _i );

    /// First any and all required FastProp algorithms.
    static std::shared_ptr<const FastPropContainer> fit_fast_prop_container(
        const helpers::TableHolder& _table_holder, const MakerParams& _params );

    /// Generates the subselection of the mapped container needed to fit or
    /// transform FastProp.
    static std::optional<const helpers::MappedContainer> make_mapped(
        const MakerParams& _params );

    /// Generates the subcontainers.
    static std::shared_ptr<typename FastPropContainer::Subcontainers>
    make_subcontainers(
        const helpers::TableHolder& _table_holder, const MakerParams& _params );

    /// Generates the params for fitting a subcontainer.
    static MakerParams make_params(
        const MakerParams& _params, const size_t _i );

    /// Generates placeholder used to generate the propositionalization.
    static std::shared_ptr<const helpers::Placeholder> make_placeholder(
        const MakerParams& _params );
};

// ------------------------------------------------------------------------
}  // namespace subfeatures
}  // namespace fastprop

#endif  // FASTPROP_SUBFEATURES_MAKER_HPP_

