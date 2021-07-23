#include "multirel/utils/utils.hpp"

namespace multirel
{
namespace utils
{
// ----------------------------------------------------------------------------

std::vector<containers::Match> Matchmaker::make_matches(
    const containers::DataFrameView& _population,
    const containers::DataFrame& _peripheral,
    const std::shared_ptr<const std::vector<Float>>& _sample_weights )
{
    const auto make_match = []( const size_t ix_input,
                                const size_t ix_output ) {
        return containers::Match{
            false,      // activated
            0,          // categorical_value
            ix_input,   // ix_x_perip
            ix_output,  // ix_x_popul
            0.0         // numerical_value
        };
    };

    return helpers::Matchmaker<
        containers::DataFrameView,
        containers::Match,
        decltype( make_match )>::
        make_matches( _population, _peripheral, _sample_weights, make_match );
}

// ----------------------------------------------------------------------------

void Matchmaker::make_matches(
    const containers::DataFrameView& _population,
    const containers::DataFrame& _peripheral,
    const size_t _ix_output,
    std::vector<containers::Match>* _matches )
{
    const auto make_match = []( const size_t ix_input,
                                const size_t ix_output ) {
        return containers::Match{
            false,      // activated
            0,          // categorical_value
            ix_input,   // ix_x_perip
            ix_output,  // ix_x_popul
            0.0         // numerical_value
        };
    };

    helpers::Matchmaker<
        containers::DataFrameView,
        containers::Match,
        decltype( make_match )>::
        make_matches(
            _population, _peripheral, _ix_output, make_match, _matches );
}

// ----------------------------------------------------------------------------

containers::MatchPtrs Matchmaker::make_pointers(
    std::vector<containers::Match>* _matches )
{
    auto pointers = containers::MatchPtrs( _matches->size() );

    for ( size_t i = 0; i < _matches->size(); ++i )
        {
            pointers[i] = _matches->data() + i;
        }

    return pointers;
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace multirel
