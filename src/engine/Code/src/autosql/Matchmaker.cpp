#include "autosql/utils/utils.hpp"

namespace autosql
{
namespace utils
{
// ----------------------------------------------------------------------------

std::vector<containers::Match> Matchmaker::make_matches(
    const containers::DataFrameView& _population,
    const containers::DataFrame& _peripheral,
    const std::shared_ptr<const std::vector<Float>>& _sample_weights,
    const bool _use_timestamps )
{
    std::vector<containers::Match> matches;

    for ( size_t ix_output = 0; ix_output < _population.nrows(); ++ix_output )
        {
            if ( _sample_weights )
                {
                    assert_true( _sample_weights->size() == _population.nrows() );
                    if ( ( *_sample_weights )[ix_output] <= 0.0 )
                        {
                            continue;
                        }
                }

            Matchmaker::make_matches(
                _population,
                _peripheral,
                _use_timestamps,
                ix_output,
                &matches );
        }

    return matches;
}

// ----------------------------------------------------------------------------

void Matchmaker::make_matches(
    const containers::DataFrameView& _population,
    const containers::DataFrame& _peripheral,
    const bool _use_timestamps,
    const size_t _ix_output,
    std::vector<containers::Match>* _matches )
{
    const auto join_key = _population.join_key( _ix_output );

    const auto time_stamp_out = _population.time_stamp( _ix_output );

    if ( _peripheral.has( join_key ) )
        {
            auto it = _peripheral.find( join_key );

            for ( size_t ix_input : it->second )
                {
                    const auto lower = _peripheral.time_stamp( ix_input );

                    const auto upper = _peripheral.upper_time_stamp( ix_input );

                    const bool match_in_range =
                        lower <= time_stamp_out &&
                        ( std::isnan( upper ) || upper > time_stamp_out );

                    if ( !_use_timestamps || match_in_range )
                        {
                            _matches->push_back( {
                                false,       // activated
                                0,           // categorical_value
                                ix_input,    // ix_x_perip
                                _ix_output,  // ix_x_popul
                                0.0          // numerical_value
                            } );
                        }
                }
        }
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
}  // namespace autosql
