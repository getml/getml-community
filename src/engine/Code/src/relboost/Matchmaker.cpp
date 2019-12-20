#include "relboost/utils/utils.hpp"

namespace relboost
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
                    assert_true(
                        _sample_weights->size() == _population.nrows() );
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

std::vector<containers::Match> Matchmaker::make_matches(
    const containers::DataFrameView& _population )
{
    std::vector<containers::Match> matches;

    for ( size_t ix_output = 0; ix_output < _population.nrows(); ++ix_output )
        {
            matches.emplace_back( containers::Match{0, ix_output} );
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
                            _matches->push_back(
                                containers::Match{ix_input, _ix_output} );
                        }
                }
        }
}

// ----------------------------------------------------------------------------

std::vector<const containers::Match*> Matchmaker::make_pointers(
    const std::vector<containers::Match>& _matches )
{
    auto pointers = std::vector<const containers::Match*>( _matches.size() );

    for ( size_t i = 0; i < _matches.size(); ++i )
        {
            pointers[i] = _matches.data() + i;
        }

    return pointers;
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost
