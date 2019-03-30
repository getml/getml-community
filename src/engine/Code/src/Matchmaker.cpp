#include "utils/utils.hpp"

namespace relboost
{
namespace utils
{
// ----------------------------------------------------------------------------

std::vector<containers::Match> Matchmaker::make_matches(
    const containers::DataFrame& _population,
    const containers::DataFrame& _peripheral,
    const std::shared_ptr<const std::vector<RELBOOST_FLOAT>>& _sample_weights,
    const bool _use_timestamps )
{
    std::vector<containers::Match> matches;

    for ( size_t ix_output = 0; ix_output < _population.nrows(); ++ix_output )
        {
            if ( _sample_weights )
                {
                    assert( _sample_weights->size() == _population.nrows() );
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
    const containers::DataFrame& _population,
    const containers::DataFrame& _peripheral,
    const bool _use_timestamps,
    const size_t _ix_output,
    std::vector<containers::Match>* _matches )
{
    const auto join_key = _population.join_keys_[0][_ix_output];

    const auto time_stamp_out = _population.time_stamps( _ix_output );

    auto it = _peripheral.indices_[0]->find( join_key );

    if ( it != _peripheral.indices_[0]->end() )
        {
            for ( size_t ix_input : it->second )
                {
                    const auto lower = _peripheral.time_stamps( ix_input );

                    const auto upper =
                        _peripheral.upper_time_stamps( ix_input );

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
