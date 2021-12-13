#ifndef HELPERS_MATCHMAKER_HPP_
#define HELPERS_MATCHMAKER_HPP_

// ----------------------------------------------------------------------------

namespace helpers
{
// ------------------------------------------------------------------------

template <class PopulationType, class MatchType, class MakeMatchType>
struct Matchmaker
{
    /// Identifies matches between population table and peripheral tables.
    static std::vector<MatchType> make_matches(
        const PopulationType& _population,
        const DataFrame& _peripheral,
        const std::shared_ptr<const std::vector<Float>>& _sample_weights,
        const MakeMatchType _make_match );

    /// Identifies matches between a specific sample in the population table
    /// (signified by _ix_output and peripheral tables.
    static void make_matches(
        const PopulationType& _population,
        const DataFrame& _peripheral,
        const size_t _ix_output,
        const MakeMatchType _make_match,
        std::vector<MatchType>* _matches );
};

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------

template <class PopulationType, class MatchType, class MakeMatchType>
std::vector<MatchType>
Matchmaker<PopulationType, MatchType, MakeMatchType>::make_matches(
    const PopulationType& _population,
    const DataFrame& _peripheral,
    const std::shared_ptr<const std::vector<Float>>& _sample_weights,
    const MakeMatchType _make_match )
{
    std::vector<MatchType> matches;

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
                _population, _peripheral, ix_output, _make_match, &matches );
        }

    return matches;
}

// ------------------------------------------------------------------------

template <class PopulationType, class MatchType, class MakeMatchType>
void Matchmaker<PopulationType, MatchType, MakeMatchType>::make_matches(
    const PopulationType& _population,
    const DataFrame& _peripheral,
    const size_t _ix_output,
    const MakeMatchType _make_match,
    std::vector<MatchType>* _matches )
{
    const auto join_key = _population.join_key( _ix_output );

    const auto time_stamp_out = _population.time_stamp( _ix_output );

    if ( _peripheral.has( join_key ) )
        {
            const auto [begin, end] = _peripheral.find( join_key );

            for ( auto it = begin; it != end; ++it )
                {
                    const auto ix_input = *it;

                    const auto lower = _peripheral.time_stamp( ix_input );

                    const auto upper = _peripheral.upper_time_stamp( ix_input );

                    const bool match_in_range =
                        lower <= time_stamp_out &&
                        ( std::isnan( upper ) || upper > time_stamp_out );

                    if ( match_in_range )
                        {
                            _matches->push_back(
                                _make_match( ix_input, _ix_output ) );
                        }
                }
        }
}

// ------------------------------------------------------------------------
}  // namespace helpers

// ----------------------------------------------------------------------------

#endif  // HELPERS_MATCHMAKER_HPP_
