#ifndef AUTOSQL_UTILS_MATCHMAKER_HPP_
#define AUTOSQL_UTILS_MATCHMAKER_HPP_

// ----------------------------------------------------------------------------

namespace autosql
{
namespace utils
{
// ------------------------------------------------------------------------

struct Matchmaker
{
    /// Identifies matches between population table and peripheral tables.
    static std::vector<containers::Match> make_matches(
        const containers::DataFrameView& _population,
        const containers::DataFrame& _peripheral,
        const std::shared_ptr<const std::vector<AUTOSQL_FLOAT>>&
            _sample_weights,
        const bool _use_timestamps );

    /// Identifies matches between a specific sample in the population table
    /// (signified by _ix_output and peripheral tables.
    static void make_matches(
        const containers::DataFrameView& _population,
        const containers::DataFrame& _peripheral,
        const bool _use_timestamps,
        const size_t _ix_output,
        std::vector<containers::Match>* _matches );

    /// Creates pointers to the matches.
    static containers::MatchPtrs make_pointers(
        std::vector<containers::Match>* _matches );
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace autosql

// ----------------------------------------------------------------------------

#endif  // AUTOSQL_UTILS_MATCHMAKER_HPP_