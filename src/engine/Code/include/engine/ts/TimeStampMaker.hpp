#ifndef ENGINE_TS_TIMESTAMPMAKER_HPP_
#define ENGINE_TS_TIMESTAMPMAKER_HPP_

namespace engine
{
namespace ts
{
// ----------------------------------------------------------------------------

struct TimeStampMaker
{
    /// Generates lagged time stamps on the basis of _horizon and _memory.
    static std::vector<containers::Column<Float>> make_time_stamps(
        const std::string &_ts_name,
        const Float _horizon,
        const Float _memory,
        const containers::DataFrame &_df );

    /// Generates the columns name for the time stamp.
    static std::string make_ts_name(
        const std::string &_ts_used, const Float _diff );
};

// ----------------------------------------------------------------------------
}  // namespace ts
}  // namespace engine

#endif  // ENGINE_TS_TIMESTAMPMAKER_HPP_
