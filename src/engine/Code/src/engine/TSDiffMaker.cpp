#include "engine/utils/utils.hpp"

namespace engine
{
namespace utils
{
// ----------------------------------------------------------------------------

std::string TSDiffMaker::make_time_stamp_diff( const Float _diff )
{
    constexpr Float seconds_per_day = 24.0 * 60.0 * 60.0;
    constexpr Float seconds_per_hour = 60.0 * 60.0;
    constexpr Float seconds_per_minute = 60.0;

    const auto abs_diff = std::abs( _diff );

    auto diffstr = std::to_string( _diff ) + " seconds";

    if ( abs_diff >= seconds_per_day )
        {
            diffstr = std::to_string( _diff / seconds_per_day ) + " days";
        }
    else if ( abs_diff >= seconds_per_hour )
        {
            diffstr = std::to_string( _diff / seconds_per_hour ) + " hours";
        }
    else if ( abs_diff >= seconds_per_minute )
        {
            diffstr = std::to_string( _diff / seconds_per_minute ) + " minutes";
        }

    const std::string sign = _diff >= 0.0 ? "+" : "";

    return ", '" + sign + diffstr + "'";
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace engine
