#ifndef ENGINE_UTILS_TSDIFFMAKER_HPP_
#define ENGINE_UTILS_TSDIFFMAKER_HPP_

namespace engine
{
namespace utils
{
// ----------------------------------------------------------------------------

struct TSDiffMaker
{
    /// Infers the correct unit to _diff.
    static std::string make_time_stamp_diff( const Float _diff )
    {
        constexpr Float seconds_per_day = 24.0 * 60.0 * 60.0;
        constexpr Float seconds_per_hour = 60.0 * 60.0;
        constexpr Float seconds_per_minute = 60.0;

        auto diffstr = std::to_string( _diff ) + " seconds";

        if ( _diff >= seconds_per_day )
            {
                diffstr = std::to_string( _diff / seconds_per_day ) + " days";
            }
        else if ( _diff >= seconds_per_hour )
            {
                diffstr = std::to_string( _diff / seconds_per_hour ) + " hours";
            }
        else if ( _diff >= seconds_per_minute )
            {
                diffstr =
                    std::to_string( _diff / seconds_per_minute ) + " minutes";
            }

        return ", '+" + diffstr + "'";
    }
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace engine

#endif  // ENGINE_UTILS_TSDIFFMAKER_HPP_
