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
    static std::string make_time_stamp_diff( const Float _diff );
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace engine

#endif  // ENGINE_UTILS_TSDIFFMAKER_HPP_
