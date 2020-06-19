#ifndef ENGINE_UTILS_NULLCHECKER_HPP_
#define ENGINE_UTILS_NULLCHECKER_HPP_

namespace engine
{
namespace utils
{
// ----------------------------------------------------------------------------

struct NullChecker
{
    /// Checks whether a string is on the list of strings interpreted as NULL.
    static bool is_null( const strings::String& _val )
    {
        return (
            _val == "" || _val == "nan" || _val == "NaN" || _val == "NA" ||
            _val == "NULL" || _val == "none" || _val == "None" );
    }
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace engine

#endif  // ENGINE_UTILS_NULLCHECKER_HPP_
