#ifndef ENGINE_UTILS_STRINGREPLACER_HPP_
#define ENGINE_UTILS_STRINGREPLACER_HPP_

namespace engine
{
namespace utils
{
// ----------------------------------------------------------------------------

struct StringReplacer
{
    /// Replaces all instances of _from in _str with _to.
    static std::string replace_all(
        const std::string &_str,
        const std::string &_from,
        const std::string &_to );
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace engine

#endif  // ENGINE_UTILS_STRINGREPLACER_HPP_
