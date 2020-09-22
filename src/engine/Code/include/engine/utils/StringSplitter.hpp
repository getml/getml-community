#ifndef ENGINE_UTILS_STRINGSPLITTER_HPP_
#define ENGINE_UTILS_STRINGSPLITTER_HPP_

namespace engine
{
namespace utils
{
// ----------------------------------------------------------------------------

struct StringSplitter
{
    /// Splits a string into its individual components.
    static std::vector<std::string> split(
        const std::string& _str, const std::string& _sep );
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace engine

#endif  // ENGINE_UTILS_STRINGSPLITTER_HPP_
