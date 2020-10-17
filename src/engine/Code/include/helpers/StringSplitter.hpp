#ifndef HELPERS_STRINGSPLITTER_HPP_
#define HELPERS_STRINGSPLITTER_HPP_

namespace helpers
{
// ----------------------------------------------------------------------------

struct StringSplitter
{
    /// Splits a string into its individual components.
    static std::vector<std::string> split(
        const std::string& _str, const std::string& _sep );
};

// ----------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_STRINGSPLITTER_HPP_
