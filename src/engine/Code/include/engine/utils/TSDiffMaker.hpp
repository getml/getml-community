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
        const std::string &_to )
    {
        if ( _from.empty() )
            {
                return _str;
            }

        auto modified = _str;

        size_t pos = 0;

        while ( ( pos = modified.find( _from, pos ) ) != std::string::npos )
            {
                modified.replace( pos, _from.length(), _to );
                pos += _to.length();
            }

        return modified;
    }
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace engine

#endif  // ENGINE_UTILS_STRINGREPLACER_HPP_
