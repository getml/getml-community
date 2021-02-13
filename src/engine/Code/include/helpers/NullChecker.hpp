#ifndef HELPERS_NULLCHECKER_HPP_
#define HELPERS_NULLCHECKER_HPP_

namespace helpers
{
// ----------------------------------------------------------------------------

struct NullChecker
{
    /// When ever we us integers, they signify encodings. -1 means that they are
    /// NULL.
    static bool is_null( const Int _val ) { return _val < 0; }

    /// Checks whether a float is NaN.
    static bool is_null( const Float _val ) { return std::isnan( _val ); }

    /// Checks whether a string is on the list of strings interpreted as NULL.
    static bool is_null( const strings::String& _val )
    {
        return (
            _val == "" || _val == "nan" || _val == "NaN" || _val == "NA" ||
            _val == "NULL" || _val == "none" || _val == "None" );
    }
};

// ----------------------------------------------------------------------------
}  // namespace helpers

#endif  // ENGINE_UTILS_NULLCHECKER_HPP_
