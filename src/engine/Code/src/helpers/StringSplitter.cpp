#include "helpers/helpers.hpp"

namespace helpers
{
// ----------------------------------------------------------------------------

std::vector<std::string> StringSplitter::split(
    const std::string& _str, const std::string& _sep )
{
    std::vector<std::string> splitted;

    auto remaining = _str;

    while ( true )
        {
            const auto pos = remaining.find( _sep );

            if ( pos == std::string::npos )
                {
                    splitted.push_back( remaining );
                    break;
                }

            const auto token = remaining.substr( 0, pos );

            splitted.push_back( token );

            remaining.erase( 0, pos + _sep.length() );
        }

    return splitted;
}

// ----------------------------------------------------------------------------
}  // namespace helpers
