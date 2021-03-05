#include "textmining/textmining.hpp"

namespace textmining
{
// ----------------------------------------------------------------------------

std::vector<std::string> StringSplitter::split( const std::string& _str )
{
    std::vector<std::string> splitted;

    auto remaining = _str;

    while ( true )
        {
            const auto pos = remaining.find_first_of( separators_ );

            if ( pos == std::string::npos )
                {
                    splitted.push_back( remaining );
                    break;
                }

            const auto token = remaining.substr( 0, pos );

            splitted.push_back( token );

            remaining.erase( 0, pos + 1 );
        }

    return splitted;
}

// ----------------------------------------------------------------------------
}  // namespace textmining
