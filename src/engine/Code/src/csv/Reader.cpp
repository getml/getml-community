
#include "csv/csv.hpp"

namespace csv
{
// ----------------------------------------------------------------------------

std::vector<std::string> Reader::next_line()
{
    // ------------------------------------------------------------------------
    // Usually the calling function should make sure that we haven't reached
    // the end of file. But just to be sure, we do it again.

    if ( eof() )
        {
            return std::vector<std::string>();
        }

    // ------------------------------------------------------------------------
    // Read the next line from the filestream - if it is empty, return an empty
    // vector.

    std::string line;

    std::getline( *filestream_, line );

    if ( line.size() == 0 )
        {
            return std::vector<std::string>();
        }

    // ------------------------------------------------------------------------
    // Chop up lines into fields.

    std::vector<std::string> result;

    std::string field;

    bool is_quoted = false;

    for ( char c : line )
        {
            if ( c == sep_ && !is_quoted )
                {
                    result.push_back( field );
                    field.clear();
                }
            else if ( c == quotechar_ )
                {
                    is_quoted = !is_quoted;
                }
            else
                {
                    field += c;
                }
        }

    result.push_back( field );

    // ------------------------------------------------------------------------

    return result;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace csv