
#include "io/io.hpp"

namespace io
{
// ----------------------------------------------------------------------------

std::string CSVWriter::make_buffer(
    const std::vector<std::string>& _line ) const
{
    std::string buffer;

    assert_true( _line.size() == colnames_.size() );

    for ( size_t i = 0; i < _line.size(); ++i )
        {
            buffer += parse_field( _line[i] );

            if ( i < _line.size() - 1 )
                {
                    buffer += sep_;
                }
            else
                {
                    buffer += '\n';
                }
        }

    return buffer;
}

// ----------------------------------------------------------------------------

std::string CSVWriter::parse_field( const std::string& _raw_field ) const
{
    auto field = io::Parser::remove_quotechars( _raw_field, quotechar() );

    if ( field.find( sep() ) != std::string::npos )
        {
            field = quotechar() + field + quotechar();
        }

    return field;
}

// ----------------------------------------------------------------------------

void CSVWriter::write( io::Reader* _reader )
{
    //  ------------------------------------------------------------------------

    const std::string buffer = make_buffer( colnames_ );

    filestream() << buffer;

    // ----------------------------------------------------------------
    // Insert line by line.

    Int line_count = 0;

    while ( !_reader->eof() )
        {
            const std::vector<std::string> line = _reader->next_line();

            ++line_count;

            if ( line.size() != colnames_.size() )
                {
                    std::cout << "Corrupted line: " << line_count
                              << ". Expected " << colnames_.size()
                              << " fields, saw " << line.size() << "."
                              << std::endl;

                    continue;
                }

            const std::string buffer = make_buffer( line );

            filestream() << buffer;
        }
}

// ------------------------------------------------------------------------
}  // namespace io

