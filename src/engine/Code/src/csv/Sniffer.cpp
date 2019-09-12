
#include "csv/csv.hpp"

namespace csv
{
// ----------------------------------------------------------------------------

void Sniffer::check(
    const std::vector<std::string>& _line,
    const std::vector<std::string>& _colnames,
    const std::string& _fname ) const
{
    if ( _line.size() != _colnames.size() )
        {
            throw std::invalid_argument(
                "Wrong number of columns in '" + _fname + "'. Expected " +
                std::to_string( _colnames.size() ) + ", saw " +
                std::to_string( _line.size() ) + "." );
        }

    if ( header_ )
        {
            for ( size_t i = 0; i < _line.size(); ++i )
                {
                    if ( _line[i] != _colnames[i] )
                        {
                            throw std::runtime_error(
                                "Column " + std::to_string( i + 1 ) + " in '" +
                                _fname + "' has wrong name. Expected '" +
                                _colnames[i] + "', saw '" + _line[i] + "'." );
                        }
                }
        }
}

// ----------------------------------------------------------------------------

Datatype Sniffer::infer_datatype(
    const Datatype _type, const std::string& _str ) const
{
    if ( ( _type == Datatype::integer || _type == Datatype::unknown ) &&
         is_int( _str ) )
        {
            return Datatype::integer;
        }
    else if (
        ( _type == Datatype::double_precision || _type == Datatype::unknown ||
          _type == Datatype::integer ) &&
        ( is_double( _str ) || is_int( _str ) ) )
        {
            return Datatype::double_precision;
        }
    else if (
        ( _type == Datatype::time_stamp || _type == Datatype::unknown ) &&
        is_time_stamp( _str ) )
        {
            return Datatype::time_stamp;
        }
    else
        {
            return Datatype::string;
        }
}

// ----------------------------------------------------------------------------

void Sniffer::init(
    const std::vector<std::string>& _line,
    std::vector<std::string>* _colnames,
    std::vector<Datatype>* _datatypes ) const
{
    if ( header_ )
        {
            *_colnames = _line;
        }
    else
        {
            for ( size_t i = 0; i < _line.size(); ++i )
                {
                    _colnames->push_back( "COLUMN_" + std::to_string( i + 1 ) );
                }
        }

    *_datatypes = std::vector<Datatype>( _line.size(), Datatype::unknown );
}

// ----------------------------------------------------------------------------

std::string Sniffer::sniff() const
{
    // ------------------------------------------------------------------------

    auto colnames = std::vector<std::string>( 0 );

    auto datatypes = std::vector<Datatype>( 0 );

    // ------------------------------------------------------------------------

    for ( auto fname : files_ )
        {
            size_t line_count = 0;

            auto reader = Reader( fname, quotechar_, sep_ );

            while ( !reader.eof() && line_count < num_lines_sniffed_ )
                {
                    // --------------------------------------------------------
                    // Read the next line.

                    std::vector<std::string> line = reader.next_line();

                    if ( line.size() == 0 ) continue;

                    // --------------------------------------------------------
                    // Check the line size.

                    ++line_count;

                    if ( line_count - 1 == skip_ )
                        {
                            if ( colnames.size() == 0 )
                                init( line, &colnames, &datatypes );
                            else
                                check( line, colnames, fname );

                            if ( header_ ) continue;
                        }
                    else if ( line_count - 1 < skip_ )
                        {
                            continue;
                        }
                    else if ( line.size() != datatypes.size() )
                        {
                            std::cout << "Corrupted line: " << line_count
                                      << ". Expected " << datatypes.size()
                                      << " fields, saw " << line.size() << "."
                                      << std::endl;

                            continue;
                        }

                    // --------------------------------------------------------
                    // Do the actual parsing.

                    assert_true( datatypes.size() == line.size() );

                    assert_true( datatypes.size() == colnames.size() );

                    for ( size_t i = 0; i < datatypes.size(); ++i )
                        {
                            datatypes[i] =
                                infer_datatype( datatypes[i], line[i] );
                        }

                    // --------------------------------------------------------
                }
        }

    // ------------------------------------------------------------------------

    return StatementMaker::make_statement(
        table_name_, dialect_, colnames, datatypes );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace csv
