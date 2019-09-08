
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

size_t Sniffer::find_max_size( const std::vector<std::string>& _colnames ) const
{
    if ( _colnames.size() == 0 )
        {
            return 0;
        }
    else
        {
            const auto comp = []( const std::string& str1,
                                  const std::string& str2 ) {
                return ( str1.size() < str2.size() );
            };

            const auto it =
                std::max_element( _colnames.begin(), _colnames.end(), comp );

            return it->size();
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

std::string Sniffer::make_statement(
    const std::vector<std::string>& _colnames,
    const std::vector<Datatype>& _datatypes ) const
{
    if ( dialect_ == "postgres" )
        {
            return make_statement_postgres( _colnames, _datatypes );
        }
    if ( dialect_ == "sqlite" )
        {
            return make_statement_sqlite( _colnames, _datatypes );
        }
    else
        {
            throw std::invalid_argument(
                "SQL dialect '" + dialect_ + "' not known!" );
            return "";
        }
}

// ----------------------------------------------------------------------------

std::string Sniffer::make_statement_postgres(
    const std::vector<std::string>& _colnames,
    const std::vector<Datatype>& _datatypes ) const
{
    assert_true( _colnames.size() == _datatypes.size() );

    const auto max_size = find_max_size( _colnames );

    std::stringstream statement;

    statement << "DROP TABLE IF EXISTS \"" << table_name_ << "\";" << std::endl
              << std::endl;

    statement << "CREATE TABLE \"" << table_name_ << "\"(" << std::endl;

    for ( size_t i = 0; i < _colnames.size(); ++i )
        {
            statement << "    \"" << _colnames[i] << "\" "
                      << make_gap( _colnames[i], max_size )
                      << to_string_postgres( _datatypes[i] );

            if ( i < _colnames.size() - 1 )
                {
                    statement << "," << std::endl;
                }
            else
                {
                    statement << ");" << std::endl;
                }
        }

    return statement.str();
}

// ----------------------------------------------------------------------------

std::string Sniffer::make_statement_sqlite(
    const std::vector<std::string>& _colnames,
    const std::vector<Datatype>& _datatypes ) const
{
    assert_true( _colnames.size() == _datatypes.size() );

    const auto max_size = find_max_size( _colnames );

    std::stringstream statement;

    statement << "DROP TABLE IF EXISTS " << table_name_ << ";" << std::endl
              << std::endl;

    statement << "CREATE TABLE " << table_name_ << "(" << std::endl;

    for ( size_t i = 0; i < _colnames.size(); ++i )
        {
            statement << "    " << _colnames[i] << " "
                      << make_gap( _colnames[i], max_size )
                      << to_string_sqlite( _datatypes[i] );

            if ( i < _colnames.size() - 1 )
                {
                    statement << "," << std::endl;
                }
            else
                {
                    statement << ");" << std::endl;
                }
        }

    return statement.str();
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

    return make_statement( colnames, datatypes );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::string Sniffer::to_string_postgres( const Datatype _type ) const
{
    switch ( _type )
        {
            case Datatype::double_precision:
                return "DOUBLE PRECISION";

            case Datatype::integer:
                return "INTEGER";

            case Datatype::string:
                return "TEXT";

            case Datatype::time_stamp:
                return "TIMESTAMP";

            default:
                assert_true( false );
                return "";
        }
}

// ----------------------------------------------------------------------------

std::string Sniffer::to_string_sqlite( const Datatype _type ) const
{
    switch ( _type )
        {
            case Datatype::double_precision:
                return "REAL";

            case Datatype::integer:
                return "INTEGER";

            case Datatype::string:
                return "TEXT";

            case Datatype::time_stamp:
                return "TEXT";  // sqlite has no time stamp type.

            default:
                assert_true( false );
                return "";
        }
}

// ----------------------------------------------------------------------------
}  // namespace csv
