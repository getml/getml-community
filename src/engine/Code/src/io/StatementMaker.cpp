#include "io/io.hpp"

namespace io
{
// ----------------------------------------------------------------------------

size_t StatementMaker::find_max_size(
    const std::vector<std::string>& _colnames )
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

std::string StatementMaker::make_statement(
    const std::string& _table_name,
    const std::string& _dialect,
    const std::vector<std::string>& _colnames,
    const std::vector<Datatype>& _datatypes )
{
    if ( _dialect == "mysql" )
        {
            return make_statement_mysql( _table_name, _colnames, _datatypes );
        }
    else if (
        _dialect.find( "odbc" ) != std::string::npos && _dialect.size() == 6 )
        {
            return make_statement_odbc(
                _table_name, _colnames, _datatypes, _dialect[4], _dialect[5] );
        }
    else if ( _dialect == "postgres" )
        {
            return make_statement_postgres(
                _table_name, _colnames, _datatypes );
        }
    else if ( _dialect == "python" )
        {
            return make_statement_python( _colnames, _datatypes );
        }
    if ( _dialect == "sqlite" )
        {
            return make_statement_sqlite( _table_name, _colnames, _datatypes );
        }
    else
        {
            throw std::invalid_argument(
                "SQL dialect '" + _dialect + "' not known!" );
            return "";
        }
}

// ----------------------------------------------------------------------------

std::string StatementMaker::make_statement_mysql(
    const std::string& _table_name,
    const std::vector<std::string>& _colnames,
    const std::vector<Datatype>& _datatypes )
{
    assert_true( _colnames.size() == _datatypes.size() );

    const auto max_size = find_max_size( _colnames );

    std::stringstream statement;

    statement << "DROP TABLE IF EXISTS `" << _table_name << "`;" << std::endl
              << std::endl;

    statement << "CREATE TABLE `" << _table_name << "`(" << std::endl;

    for ( size_t i = 0; i < _colnames.size(); ++i )
        {
            statement << "    `" << _colnames[i] << "` "
                      << make_gap( _colnames[i], max_size )
                      << to_string_mysql( _datatypes[i] );

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

std::string StatementMaker::make_statement_odbc(
    const std::string& _table_name,
    const std::vector<std::string>& _colnames,
    const std::vector<Datatype>& _datatypes,
    const char _escape_char1,
    const char _escape_char2 )
{
    assert_true( _colnames.size() == _datatypes.size() );

    const auto max_size = find_max_size( _colnames );

    std::stringstream statement;

    statement << "CREATE TABLE ";

    if ( _escape_char1 != ' ' )
        {
            statement << _escape_char1;
        }

    statement << _table_name;

    if ( _escape_char2 != ' ' )
        {
            statement << _escape_char2;
        }

    statement << "(" << std::endl;

    for ( size_t i = 0; i < _colnames.size(); ++i )
        {
            statement << "    ";

            if ( _escape_char1 != ' ' )
                {
                    statement << _escape_char1;
                }

            statement << _colnames[i];

            if ( _escape_char2 != ' ' )
                {
                    statement << _escape_char2;
                }

            statement << " " << make_gap( _colnames[i], max_size )
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

std::string StatementMaker::make_statement_postgres(
    const std::string& _table_name,
    const std::vector<std::string>& _colnames,
    const std::vector<Datatype>& _datatypes )
{
    assert_true( _colnames.size() == _datatypes.size() );

    const auto max_size = find_max_size( _colnames );

    std::stringstream statement;

    statement << "DROP TABLE IF EXISTS \"" << _table_name << "\";" << std::endl
              << std::endl;

    statement << "CREATE TABLE \"" << _table_name << "\"(" << std::endl;

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

std::string StatementMaker::make_statement_python(
    const std::vector<std::string>& _colnames,
    const std::vector<Datatype>& _datatypes )
{
    assert_true( _colnames.size() == _datatypes.size() );

    Poco::JSON::Array unused_floats;
    Poco::JSON::Array unused_strings;

    for ( size_t i = 0; i < _colnames.size(); ++i )
        {
            switch ( _datatypes[i] )
                {
                    case Datatype::double_precision:
                    case Datatype::integer:
                        unused_floats.add( _colnames[i] );
                        break;

                    default:
                        unused_strings.add( _colnames[i] );
                        break;
                }
        }

    Poco::JSON::Object obj;

    obj.set( "unused_float", unused_floats );
    obj.set( "unused_string", unused_strings );

    return jsonutils::JSON::stringify( obj );
}

// ----------------------------------------------------------------------------

std::string StatementMaker::make_statement_sqlite(
    const std::string& _table_name,
    const std::vector<std::string>& _colnames,
    const std::vector<Datatype>& _datatypes )
{
    assert_true( _colnames.size() == _datatypes.size() );

    const auto max_size = find_max_size( _colnames );

    std::stringstream statement;

    statement << "DROP TABLE IF EXISTS \"" << _table_name << "\";" << std::endl
              << std::endl;

    statement << "CREATE TABLE \"" << _table_name << "\"(" << std::endl;

    for ( size_t i = 0; i < _colnames.size(); ++i )
        {
            statement << "    \"" << _colnames[i] << "\" "
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

std::string StatementMaker::to_string_mysql( const Datatype _type )
{
    switch ( _type )
        {
            case Datatype::double_precision:
                return "DOUBLE";

            case Datatype::integer:
                return "INT";

            case Datatype::string:
                return "TEXT";

            default:
                assert_true( false );
                return "";
        }
}

// ----------------------------------------------------------------------------

std::string StatementMaker::to_string_postgres( const Datatype _type )
{
    switch ( _type )
        {
            case Datatype::double_precision:
                return "DOUBLE PRECISION";

            case Datatype::integer:
                return "INTEGER";

            case Datatype::string:
                return "TEXT";

            default:
                assert_true( false );
                return "";
        }
}

// ----------------------------------------------------------------------------

std::string StatementMaker::to_string_sqlite( const Datatype _type )
{
    switch ( _type )
        {
            case Datatype::double_precision:
                return "REAL";

            case Datatype::integer:
                return "INTEGER";

            case Datatype::string:
                return "TEXT";

            default:
                assert_true( false );
                return "";
        }
}

// ----------------------------------------------------------------------------
}  // namespace io
