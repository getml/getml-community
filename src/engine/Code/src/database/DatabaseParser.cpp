
#include "database/database.hpp"

namespace database
{
// ----------------------------------------------------------------------------

std::shared_ptr<Connector> DatabaseParser::parse(
    const Poco::JSON::Object& _obj )
{
    const auto db = jsonutils::JSON::get_value<std::string>( _obj, "db_" );

    const auto time_formats = jsonutils::JSON::array_to_vector<std::string>(
        jsonutils::JSON::get_array( _obj, "time_formats_" ) );

    if ( db == "sqlite3" )
        {
            const auto name =
                jsonutils::JSON::get_value<std::string>( _obj, "name_" );

            return std::make_shared<Sqlite3>( name, time_formats );
        }
    else if ( db == "mysql" || db == "mariadb" )
        {
            return std::make_shared<MySQL>( _obj, time_formats );
        }
    else if ( db == "postgres" || db == "greenplum" )
        {
#if ( defined( _WIN32 ) || defined( _WIN64 ) )
            throw std::invalid_argument(
                "PostgreSQL and Greenplum are not supported on Windows!" );

            return std::shared_ptr<Connector>();
#else
            return std::make_shared<Postgres>( _obj, time_formats );
#endif
        }
    else
        {
            throw std::invalid_argument(
                "Database of type '" + db + "' not recognized." );

            return std::shared_ptr<Connector>();
        }
}

// ----------------------------------------------------------------------------
}  // namespace database
