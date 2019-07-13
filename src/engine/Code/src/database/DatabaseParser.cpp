
#include "database/database.hpp"

namespace database
{
// ----------------------------------------------------------------------------

std::shared_ptr<Connector> DatabaseParser::parse(
    const Poco::JSON::Object& _obj )
{
    const auto type = jsonutils::JSON::get_value<std::string>( _obj, "type_" );

    const auto time_formats = jsonutils::JSON::array_to_vector<std::string>(
        jsonutils::JSON::get_array( _obj, "time_formats_" ) );

    if ( type == "sqlite3" )
        {
            const auto name =
                jsonutils::JSON::get_value<std::string>( _obj, "name_" );

            return std::make_shared<Sqlite3>( name, time_formats );
        }
    else
        {
            throw std::invalid_argument(
                "Database of type '" + type + "' not recognized." );

            return std::shared_ptr<Connector>();
        }
}

// ----------------------------------------------------------------------------
}  // namespace database
