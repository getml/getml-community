#include "helpers/helpers.hpp"

namespace helpers
{
// ----------------------------------------------------------------------------

Schema::Schema( const Poco::JSON::Object& _json_obj )
    : categoricals_( jsonutils::JSON::array_to_vector<std::string>(
          jsonutils::JSON::get_array( _json_obj, "categorical_" ) ) ),
      discretes_(
          _json_obj.has( "discrete_" )
              ? jsonutils::JSON::array_to_vector<std::string>(
                    jsonutils::JSON::get_array( _json_obj, "discrete_" ) )
              : std::vector<std::string>() ),
      join_keys_( jsonutils::JSON::array_to_vector<std::string>(
          jsonutils::JSON::get_array( _json_obj, "join_keys_" ) ) ),
      numericals_( jsonutils::JSON::array_to_vector<std::string>(
          jsonutils::JSON::get_array( _json_obj, "numerical_" ) ) ),
      targets_( jsonutils::JSON::array_to_vector<std::string>(
          jsonutils::JSON::get_array( _json_obj, "targets_" ) ) ),
      text_( jsonutils::JSON::array_to_vector<std::string>(
          jsonutils::JSON::get_array( _json_obj, "text_" ) ) ),
      time_stamps_( jsonutils::JSON::array_to_vector<std::string>(
          jsonutils::JSON::get_array( _json_obj, "time_stamps_" ) ) ),
      unused_floats_(
          Schema::parse_columns<std::string>( _json_obj, "unused_floats_" ) ),
      unused_strings_(
          Schema::parse_columns<std::string>( _json_obj, "unused_strings_" ) )
{
}

// ----------------------------------------------------------------------------

Schema::~Schema() = default;

// ----------------------------------------------------------------------------

Poco::JSON::Object::Ptr Schema::to_json_obj() const
{
    Poco::JSON::Object::Ptr obj( new Poco::JSON::Object() );

    // ---------------------------------------------------------

    obj->set(
        "categorical_", jsonutils::JSON::vector_to_array_ptr( categoricals_ ) );

    if ( discretes_.size() > 0 )
        {
            obj->set(
                "discrete_",
                jsonutils::JSON::vector_to_array_ptr( discretes_ ) );
        }

    obj->set(
        "join_keys_", jsonutils::JSON::vector_to_array_ptr( join_keys_ ) );

    obj->set(
        "numerical_", jsonutils::JSON::vector_to_array_ptr( numericals_ ) );

    obj->set( "targets_", jsonutils::JSON::vector_to_array_ptr( targets_ ) );

    obj->set( "text_", jsonutils::JSON::vector_to_array_ptr( targets_ ) );

    obj->set(
        "time_stamps_", jsonutils::JSON::vector_to_array_ptr( time_stamps_ ) );

    obj->set(
        "unused_floats_",
        jsonutils::JSON::vector_to_array_ptr( unused_floats_ ) );

    obj->set(
        "unused_strings_",
        jsonutils::JSON::vector_to_array_ptr( unused_strings_ ) );

    // ---------------------------------------------------------

    return obj;
}

// ----------------------------------------------------------------------------
}  // namespace helpers
