#include "helpers/helpers.hpp"

namespace helpers
{
// ----------------------------------------------------------------------------

Schema::Schema( const Poco::JSON::Object& _json_obj )
    : categoricals_( jsonutils::JSON::array_to_vector<std::string>(
          jsonutils::JSON::get_array( _json_obj, "categorical_" ) ) ),
      discretes_( jsonutils::JSON::array_to_vector<std::string>(
          jsonutils::JSON::get_array( _json_obj, "discrete_" ) ) ),
      join_keys_( jsonutils::JSON::array_to_vector<std::string>(
          jsonutils::JSON::get_array( _json_obj, "join_key_" ) ) ),
      numericals_( jsonutils::JSON::array_to_vector<std::string>(
          jsonutils::JSON::get_array( _json_obj, "numerical_" ) ) ),
      targets_( jsonutils::JSON::array_to_vector<std::string>(
          jsonutils::JSON::get_array( _json_obj, "target_" ) ) ),
      text_( jsonutils::JSON::array_to_vector<std::string>(
          jsonutils::JSON::get_array( _json_obj, "text_" ) ) ),
      time_stamps_( jsonutils::JSON::array_to_vector<std::string>(
          jsonutils::JSON::get_array( _json_obj, "time_stamp_" ) ) ),
      unused_floats_(
          Schema::parse_columns<std::string>( _json_obj, "unused_float_" ) ),
      unused_strings_(
          Schema::parse_columns<std::string>( _json_obj, "unused_string_" ) )
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

    obj->set( "discrete_", jsonutils::JSON::vector_to_array_ptr( discretes_ ) );

    obj->set( "join_key_", jsonutils::JSON::vector_to_array_ptr( join_keys_ ) );

    obj->set(
        "numerical_", jsonutils::JSON::vector_to_array_ptr( numericals_ ) );

    obj->set( "target_", jsonutils::JSON::vector_to_array_ptr( targets_ ) );

    obj->set( "text_", jsonutils::JSON::vector_to_array_ptr( targets_ ) );

    obj->set(
        "time_stamp_", jsonutils::JSON::vector_to_array_ptr( time_stamps_ ) );

    obj->set(
        "unused_float_",
        jsonutils::JSON::vector_to_array_ptr( unused_floats_ ) );

    obj->set(
        "unused_string_",
        jsonutils::JSON::vector_to_array_ptr( unused_strings_ ) );

    // ---------------------------------------------------------

    return obj;
}

// ----------------------------------------------------------------------------
}  // namespace helpers
