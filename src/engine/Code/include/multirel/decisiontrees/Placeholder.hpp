#ifndef MULTIREL_ENSEMBLE_PLACEHOLDER_HPP_
#define MULTIREL_ENSEMBLE_PLACEHOLDER_HPP_

// ------------------------------------------------------------------------
// Dependencies

#include "multirel/JSON.hpp"

// ------------------------------------------------------------------------

namespace multirel
{
namespace decisiontrees
{
// ------------------------------------------------------------------------

struct Placeholder
{
    // --------------------------------------------------------

    Placeholder( const Poco::JSON::Object& _json_obj )
        : joined_tables_( Placeholder::parse_joined_tables(
              JSON::get_array( _json_obj, "joined_tables_" ) ) ),
          join_keys_used_( JSON::array_to_vector<std::string>(
              JSON::get_array( _json_obj, "join_keys_used_" ) ) ),
          other_join_keys_used_( JSON::array_to_vector<std::string>(
              JSON::get_array( _json_obj, "other_join_keys_used_" ) ) ),
          other_time_stamps_used_( JSON::array_to_vector<std::string>(
              JSON::get_array( _json_obj, "other_time_stamps_used_" ) ) ),
          name_( _json_obj.getValue<std::string>( "name_" ) ),
          time_stamps_used_( JSON::array_to_vector<std::string>(
              JSON::get_array( _json_obj, "time_stamps_used_" ) ) ),
          upper_time_stamps_used_( JSON::array_to_vector<std::string>(
              JSON::get_array( _json_obj, "upper_time_stamps_used_" ) ) )
    {
        check_vector_length();
    }

    Placeholder( const Poco::JSON::Object::Ptr& _json_obj )
        : Placeholder( *_json_obj )
    {
    }

    ~Placeholder() = default;

    // --------------------------------------------------------

    /// Checks the length of the vectors.
    void check_vector_length();

    /// Returns the joined tables as a JSON array
    static Poco::JSON::Array joined_tables_to_array(
        const std::vector<Placeholder>& _vector );

    /// Parses the joined tables
    static std::vector<Placeholder> parse_joined_tables(
        const Poco::JSON::Array::Ptr _array );

    /// Transforms the placeholder into a JSON object
    Poco::JSON::Object to_json_obj() const;

    // --------------------------------------------------------

    /// Transforms the placeholder into a JSON string
    std::string to_json() const { return JSON::stringify( to_json_obj() ); }

    // --------------------------------------------------------

    /// Placeholders that are LEFT JOINED
    /// to this placeholder
    const std::vector<Placeholder> joined_tables_;

    /// Names of the join keys used (LEFT) - should have
    /// same length as joined_tables_
    const std::vector<std::string> join_keys_used_;

    /// Names of the join keys used (RIGHT) - should have
    /// same length as joined_tables_
    const std::vector<std::string> other_join_keys_used_;

    /// Names of the time stamps used (RIGHT) - should have
    /// same length as joined_tables_
    const std::vector<std::string> other_time_stamps_used_;

    /// Name of the Placeholder object
    const std::string name_;

    /// Names of the time stamps used (LEFT) - should have
    /// same length as joined_tables_
    const std::vector<std::string> time_stamps_used_;

    /// Names of the time stamps used (LEFT) for the upper bound - should have
    /// same length as joined_tables_
    const std::vector<std::string> upper_time_stamps_used_;

    // ----------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace multirel

#endif  // MULTIREL_ENSEMBLE_PLACEHOLDER_HPP_
