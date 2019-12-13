#ifndef RELBOOST_ENSEMBLE_PLACEHOLDER_HPP_
#define RELBOOST_ENSEMBLE_PLACEHOLDER_HPP_

// ------------------------------------------------------------------------
// Dependencies

#include "relboost/JSON.hpp"

// ------------------------------------------------------------------------

namespace relboost
{
namespace ensemble
{
// ------------------------------------------------------------------------

struct Placeholder
{
    // --------------------------------------------------------

    Placeholder( const Poco::JSON::Object& _json_obj )
        : categoricals_(
              Placeholder::parse_columns( _json_obj, "categoricals_" ) ),
          discretes_( Placeholder::parse_columns( _json_obj, "discretes_" ) ),
          joined_tables_( Placeholder::parse_joined_tables(
              JSON::get_array( _json_obj, "joined_tables_" ) ) ),
          join_keys_( Placeholder::parse_columns( _json_obj, "join_keys_" ) ),
          join_keys_used_( JSON::array_to_vector<std::string>(
              JSON::get_array( _json_obj, "join_keys_used_" ) ) ),
          numericals_( Placeholder::parse_columns( _json_obj, "numericals_" ) ),
          other_join_keys_used_( JSON::array_to_vector<std::string>(
              JSON::get_array( _json_obj, "other_join_keys_used_" ) ) ),
          other_time_stamps_used_( JSON::array_to_vector<std::string>(
              JSON::get_array( _json_obj, "other_time_stamps_used_" ) ) ),
          name_( _json_obj.getValue<std::string>( "name_" ) ),
          targets_( Placeholder::parse_columns( _json_obj, "targets_" ) ),
          time_stamps_(
              Placeholder::parse_columns( _json_obj, "time_stamps_" ) ),
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

    /// Makes sure that all joined tables are found in the peripheral names.
    void check_data_model(
        const std::vector<std::string>& _peripheral_names,
        const bool _is_population ) const;

    /// Checks the length of the vectors.
    void check_vector_length();

    /// Returns the joined tables as a JSON array
    static Poco::JSON::Array::Ptr joined_tables_to_array(
        const std::vector<Placeholder>& _vector );

    /// Parses the joined tables
    static std::vector<Placeholder> parse_joined_tables(
        const Poco::JSON::Array::Ptr _array );

    /// Transforms the placeholder into a JSON object
    Poco::JSON::Object::Ptr to_json_obj() const;

    // --------------------------------------------------------

    /// Checks whether an array exists (because only the Python API has one),
    /// and returns and empty array, if it doesn't.
    static std::vector<std::string> parse_columns(
        const Poco::JSON::Object& _json_obj, const std::string& _name )
    {
        if ( _json_obj.has( _name ) )
            {
                return JSON::array_to_vector<std::string>(
                    JSON::get_array( _json_obj, _name ) );
            }
        else
            {
                return std::vector<std::string>();
            }
    }

    /// Transforms the placeholder into a JSON string
    std::string to_json() const { return JSON::stringify( *to_json_obj() ); }

    // --------------------------------------------------------

    /// The name of the categorical columns
    /// (this is only required for the Python API).
    const std::vector<std::string> categoricals_;

    /// The name of the discrete columns
    /// (this is only required for the Python API).
    const std::vector<std::string> discretes_;

    /// Placeholders that are LEFT JOINED
    /// to this placeholder
    const std::vector<Placeholder> joined_tables_;

    /// The name of the join keys
    /// (this is only required for the Python API).
    const std::vector<std::string> join_keys_;

    /// Names of the join keys used (LEFT) - should have
    /// same length as joined_tables_
    const std::vector<std::string> join_keys_used_;

    /// The name of the numerical columns
    /// (this is only required for the Python API).
    const std::vector<std::string> numericals_;

    /// Names of the join keys used (RIGHT) - should have
    /// same length as joined_tables_
    const std::vector<std::string> other_join_keys_used_;

    /// Names of the time stamps used (RIGHT) - should have
    /// same length as joined_tables_
    const std::vector<std::string> other_time_stamps_used_;

    /// Name of the Placeholder object
    const std::string name_;

    /// The name of the target columns
    /// (this is only required for the Python API).
    const std::vector<std::string> targets_;

    /// The name of the time stamp columns
    /// (this is only required for the Python API).
    const std::vector<std::string> time_stamps_;

    /// Names of the time stamps used (LEFT) - should have
    /// same length as joined_tables_
    const std::vector<std::string> time_stamps_used_;

    /// Names of the time stamps used (LEFT) for the upper bound - should have
    /// same length as joined_tables_
    const std::vector<std::string> upper_time_stamps_used_;

    // ----------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relboost

#endif  // RELBOOST_ENSEMBLE_PLACEHOLDER_HPP_
