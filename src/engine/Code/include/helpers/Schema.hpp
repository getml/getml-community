#ifndef HELPERS_SCHEMA_HPP_
#define HELPERS_SCHEMA_HPP_

// ------------------------------------------------------------------------

namespace helpers
{
// ------------------------------------------------------------------------

struct Schema
{
    /// Constructs a new schema from a JSON object.
    static Schema from_json( const Poco::JSON::Object& _json_obj );

    /// Expresses the Schema as a JSON object.
    Poco::JSON::Object::Ptr to_json_obj() const;

    /// Checks whether an array exists,
    /// and returns and empty array, if it doesn't.
    template <typename T>
    static std::vector<T> parse_columns(
        const Poco::JSON::Object& _json_obj, const std::string& _name )
    {
        if ( _json_obj.has( _name ) )
            {
                return jsonutils::JSON::array_to_vector<T>(
                    jsonutils::JSON::get_array( _json_obj, _name ) );
            }
        else
            {
                return std::vector<T>();
            }
    }

    /// The names of the categorical columns
    const std::vector<std::string> categoricals_;

    /// The names of the discrete columns
    const std::vector<std::string> discretes_;

    /// The names of the join keys
    const std::vector<std::string> join_keys_;

    /// The table name
    const std::string name_;

    /// The names of the numerical columns
    const std::vector<std::string> numericals_;

    /// The names of the target columns
    const std::vector<std::string> targets_;

    /// The names of the text columns
    const std::vector<std::string> text_;

    /// The names of the time stamp columns
    const std::vector<std::string> time_stamps_;

    /// The names of the unused float columns
    const std::vector<std::string> unused_floats_;

    /// The names of the unused string columns
    const std::vector<std::string> unused_strings_;
};

// ------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_SCHEMA_HPP_
