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

    /// Constructs a vector of schemata from a JSON array.
    static std::shared_ptr<const std::vector<Schema>> from_json(
        const Poco::JSON::Array& _json_arr );

    /// Expresses the Schema as a JSON object.
    Poco::JSON::Object::Ptr to_json_obj() const;

    /// Getter for a categorical name.
    const std::string& categorical_name( size_t _j ) const
    {
        assert_true( _j < categoricals_.size() );
        return categoricals_.at( _j );
    }

    /// Getter for a discrete name.
    const std::string& discrete_name( size_t _j ) const
    {
        assert_true( _j < discretes_.size() );
        return discretes_.at( _j );
    }

    /// Getter for a join keys name  name.
    const std::string& join_keys_name( size_t _j ) const
    {
        assert_true( _j < join_keys_.size() );
        return join_keys_.at( _j );
    }

    /// Getter for the join key name.
    const std::string& join_keys_name() const
    {
        assert_true( join_keys_.size() == 1 );
        return join_keys_.at( 0 );
    }

    /// Return the name of the data frame.
    const std::string& name() const { return name_; }

    /// Trivial getter
    size_t num_categoricals() const { return categoricals_.size(); }

    /// Trivial getter
    size_t num_discretes() const { return discretes_.size(); }

    /// Trivial getter
    size_t num_join_keys() const { return join_keys_.size(); }

    /// Trivial getter
    size_t num_numericals() const { return numericals_.size(); }

    /// Trivial getter
    size_t num_targets() const { return targets_.size(); }

    /// Trivial getter
    size_t num_text() const { return text_.size(); }

    /// Trivial getter
    size_t num_time_stamps() const { return time_stamps_.size(); }

    /// Getter for a numerical name.
    const std::string& numerical_name( size_t _j ) const
    {
        assert_true( _j < numericals_.size() );
        return numericals_.at( _j );
    }

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

    /// Getter for a target name.
    const std::string& target_name( size_t _j ) const
    {
        assert_true( _j < targets_.size() );
        return targets_.at( _j );
    }

    /// Getter for a text name.
    const std::string& text_name( size_t _j ) const
    {
        assert_true( _j < text_.size() );
        return text_.at( _j );
    }

    /// Getter for a time stamp name.
    const std::string& time_stamps_name( size_t _j ) const
    {
        assert_true( _j < time_stamps_.size() );
        return time_stamps_.at( _j );
    }

    /// Getter for the time stamps name.
    const std::string& time_stamps_name() const
    {
        assert_true( time_stamps_.size() == 1 || time_stamps_.size() == 2 );
        return time_stamps_.at( 0 );
    }

    /// Transforms the placeholder into a JSON string
    std::string to_json() const
    {
        const auto ptr = to_json_obj();
        assert_true( ptr );
        return jsonutils::JSON::stringify( *ptr );
    }

    /// Getter for the time stamps name.
    const std::string& upper_time_stamps_name() const
    {
        assert_true( time_stamps_.size() == 2 );
        return time_stamps_.at( 1 );
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
