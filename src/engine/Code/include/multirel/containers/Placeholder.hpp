#ifndef MULTIREL_ENSEMBLE_PLACEHOLDER_HPP_
#define MULTIREL_ENSEMBLE_PLACEHOLDER_HPP_

// ------------------------------------------------------------------------

namespace multirel
{
namespace containers
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
              _json_obj.getArray( "joined_tables_" ) ) ),
          join_keys_( Placeholder::parse_columns( _json_obj, "join_keys_" ) ),
          join_keys_used_(
              Placeholder::parse_columns( _json_obj, "join_keys_used_" ) ),
          name_( JSON::get_value<std::string>( _json_obj, "name_" ) ),
          numericals_( Placeholder::parse_columns( _json_obj, "numericals_" ) ),
          other_join_keys_used_( Placeholder::parse_columns(
              _json_obj, "other_join_keys_used_" ) ),
          other_time_stamps_used_( Placeholder::parse_columns(
              _json_obj, "other_time_stamps_used_" ) ),
          targets_( Placeholder::parse_columns( _json_obj, "targets_" ) ),
          time_stamps_(
              Placeholder::parse_columns( _json_obj, "time_stamps_" ) ),
          time_stamps_used_(
              Placeholder::parse_columns( _json_obj, "time_stamps_used_" ) ),
          upper_time_stamps_used_( Placeholder::parse_columns(
              _json_obj, "upper_time_stamps_used_" ) )
    {
        check_vector_length();
    }

    Placeholder( const Poco::JSON::Object::Ptr& _json_obj )
        : Placeholder( *_json_obj )
    {
    }

    Placeholder(
        const std::vector<std::string>& _categoricals,
        const std::vector<std::string>& _discretes,
        const std::vector<std::string>& _join_keys,
        const std::string& _name,
        const std::vector<std::string>& _numericals,
        const std::vector<std::string>& _targets,
        const std::vector<std::string>& _time_stamps )
        : categoricals_( _categoricals ),
          discretes_( _discretes ),
          join_keys_( _join_keys ),
          name_( _name ),
          numericals_( _numericals ),
          targets_( _targets ),
          time_stamps_( _time_stamps )
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
    static Poco::JSON::Array joined_tables_to_array(
        const std::vector<Placeholder>& _vector );

    /// Parses the joined tables
    static std::vector<Placeholder> parse_joined_tables(
        const Poco::JSON::Array::Ptr _array );

    /// Transforms the placeholder into a JSON object
    Poco::JSON::Object to_json_obj() const;

    // --------------------------------------------------------

    /// Getter for a categorical name.
    const std::string& categorical_name( size_t _j ) const
    {
        assert_true( _j < categoricals_.size() );
        return categoricals_[_j];
    }

    /// Getter for a discrete name.
    const std::string& discrete_name( size_t _j ) const
    {
        assert_true( _j < discretes_.size() );
        return discretes_[_j];
    }

    /// Getter for a join keys name  name.
    const std::string& join_keys_name( size_t _j ) const
    {
        assert_true( _j < join_keys_.size() );
        return join_keys_[_j];
    }

    /// Getter for the join key name.
    const std::string& join_keys_name() const
    {
        assert_true( join_keys_.size() == 1 );

        return join_keys_[0];
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
    size_t num_time_stamps() const { return time_stamps_.size(); }

    /// Getter for a numerical name.
    const std::string& numerical_name( size_t _j ) const
    {
        assert_true( _j < numericals_.size() );
        return numericals_[_j];
    }

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

    /// Getter for a targets.
    const std::vector<std::string>& targets() const { return targets_; }

    /// Getter for a target name.
    const std::string& target_name( size_t _j ) const
    {
        assert_true( _j < targets_.size() );
        return targets_[_j];
    }

    /// Getter for a time stamp name.
    const std::string& time_stamps_name( size_t _j ) const
    {
        assert_true( _j < time_stamps_.size() );
        return time_stamps_[_j];
    }

    /// Getter for the time stamps name.
    const std::string& time_stamps_name() const
    {
        assert_true( time_stamps_.size() == 1 || time_stamps_.size() == 2 );

        return time_stamps_[0];
    }

    /// Transforms the placeholder into a JSON string
    std::string to_json() const { return JSON::stringify( to_json_obj() ); }

    /// Getter for the time stamps name.
    const std::string& upper_time_stamps_name() const
    {
        assert_true( time_stamps_.size() == 2 );

        return time_stamps_[1];
    }

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

    /// Name of the Placeholder object
    const std::string name_;

    /// The name of the numerical columns
    /// (this is only required for the Python API).
    const std::vector<std::string> numericals_;

    /// Names of the join keys used (RIGHT) - should have
    /// same length as joined_tables_
    const std::vector<std::string> other_join_keys_used_;

    /// Names of the time stamps used (RIGHT) - should have
    /// same length as joined_tables_
    const std::vector<std::string> other_time_stamps_used_;

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
}  // namespace containers
}  // namespace multirel

#endif  // MULTIREL_ENSEMBLE_PLACEHOLDER_HPP_
