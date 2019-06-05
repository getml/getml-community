#ifndef AUTOSQL_CONTAINERS_SCHEMA_HPP_
#define AUTOSQL_CONTAINERS_SCHEMA_HPP_

namespace autosql
{
namespace containers
{
// -------------------------------------------------------------------------

class Schema
{
    // ---------------------------------------------------------------------

   public:
    Schema(
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

    Schema( const Poco::JSON::Object& _obj )
        : categoricals_( JSON::array_to_vector<std::string>(
              JSON::get_array( _obj, "categoricals_" ) ) ),
          discretes_( JSON::array_to_vector<std::string>(
              JSON::get_array( _obj, "discretes_" ) ) ),
          join_keys_( JSON::array_to_vector<std::string>(
              JSON::get_array( _obj, "join_keys_" ) ) ),
          name_( JSON::get_value<std::string>( _obj, "name_" ) ),
          numericals_( JSON::array_to_vector<std::string>(
              JSON::get_array( _obj, "numericals_" ) ) ),
          targets_( JSON::array_to_vector<std::string>(
              JSON::get_array( _obj, "targets_" ) ) ),
          time_stamps_( JSON::array_to_vector<std::string>(
              JSON::get_array( _obj, "time_stamps_" ) ) )
    {
    }

    ~Schema() = default;

    // ---------------------------------------------------------------------

   public:
    /// Getter for a categorical name.
    const std::string& categorical_name( size_t _j ) const
    {
        assert( _j < categoricals_.size() );
        return categoricals_[_j];
    }

    /// Getter for a discrete name.
    const std::string& discrete_name( size_t _j ) const
    {
        assert( _j < discretes_.size() );
        return discretes_[_j];
    }

    /// Getter for the join key name.
    const std::string& join_keys_name() const
    {
        assert( join_keys_.size() == 1 );

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
        assert( _j < numericals_.size() );
        return numericals_[_j];
    }

    /// Getter for a targets.
    const std::vector<std::string>& targets() const { return targets_; }

    /// Getter for a target name.
    const std::string& target_name( size_t _j ) const
    {
        assert( _j < targets_.size() );
        return targets_[_j];
    }

    /// Getter for the time stamps name.
    const std::string& time_stamps_name() const
    {
        assert( time_stamps_.size() == 1 || time_stamps_.size() == 2 );

        return time_stamps_[0];
    }

    /// Transforms the schema to a JSON object.
    const Poco::JSON::Object to_json_obj() const
    {
        Poco::JSON::Object obj;

        obj.set( "name_", name_ );

        obj.set( "categoricals_", JSON::vector_to_array( categoricals_ ) );
        obj.set( "discretes_", JSON::vector_to_array( discretes_ ) );
        obj.set( "join_keys_", JSON::vector_to_array( join_keys_ ) );
        obj.set( "numericals_", JSON::vector_to_array( numericals_ ) );
        obj.set( "targets_", JSON::vector_to_array( targets_ ) );
        obj.set( "time_stamps_", JSON::vector_to_array( time_stamps_ ) );

        return obj;
    }

    /// Getter for the time stamps name.
    const std::string& upper_time_stamps_name() const
    {
        assert( time_stamps_.size() == 2 );

        return time_stamps_[1];
    }

    // ---------------------------------------------------------------------

   private:
    /// Pointer to categorical columns.
    const std::vector<std::string> categoricals_;

    /// Pointer to discrete columns.
    const std::vector<std::string> discretes_;

    /// Join keys of this data frame.
    const std::vector<std::string> join_keys_;

    /// Name of the data frame.
    const std::string name_;

    /// Pointer to numerical columns.
    const std::vector<std::string> numericals_;

    /// Pointer to target column.
    const std::vector<std::string> targets_;

    /// Time stamps of this data frame.
    const std::vector<std::string> time_stamps_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace autosql

#endif  // AUTOSQL_CONTAINERS_SCHEMA_HPP_
