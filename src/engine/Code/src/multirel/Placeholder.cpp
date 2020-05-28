#include "multirel/containers/containers.hpp"

namespace multirel
{
namespace containers
{
// ----------------------------------------------------------------------------

void Placeholder::check_data_model(
    const std::vector<std::string>& _peripheral_names,
    const bool _is_population ) const
{
    if ( _is_population && joined_tables_.size() == 0 )
        {
            throw std::invalid_argument(
                "The population placeholder contains no joined tables!" );
        }

    for ( const auto& joined_table : joined_tables_ )
        {
            const auto it = std::find(
                _peripheral_names.begin(),
                _peripheral_names.end(),
                joined_table.name_ );

            if ( it == _peripheral_names.end() )
                {
                    throw std::invalid_argument(
                        "Placeholder '" + joined_table.name_ +
                        "' is contained in the relational tree, but not among "
                        "the peripheral placeholders!" );
                }

            joined_table.check_data_model( _peripheral_names, false );
        }
}

// ----------------------------------------------------------------------------

void Placeholder::check_vector_length()
{
    const size_t expected = joined_tables_.size();

    if ( join_keys_used_.size() != expected )
        {
            throw std::invalid_argument(
                "Error: Length of join keys used does not match length of "
                "joined tables!" );
        }

    if ( other_join_keys_used_.size() != expected )
        {
            throw std::invalid_argument(
                "Error: Length of other join keys used does not match "
                "length of "
                "joined tables!" );
        }

    if ( time_stamps_used_.size() != expected )
        {
            throw std::invalid_argument(
                "Error: Length of time stamps used does not match length "
                "of "
                "joined tables!" );
        }

    if ( other_time_stamps_used_.size() != expected )
        {
            throw std::invalid_argument(
                "Error: Length of other time stamps used does not match "
                "length of "
                "joined tables!" );
        }

    if ( upper_time_stamps_used_.size() != expected )
        {
            throw std::invalid_argument(
                "Error: Length of upper time stamps used does not match "
                "length of "
                "joined tables!" );
        }
}

// ----------------------------------------------------------------------------

Poco::JSON::Array::Ptr Placeholder::joined_tables_to_array(
    const std::vector<Placeholder>& _vector )
{
    Poco::JSON::Array::Ptr arr( new Poco::JSON::Array() );

    for ( auto& elem : _vector )
        {
            arr->add( elem.to_json_obj() );
        }

    return arr;
}

// ----------------------------------------------------------------------------

std::vector<Placeholder> Placeholder::parse_joined_tables(
    const Poco::JSON::Array::Ptr _array )
{
    std::vector<Placeholder> vec;

    if ( _array.isNull() )
        {
            return vec;
        }

    for ( size_t i = 0; i < _array->size(); ++i )
        {
            const auto ptr =
                _array->getObject( static_cast<unsigned int>( i ) );

            if ( !ptr )
                {
                    throw std::invalid_argument(
                        "Element " + std::to_string( i ) +
                        " in joined_tables_ is not an Object!" );
                }

            vec.push_back( Placeholder( *ptr ) );
        }

    return vec;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object::Ptr Placeholder::to_json_obj() const
{
    // ---------------------------------------------------------

    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    // ---------------------------------------------------------

    obj->set(
        "joined_tables_",
        Placeholder::joined_tables_to_array( joined_tables_ ) );

    obj->set( "join_keys_used_", JSON::vector_to_array_ptr( join_keys_used_ ) );

    obj->set(
        "other_join_keys_used_",
        JSON::vector_to_array_ptr( other_join_keys_used_ ) );

    obj->set(
        "other_time_stamps_used_",
        JSON::vector_to_array_ptr( other_time_stamps_used_ ) );

    obj->set( "name_", name_ );

    obj->set(
        "time_stamps_used_", JSON::vector_to_array_ptr( time_stamps_used_ ) );

    obj->set(
        "upper_time_stamps_used_",
        JSON::vector_to_array_ptr( upper_time_stamps_used_ ) );

    // ---------------------------------------------------------

    obj->set( "categorical_", JSON::vector_to_array_ptr( categoricals_ ) );

    obj->set( "discrete_", JSON::vector_to_array_ptr( discretes_ ) );

    obj->set( "join_keys_", JSON::vector_to_array_ptr( join_keys_ ) );

    obj->set( "numerical_", JSON::vector_to_array_ptr( numericals_ ) );

    obj->set( "targets_", JSON::vector_to_array_ptr( targets_ ) );

    obj->set( "time_stamps_", JSON::vector_to_array_ptr( time_stamps_ ) );

    // ---------------------------------------------------------

    return obj;

    // ---------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace multirel
