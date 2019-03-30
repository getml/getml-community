#include "ensemble/ensemble.hpp"

namespace relboost
{
namespace ensemble
{
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

Poco::JSON::Array Placeholder::joined_tables_to_array(
    const std::vector<Placeholder>& _vector )
{
    Poco::JSON::Array arr;

    for ( auto& elem : _vector )
        {
            arr.add( elem.to_json_obj() );
        }

    return arr;
}

// ----------------------------------------------------------------------------

std::vector<Placeholder> Placeholder::parse_joined_tables(
    const Poco::JSON::Array::Ptr _array )
{
    if ( _array.isNull() )
        {
            std::runtime_error(
                "Error while parsing Placeholder: Array does not exist or "
                "is not an array!" );
        }

    std::vector<Placeholder> vec;

    for ( size_t i = 0; i < _array->size(); ++i )
        {
            vec.push_back( Placeholder(
                *_array->getObject( static_cast<unsigned int>( i ) ) ) );
        }

    return vec;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Placeholder::to_json_obj() const
{
    Poco::JSON::Object obj;

    // ---------------------------------------------------------

    obj.set(
        "joined_tables_",
        Placeholder::joined_tables_to_array( joined_tables_ ) );

    obj.set( "join_keys_used_", join_keys_used_ );

    obj.set( "other_join_keys_used_", other_join_keys_used_ );

    obj.set( "other_time_stamps_used_", other_time_stamps_used_ );

    obj.set( "name_", name_ );

    obj.set( "time_stamps_used_", time_stamps_used_ );

    obj.set( "upper_time_stamps_used_", upper_time_stamps_used_ );

    // ---------------------------------------------------------

    obj.set( "categorical_", categorical_ );

    obj.set( "discrete_", discrete_ );

    obj.set( "join_keys_", join_keys_ );

    obj.set( "numerical_", numerical_ );

    obj.set( "targets_", targets_ );

    obj.set( "time_stamps_", time_stamps_ );

    // ---------------------------------------------------------

    return obj;
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relboost
