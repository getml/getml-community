#include "multirel/decisiontrees/decisiontrees.hpp"

namespace multirel
{
namespace decisiontrees
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

    return obj;
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace multirel
