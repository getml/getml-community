#include "engine/pipelines/pipelines.hpp"

namespace engine
{
namespace pipelines
{
// ----------------------------------------------------------------------------

void PlaceholderMaker::extract_joined_tables(
    const helpers::Placeholder& _placeholder, std::set<std::string>* _names )
{
    for ( const auto& p : _placeholder.joined_tables_ )
        {
            extract_joined_tables( p, _names );
            _names->insert( p.name() );
        }
}

// ----------------------------------------------------------------------------

std::vector<std::string> PlaceholderMaker::handle_horizon(
    const helpers::Placeholder& _placeholder,
    const std::vector<Float>& _horizon )
{
    auto other_time_stamps_used = _placeholder.other_time_stamps_used_;

    assert_true( other_time_stamps_used.size() == _horizon.size() );

    for ( size_t i = 0; i < _horizon.size(); ++i )
        {
            if ( _horizon.at( i ) == 0.0 )
                {
                    continue;
                }

            other_time_stamps_used.at( i ) = make_ts_name(
                _placeholder.other_time_stamps_used_.at( i ),
                _horizon.at( i ) );
        }

    return other_time_stamps_used;
}

// ----------------------------------------------------------------------------

helpers::Placeholder PlaceholderMaker::handle_joined_tables(
    const helpers::Placeholder& _placeholder,
    const Poco::JSON::Array& _joined_tables_arr,
    const std::vector<std::string>& _relationship,
    const std::vector<std::string>& _other_time_stamps_used,
    const std::vector<std::string>& _upper_time_stamps_used )
{
    // ------------------------------------------------------------------------

    const auto size = _joined_tables_arr.size();

    assert_true( _relationship.size() == size );

    assert_true( _placeholder.allow_lagged_targets_.size() == size );

    assert_true( _placeholder.join_keys_used_.size() == size );

    assert_true( _placeholder.other_join_keys_used_.size() == size );

    assert_true( _other_time_stamps_used.size() == size );

    assert_true( _placeholder.time_stamps_used_.size() == size );

    assert_true( _upper_time_stamps_used.size() == size );

    // ------------------------------------------------------------------------

    auto allow_lagged_targets = std::vector<bool>();

    auto join_keys_used = std::vector<std::string>();

    auto joined_tables = std::vector<helpers::Placeholder>();

    auto name = _placeholder.name();

    auto other_join_keys_used = std::vector<std::string>();

    auto other_time_stamps_used = std::vector<std::string>();

    auto time_stamps_used = std::vector<std::string>();

    auto upper_time_stamps_used = std::vector<std::string>();

    // ------------------------------------------------------------------------

    for ( size_t i = 0; i < size; ++i )
        {
            const auto joined_table_obj =
                _joined_tables_arr.getObject( static_cast<unsigned int>( i ) );

            // Has already been checked when initializing the Placeholder.
            assert_true( joined_table_obj );

            const auto joined_table = make_placeholder( *joined_table_obj );

            if ( _relationship.at( i ) == RELATIONSHIP_DEFAULT )
                {
                    allow_lagged_targets.push_back(
                        _placeholder.allow_lagged_targets_.at( i ) );

                    join_keys_used.push_back(
                        _placeholder.join_keys_used_.at( i ) );

                    joined_tables.push_back( joined_table );

                    other_join_keys_used.push_back(
                        _placeholder.other_join_keys_used_.at( i ) );

                    other_time_stamps_used.push_back(
                        _other_time_stamps_used.at( i ) );

                    time_stamps_used.push_back(
                        _placeholder.time_stamps_used_.at( i ) );

                    upper_time_stamps_used.push_back(
                        _upper_time_stamps_used.at( i ) );

                    continue;
                }

            append( joined_table.allow_lagged_targets_, &allow_lagged_targets );

            append( joined_table.join_keys_used_, &join_keys_used );

            append( joined_table.other_join_keys_used_, &other_join_keys_used );

            append( joined_table.joined_tables_, &joined_tables );

            append(
                joined_table.other_time_stamps_used_, &other_time_stamps_used );

            append( joined_table.time_stamps_used_, &time_stamps_used );

            append(
                joined_table.upper_time_stamps_used_, &upper_time_stamps_used );

            const auto one_to_one =
                ( _relationship.at( i ) == RELATIONSHIP_ONE_TO_ONE );

            name += containers::Macros::make_table_name(
                _placeholder.join_keys_used_.at( i ),
                _placeholder.other_join_keys_used_.at( i ),
                _placeholder.time_stamps_used_.at( i ),
                _placeholder.other_time_stamps_used_.at( i ),
                _placeholder.upper_time_stamps_used_.at( i ),
                joined_table.name(),
                _placeholder.name(),
                one_to_one );
        }

    // ------------------------------------------------------------------------

    return helpers::Placeholder(
        allow_lagged_targets,
        joined_tables,
        join_keys_used,
        name,
        other_join_keys_used,
        other_time_stamps_used,
        time_stamps_used,
        upper_time_stamps_used );
}

// ----------------------------------------------------------------------------

std::vector<std::string> PlaceholderMaker::handle_memory(
    const helpers::Placeholder& _placeholder,
    const std::vector<Float>& _horizon,
    const std::vector<Float>& _memory )
{
    auto upper_time_stamps_used = _placeholder.upper_time_stamps_used_;

    assert_true( _memory.size() == upper_time_stamps_used.size() );
    assert_true( _memory.size() == _horizon.size() );
    assert_true(
        _memory.size() == _placeholder.other_time_stamps_used_.size() );

    for ( size_t i = 0; i < _memory.size(); ++i )
        {
            if ( _memory.at( i ) <= 0.0 )
                {
                    continue;
                }

            if ( upper_time_stamps_used.at( i ) != "" )
                {
                    throw std::invalid_argument(
                        "You can either set an upper time stamp or "
                        "memory, but not both!" );
                }

            upper_time_stamps_used.at( i ) = make_ts_name(
                _placeholder.other_time_stamps_used_.at( i ),
                _horizon.at( i ) + _memory.at( i ) );
        }

    return upper_time_stamps_used;
}

// ----------------------------------------------------------------------------

std::vector<std::string> PlaceholderMaker::make_peripheral(
    const helpers::Placeholder& _placeholder )
{
    std::set<std::string> names;

    extract_joined_tables( _placeholder, &names );

    return std::vector<std::string>( names.begin(), names.end() );
}

// ----------------------------------------------------------------------------

helpers::Placeholder PlaceholderMaker::make_placeholder(
    const Poco::JSON::Object& _obj )
{
    // ----------------------------------------------------------

    const auto placeholder = helpers::Placeholder( _obj );

    // ------------------------------------------------------------------------

    const auto joined_tables_arr = _obj.getArray( "joined_tables_" );

    assert_true( joined_tables_arr );

    const auto expected_size = joined_tables_arr->size();

    // ------------------------------------------------------------------------

    const auto horizon =
        extract_vector<Float>( _obj, "horizon_", expected_size );

    const auto memory = extract_vector<Float>( _obj, "memory_", expected_size );

    const auto relationship =
        _obj.has( "relationship_" )
            ? extract_vector<std::string>(
                  _obj, "relationship_", expected_size )
            : std::vector<std::string>( expected_size, RELATIONSHIP_DEFAULT );

    // ------------------------------------------------------------------------

    const auto other_time_stamps_used = handle_horizon( placeholder, horizon );

    // ------------------------------------------------------------------------

    const auto upper_time_stamps_used =
        handle_memory( placeholder, horizon, memory );

    // ------------------------------------------------------------------------

    return handle_joined_tables(
        placeholder,
        *joined_tables_arr,
        relationship,
        other_time_stamps_used,
        upper_time_stamps_used );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine
