#include "relboost/ensemble/ensemble.hpp"

namespace relboost
{
namespace ensemble
{
// ----------------------------------------------------------------------------

std::vector<containers::DataFrameView> TableHolder::parse_main_tables(
    const Placeholder& _placeholder,
    const containers::DataFrameView& _population )
{
    assert(
        _placeholder.joined_tables_.size() ==
        _placeholder.join_keys_used_.size() );
    assert(
        _placeholder.joined_tables_.size() ==
        _placeholder.time_stamps_used_.size() );

    std::vector<containers::DataFrameView> result;

    for ( size_t i = 0; i < _placeholder.joined_tables_.size(); ++i )
        {
            result.push_back( _population.create_subview(
                _placeholder.name_,
                _placeholder.join_keys_used_[i],
                _placeholder.time_stamps_used_[i],
                "" ) );
        }

    return result;
}

// ----------------------------------------------------------------------------

std::vector<containers::DataFrame> TableHolder::parse_peripheral_tables(
    const Placeholder& _placeholder,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::vector<std::string>& _peripheral_names )
{
    assert(
        _placeholder.joined_tables_.size() ==
        _placeholder.other_join_keys_used_.size() );
    assert(
        _placeholder.joined_tables_.size() ==
        _placeholder.other_time_stamps_used_.size() );
    assert( _peripheral.size() > 0 );
    assert( _peripheral_names.size() == _peripheral.size() );

    std::vector<containers::DataFrame> result;

    for ( size_t i = 0; i < _placeholder.joined_tables_.size(); ++i )
        {
            const auto j = std::distance(
                _peripheral_names.begin(),
                std::find(
                    _peripheral_names.begin(),
                    _peripheral_names.end(),
                    _placeholder.joined_tables_[i].name_ ) );

            assert( j < _peripheral_names.size() );

            result.push_back( _peripheral[j].create_subview(
                _placeholder.joined_tables_[i].name_,
                _placeholder.other_join_keys_used_[i],
                _placeholder.other_time_stamps_used_[i],
                _placeholder.upper_time_stamps_used_[i] ) );
        }

    return result;
}

// ----------------------------------------------------------------------------

std::vector<containers::Optional<TableHolder>> TableHolder::parse_subtables(
    const Placeholder& _placeholder,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::vector<std::string>& _peripheral_names )
{
    assert( _peripheral.size() > 0 );
    assert( _peripheral_names.size() == _peripheral.size() );

    std::vector<containers::Optional<TableHolder>> result;

    for ( auto& joined : _placeholder.joined_tables_ )
        {
            if ( joined.joined_tables_.size() > 0 )
                {
                    const auto j = std::distance(
                        _peripheral_names.begin(),
                        std::find(
                            _peripheral_names.begin(),
                            _peripheral_names.end(),
                            joined.name_ ) );

                    assert( j < _peripheral_names.size() );

                    // TODO: Replace with popular parsing of indices.
                    const auto output = containers::DataFrameView(
                        _peripheral[j],
                        std::shared_ptr<const std::vector<size_t>>() );

                    result.push_back(
                        containers::Optional<TableHolder>( new TableHolder(
                            _placeholder,
                            output,
                            _peripheral,
                            _peripheral_names ) ) );
                }
            else
                {
                    result.push_back( containers::Optional<TableHolder>() );
                }
        }

    return result;
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relboost
