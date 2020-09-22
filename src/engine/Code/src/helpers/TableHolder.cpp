#include "helpers/helpers.hpp"

namespace helpers
{
// ----------------------------------------------------------------------------

std::shared_ptr<const std::vector<size_t>> TableHolder::make_subrows(
    const DataFrameView& _population_subview,
    const DataFrame& _peripheral_subview )
{
    std::set<size_t> rows;

    for ( size_t i = 0; i < _population_subview.nrows(); ++i )
        {
            const auto jk = _population_subview.join_key( i );

            if ( _peripheral_subview.has( jk ) )
                {
                    auto it = _peripheral_subview.find( jk );

                    for ( const auto j : it->second )
                        {
                            rows.insert( j );
                        }
                }
        }

    return std::make_shared<const std::vector<size_t>>(
        rows.begin(), rows.end() );
}

// ----------------------------------------------------------------------------

std::vector<DataFrameView> TableHolder::parse_main_tables(
    const Placeholder& _placeholder, const DataFrameView& _population )
{
    assert_true(
        _placeholder.joined_tables_.size() ==
        _placeholder.join_keys_used_.size() );
    assert_true(
        _placeholder.joined_tables_.size() ==
        _placeholder.time_stamps_used_.size() );

    std::vector<DataFrameView> result;

    for ( size_t i = 0; i < _placeholder.joined_tables_.size(); ++i )
        {
            result.push_back( _population.create_subview(
                _placeholder.name_,
                _placeholder.join_keys_used_.at( i ),
                _placeholder.time_stamps_used_.at( i ),
                "" ) );
        }

    return result;
}

// ----------------------------------------------------------------------------

std::vector<DataFrame> TableHolder::parse_peripheral_tables(
    const Placeholder& _placeholder,
    const std::vector<DataFrame>& _peripheral,
    const std::vector<std::string>& _peripheral_names )
{
    assert_true(
        _placeholder.joined_tables_.size() ==
        _placeholder.other_join_keys_used_.size() );
    assert_true(
        _placeholder.joined_tables_.size() ==
        _placeholder.other_time_stamps_used_.size() );
    assert_true( _peripheral.size() > 0 );
    assert_true( _peripheral_names.size() == _peripheral.size() );

    std::vector<DataFrame> result;

    for ( size_t i = 0; i < _placeholder.joined_tables_.size(); ++i )
        {
            const auto j = std::distance(
                _peripheral_names.begin(),
                std::find(
                    _peripheral_names.begin(),
                    _peripheral_names.end(),
                    _placeholder.joined_tables_.at( i ).name_ ) );

            if ( j >= _peripheral_names.size() )
                {
                    throw std::invalid_argument(
                        "Peripheral table named '" +
                        _placeholder.joined_tables_.at( i ).name_ +
                        "' not found!" );
                }

            result.push_back( _peripheral.at( j ).create_subview(
                _placeholder.joined_tables_.at( i ).name_,
                _placeholder.other_join_keys_used_.at( i ),
                _placeholder.other_time_stamps_used_.at( i ),
                _placeholder.upper_time_stamps_used_.at( i ),
                _placeholder.allow_lagged_targets_.at( i ) ) );
        }

    return result;
}

// ----------------------------------------------------------------------------

std::vector<std::optional<TableHolder>> TableHolder::parse_subtables(
    const Placeholder& _placeholder,
    const DataFrameView& _population,
    const std::vector<DataFrame>& _peripheral,
    const std::vector<std::string>& _peripheral_names )
{
    assert_true( _peripheral.size() > 0 );
    assert_true( _peripheral_names.size() == _peripheral.size() );

    std::vector<std::optional<TableHolder>> result;

    for ( size_t i = 0; i < _placeholder.joined_tables_.size(); ++i )
        {
            const auto& joined = _placeholder.joined_tables_.at( i );

            if ( joined.joined_tables_.size() > 0 )
                {
                    const auto j = std::distance(
                        _peripheral_names.begin(),
                        std::find(
                            _peripheral_names.begin(),
                            _peripheral_names.end(),
                            joined.name_ ) );

                    if ( j >= _peripheral_names.size() )
                        {
                            throw std::invalid_argument(
                                "Peripheral table named '" + joined.name_ +
                                "' not found!" );
                        }

                    const auto population_subview = _population.create_subview(
                        _placeholder.name_,
                        _placeholder.join_keys_used_.at( i ),
                        _placeholder.time_stamps_used_.at( i ),
                        "" );

                    const auto peripheral_subview =
                        _peripheral.at( j ).create_subview(
                            _placeholder.joined_tables_.at( i ).name_,
                            _placeholder.other_join_keys_used_.at( i ),
                            _placeholder.other_time_stamps_used_.at( i ),
                            _placeholder.upper_time_stamps_used_.at( i ),
                            _placeholder.allow_lagged_targets_.at( i ) );

                    const auto output = DataFrameView(
                        _peripheral.at( j ),
                        make_subrows(
                            population_subview, peripheral_subview ) );

                    result.push_back( std::make_optional<TableHolder>(
                        joined, output, _peripheral, _peripheral_names ) );
                }
            else
                {
                    result.push_back( std::optional<TableHolder>() );
                }
        }

    return result;
}

// ----------------------------------------------------------------------------
}  // namespace helpers
