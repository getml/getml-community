#include "multirel/timeseries/timeseries.hpp"

namespace multirel
{
namespace timeseries
{
// -----------------------------------------------------------------------------

std::pair<
    std::vector<containers::Column<Float>>,
    std::vector<std::shared_ptr<std::vector<Float>>>>
TimeSeriesModel::create_modified_time_stamps(
    const std::string& _ts_name,
    const Float _lag,
    const Float _memory,
    const containers::DataFrame& _population ) const
{
    // -----------------------------------------------------------------

    if ( _lag < 0.0 )
        {
            throw std::invalid_argument( "'lag' cannot be negative!" );
        }

    if ( _memory < 0.0 )
        {
            throw std::invalid_argument( "'memory' cannot be negative!" );
        }

    // -----------------------------------------------------------------

    const auto lag_op = [_lag]( const Float val ) { return val + _lag; };

    const auto mem_op = [_lag, _memory]( const Float val ) {
        return val + _lag + _memory;
    };

    // -----------------------------------------------------------------

    size_t ix = 0;

    for ( ; ix < _population.num_time_stamps(); ++ix )
        {
            if ( _population.time_stamp_col( ix ).name_ == _ts_name )
                {
                    break;
                }
        }

    if ( _ts_name == "" )
        {
            ix = 0;
        }

    if ( _population.num_time_stamps() == 0 )
        {
            throw std::invalid_argument(
                "DataFrame '" + _population.name() + "' has no time stamps!" );
        }

    if ( _population.num_time_stamps() > 1 && _ts_name == "" )
        {
            throw std::invalid_argument(
                "DataFrame '" + _population.name() +
                "' has more than one time stamp, but no identifying time stamp "
                "has been passed!" );
        }

    if ( ix == _population.num_time_stamps() &&
         _population.num_time_stamps() > 1 )
        {
            throw std::invalid_argument(
                "DataFrame '" + _population.name() +
                "' has no time stamps named '" + _ts_name + "'!" );
        }

    const auto& ts = _population.time_stamp_col( ix );

    // -----------------------------------------------------------------

    std::vector<containers::Column<Float>> cols;

    std::vector<std::shared_ptr<std::vector<Float>>> data;

    // -----------------------------------------------------------------

    data.emplace_back( std::make_shared<std::vector<Float>>( ts.nrows_ ) );

    std::transform(
        ts.data_, ts.data_ + ts.nrows_, data.back()->begin(), lag_op );

    cols.emplace_back( containers::Column<Float>(
        data.back()->data(), ts.name_ + "$GETML_LOWER_TS", ts.nrows_, "" ) );

    // -----------------------------------------------------------------

    if ( _memory > 0.0 )
        {
            data.emplace_back(
                std::make_shared<std::vector<Float>>( ts.nrows_ ) );

            std::transform(
                ts.data_, ts.data_ + ts.nrows_, data.back()->begin(), mem_op );

            cols.emplace_back( containers::Column<Float>(
                data.back()->data(),
                ts.name_ + "$GETML_UPPER_TS",
                ts.nrows_,
                "" ) );
        }

    // -----------------------------------------------------------------

    return std::make_pair( cols, data );

    // -----------------------------------------------------------------
}

// -----------------------------------------------------------------------------

containers::Placeholder TimeSeriesModel::create_placeholder(
    const containers::Placeholder& _placeholder,
    const std::vector<std::string>& _self_join_keys,
    const std::string& _lower_time_stamp_used,
    const std::string& _upper_time_stamp_used ) const
{
    // ----------------------------------------------------------

    const auto joined_table = containers::Placeholder(
        _placeholder.categoricals_,
        _placeholder.discretes_,
        _placeholder.join_keys_,
        _placeholder.name_,
        _placeholder.numericals_,
        _placeholder.targets_,
        _placeholder.time_stamps_ );

    // ----------------------------------------------------------

    auto joined_tables = _placeholder.joined_tables_;

    auto join_keys_used = _placeholder.join_keys_used_;

    auto other_join_keys_used = _placeholder.other_join_keys_used_;

    auto other_time_stamps_used = _placeholder.other_time_stamps_used_;

    auto time_stamps_used = _placeholder.time_stamps_used_;

    auto upper_time_stamps_used = _placeholder.upper_time_stamps_used_;

    // ----------------------------------------------------------

    for ( size_t i = 0; i < _self_join_keys.size(); ++i )
        {
            joined_tables.push_back( joined_table );

            join_keys_used.push_back( _self_join_keys[i] );

            other_join_keys_used.push_back( _self_join_keys[i] );

            other_time_stamps_used.push_back( _lower_time_stamp_used );

            time_stamps_used.push_back( _lower_time_stamp_used );

            upper_time_stamps_used.push_back( _upper_time_stamp_used );
        }

    // ----------------------------------------------------------

    return containers::Placeholder(
        joined_tables,
        join_keys_used,
        _placeholder.name(),
        other_join_keys_used,
        other_time_stamps_used,
        time_stamps_used,
        upper_time_stamps_used );

    // ----------------------------------------------------------
}

// -----------------------------------------------------------------------------

void TimeSeriesModel::fit(
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger )
{
    model_.fit( _population, _peripheral, _logger );
}

// -----------------------------------------------------------------------------
}  // namespace timeseries
}  // namespace multirel
