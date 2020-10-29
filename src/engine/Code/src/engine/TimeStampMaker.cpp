#include "engine/ts/ts.hpp"

namespace engine
{
namespace ts
{
// ----------------------------------------------------------------------------

std::vector<containers::Column<Float>> TimeStampMaker::make_time_stamps(
    const std::string &_ts_name,
    const Float _horizon,
    const Float _memory,
    const containers::DataFrame &_df )
{
    // -----------------------------------------------------------------

    if ( _ts_name == "" )
        {
            return std::vector<containers::Column<Float>>();
        }

    // -----------------------------------------------------------------

    if ( _df.num_time_stamps() == 0 )
        {
            throw std::invalid_argument(
                "DataFrame '" + _df.name() + "' has no time stamps!" );
        }

    // -----------------------------------------------------------------

    const auto horizon_op = [_horizon]( const Float val ) {
        return val + _horizon;
    };

    const auto mem_op = [_horizon, _memory]( const Float val ) {
        return val + _horizon + _memory;
    };

    // -----------------------------------------------------------------

    const auto ts = _df.time_stamp( _ts_name );

    // -----------------------------------------------------------------

    std::vector<containers::Column<Float>> cols;

    // -----------------------------------------------------------------

    if ( _horizon != 0.0 )
        {
            cols.emplace_back( containers::Column<Float>( _df.nrows() ) );

            cols.back().set_unit( ts.unit() );

            std::transform(
                ts.begin(), ts.end(), cols.back().begin(), horizon_op );
        }

    // -----------------------------------------------------------------

    if ( _memory > 0.0 )
        {
            cols.emplace_back( containers::Column<Float>( _df.nrows() ) );

            cols.back().set_unit( ts.unit() );

            std::transform( ts.begin(), ts.end(), cols.back().begin(), mem_op );
        }

    // -----------------------------------------------------------------

    return cols;

    // -----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::string TimeStampMaker::make_ts_name(
    const std::string &_ts_used, const Float _diff )
{
    const bool is_rowid =
        ( _ts_used.find( helpers::Macros::rowid() ) != std::string::npos );

    const auto diffstr =
        helpers::SQLGenerator::make_time_stamp_diff( _diff, is_rowid );

    if ( is_rowid )
        {
            return helpers::Macros::open_bracket() + _ts_used + diffstr +
                   helpers::Macros::close_bracket();
        }

    return helpers::Macros::generated_ts() + _ts_used + diffstr;
}

// ----------------------------------------------------------------------------
}  // namespace ts
}  // namespace engine
