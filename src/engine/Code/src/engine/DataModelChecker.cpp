#include "engine/pipelines/pipelines.hpp"

namespace engine
{
namespace pipelines
{
// ----------------------------------------------------------------------------

void DataModelChecker::check(
    const std::shared_ptr<Poco::JSON::Object> _population_placeholder,
    const std::shared_ptr<std::vector<std::string>> _peripheral_names,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::shared_ptr<const monitoring::Logger>& _logger,
    Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------------

    communication::Warner warner;

    // --------------------------------------------------------------------------

    check_df( _population, &warner );

    for ( const auto& df : _peripheral )
        {
            check_df( df, &warner );
        }

    // --------------------------------------------------------------------------

    if ( _logger )
        {
            for ( const auto& warning : warner.warnings() )
                {
                    _logger->log( "WARNING: " + warning );
                }
        }

    // --------------------------------------------------------------------------

    warner.send( _socket );

    // --------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataModelChecker::check_df(
    const containers::DataFrame& _df, communication::Warner* _warner )
{
    // --------------------------------------------------------------------------

    if ( _df.nrows() == 0 )
        {
            _warner->add( "Data frame '" + _df.name() + "' is empty." );
            return;
        }

    // --------------------------------------------------------------------------

    for ( size_t i = 0; i < _df.num_numericals(); ++i )
        {
            check_float_column( _df.numerical( i ), _df.name(), _warner );
        }

    for ( size_t i = 0; i < _df.num_time_stamps(); ++i )
        {
            check_float_column( _df.time_stamp( i ), _df.name(), _warner );
        }

    // --------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataModelChecker::check_float_column(
    const containers::Column<Float>& _col,
    const std::string& _df_name,
    communication::Warner* _warner )
{
    // --------------------------------------------------------------------------

    assert_true( _col.size() > 0 );

    const auto length = static_cast<Float>( _col.size() );

    // --------------------------------------------------------------------------

    const Float num_non_null =
        utils::ColumnOperators::count( _col.begin(), _col.end() );

    const auto share_null = 1.0 - num_non_null / length;

    if ( share_null > 0.9 )
        {
            _warner->add(
                std::to_string( share_null * 100.0 ) +
                "\% of all entries of column '" + _col.name() +
                "' in data frame '" + _df_name +
                "' are NULL values. You should "
                "consider setting its role to unused_float." );

            return;
        }

    // --------------------------------------------------------------------------

    // --------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine
