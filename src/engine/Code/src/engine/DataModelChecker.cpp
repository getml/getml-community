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

    assert_true( _population_placeholder );

    assert_true( _peripheral_names );

    check_join(
        *_population_placeholder,
        *_peripheral_names,
        _population,
        _peripheral,
        &warner );

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

void DataModelChecker::check_categorical_column(
    const containers::Column<Int>& _col,
    const std::string& _df_name,
    communication::Warner* _warner )
{
    // --------------------------------------------------------------------------

    const auto length = static_cast<Float>( _col.size() );

    // --------------------------------------------------------------------------

    assert_true( _col.size() > 0 );

    const Float num_non_null =
        utils::ColumnOperators::count_categorical( *_col.data_ptr() );

    const auto share_null = 1.0 - num_non_null / length;

    if ( share_null > 0.9 )
        {
            _warner->add(
                std::to_string( share_null * 100.0 ) +
                "\% of all entries in column '" + _col.name() +
                "' in data frame '" + _df_name +
                "' are NULL values. You should "
                "consider setting its role to unused_string." );
        }

    if ( num_non_null < 0.5 )
        {
            return;
        }

    // --------------------------------------------------------------------------

    const Float num_distinct =
        utils::ColumnOperators::count_distinct( *_col.data_ptr() );

    if ( num_distinct == 1.0 )
        {
            _warner->add(
                "All non-NULL entries in column '" + _col.name() +
                "' in data frame '" + _df_name +
                "' are equal to each other. You should "
                "consider setting its role to unused_string." );
        }

    // --------------------------------------------------------------------------

    const bool is_comparison_only =
        ( _col.unit().find( "comparison only" ) != std::string::npos );

    const auto unique_share = num_distinct / num_non_null;

    if ( !is_comparison_only && unique_share > 0.4 )
        {
            _warner->add(
                "The ratio of unique entries to non-NULL entries in column '" +
                _col.name() + "' in data frame '" + _df_name + "' is " +
                std::to_string( unique_share * 100.0 ) +
                "\%. You should "
                "consider setting its role to unused_string or using it for "
                "comparison only (you can do the latter by setting a unit that "
                "contains 'comparison only')." );
        }

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

    for ( size_t i = 0; i < _df.num_categoricals(); ++i )
        {
            check_categorical_column(
                _df.categorical( i ), _df.name(), _warner );
        }

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

    const auto length = static_cast<Float>( _col.size() );

    // --------------------------------------------------------------------------

    assert_true( _col.size() > 0 );

    const Float num_non_null =
        utils::ColumnOperators::count( _col.begin(), _col.end() );

    const auto share_null = 1.0 - num_non_null / length;

    if ( share_null > 0.9 )
        {
            _warner->add(
                std::to_string( share_null * 100.0 ) +
                "\% of all entries in column '" + _col.name() +
                "' in data frame '" + _df_name +
                "' are NULL values. You should "
                "consider setting its role to unused_float." );
        }

    // --------------------------------------------------------------------------

    const auto all_equal = is_all_equal( _col );

    if ( all_equal )
        {
            _warner->add(
                "All non-NULL entries in column '" + _col.name() +
                "' in data frame '" + _df_name +
                "' are equal to each other. You should "
                "consider setting its role to unused_float." );
        }

    // --------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataModelChecker::check_join(
    const Poco::JSON::Object& _population_placeholder,
    const std::vector<std::string>& _peripheral_names,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    communication::Warner* _warner )
{
    // ------------------------------------------------------------------------

    if ( _peripheral_names.size() != _peripheral.size() )
        {
            throw std::invalid_argument(
                "The number of peripheral tables in the placeholder  must be "
                "equal to the number of peripheral tables passed (" +
                std::to_string( _peripheral_names.size() ) + " vs. " +
                std::to_string( _peripheral.size() ) +
                "). This is the point of having placeholders." );
        }

    // ------------------------------------------------------------------------

    const auto joined_tables_arr =
        _population_placeholder.getArray( "joined_tables_" );

    if ( !joined_tables_arr )
        {
            throw std::invalid_argument(
                "The placeholder has no array named 'joined_tables_'!" );
        }

    const auto join_keys_used_arr =
        _population_placeholder.getArray( "join_keys_used_" );

    if ( !join_keys_used_arr )
        {
            throw std::invalid_argument(
                "The placeholder has no array named 'join_keys_used_'!" );
        }

    const auto other_join_keys_used_arr =
        _population_placeholder.getArray( "other_join_keys_used_" );

    if ( !other_join_keys_used_arr )
        {
            throw std::invalid_argument(
                "The placeholder has no array named 'other_join_keys_used_'!" );
        }

    // ------------------------------------------------------------------------

    const auto join_keys_used =
        JSON::array_to_vector<std::string>( join_keys_used_arr );

    const auto other_join_keys_used =
        JSON::array_to_vector<std::string>( other_join_keys_used_arr );

    // ------------------------------------------------------------------------

    if ( joined_tables_arr->size() != join_keys_used.size() )
        {
            throw std::invalid_argument(
                "Length of 'joined_tables_' must match length of "
                "'join_keys_used_'." );
        }

    if ( joined_tables_arr->size() != other_join_keys_used.size() )
        {
            throw std::invalid_argument(
                "Length of 'joined_tables_' must match length of "
                "'other_join_keys_used_'." );
        }

    // ------------------------------------------------------------------------

    for ( size_t i = 0; i < joined_tables_arr->size(); ++i )
        {
            const auto ptr = joined_tables_arr->getObject( i );

            if ( !ptr )
                {
                    throw std::invalid_argument(
                        "Element " + std::to_string( i + 1 ) +
                        " in 'joined_tables_' is not a proper JSON object!" );
                }

            const auto& obj = *ptr;

            const auto name = JSON::get_value<std::string>( obj, "name_" );

            const auto it = std::find(
                _peripheral_names.begin(), _peripheral_names.end(), name );

            if ( it == _peripheral_names.end() )
                {
                    throw std::invalid_argument(
                        "No placeholder called '" + name +
                        "' among the peripheral placeholders." );
                }

            const auto dist = std::distance( _peripheral_names.begin(), it );

            const auto& peripheral_df = _peripheral.at( dist );

            const auto& join_key_used = join_keys_used.at( i );

            const auto& other_join_key_used = other_join_keys_used.at( i );

            check_matches(
                join_key_used,
                other_join_key_used,
                _population,
                peripheral_df,
                _warner );

            check_join(
                obj, _peripheral_names, peripheral_df, _peripheral, _warner );
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataModelChecker::check_matches(
    const std::string& _join_key_used,
    const std::string& _other_join_key_used,
    const containers::DataFrame& _population,
    const containers::DataFrame& _peripheral_df,
    communication::Warner* _warner )
{
    // ------------------------------------------------------------------------

    const auto jk1 = _population.join_key( _join_key_used );

    const auto ptr2 = _peripheral_df.index( _other_join_key_used ).map();

    assert_true( ptr2 );

    const auto& map2 = *ptr2;

    // ------------------------------------------------------------------------

    bool no_matches = true;

    bool is_many_to_one = true;

    // ------------------------------------------------------------------------

    for ( const auto val : jk1 )
        {
            const auto it2 = map2.find( val );

            if ( it2 == map2.end() )
                {
                    continue;
                }

            no_matches = false;

            if ( it2->second.size() > 1 )
                {
                    is_many_to_one = false;
                    break;
                }
        }

    // ------------------------------------------------------------------------

    if ( no_matches )
        {
            _warner->add(
                "There are not matches between '" + _join_key_used + "' in '" +
                _population.name() + "' and '" + _other_join_key_used +
                "' in '" + _peripheral_df.name() +
                "'. You should consider removing this join from your data "
                "model or re-examine your join keys." );

            return;
        }

    // ------------------------------------------------------------------------

    if ( is_many_to_one )
        {
            _warner->add(
                "'" + _population.name() + "' and '" + _peripheral_df.name() +
                "' are in a many-to-one or one-to-one relationship when joined "
                "over '" +
                _join_key_used + "' and '" + _other_join_key_used +
                "'. Aggregating over such relationships makes little sense. "
                "You should consider removing this join from your data "
                "model and directly joining '" +
                _peripheral_df.name() + "' on '" + _population.name() +
                "' using the data frame's built-in join method." );
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

bool DataModelChecker::is_all_equal( const containers::Column<Float>& _col )
{
    auto it = std::find_if( _col.begin(), _col.end(), []( Float val ) {
        return !std::isnan( val );
    } );

    if ( it == _col.end() )
        {
            return true;
        }

    Float val = *it;

    for ( ; it != _col.end(); ++it )
        {
            if ( std::isnan( *it ) )
                {
                    continue;
                }

            if ( val != *it )
                {
                    return false;
                }
        }

    return true;
}

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine
