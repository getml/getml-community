#include "engine/preprocessors/preprocessors.hpp"

namespace engine
{
namespace preprocessors
{
// ----------------------------------------------------

std::optional<containers::Column<Int>> Seasonal::extract_hour(
    const containers::Column<Float>& _col,
    containers::Encoding* _categories ) const
{
    auto result = to_categorical( _col, utils::Time::hour, _categories );

    result.set_name( "$GETML_HOUR" + _col.name() );
    result.set_unit( "hour" );

    if ( PreprocessorImpl::has_warnings( result ) )
        {
            return std::nullopt;
        }

    return result;
}

// ----------------------------------------------------

containers::Column<Int> Seasonal::extract_hour(
    const containers::Encoding& _categories,
    const containers::Column<Float>& _col ) const
{
    auto result = to_categorical( _categories, _col, utils::Time::hour );

    result.set_name( "$GETML_HOUR" + _col.name() );
    result.set_unit( "hour" );

    return result;
}

// ----------------------------------------------------

std::optional<containers::Column<Int>> Seasonal::extract_minute(
    const containers::Column<Float>& _col,
    containers::Encoding* _categories ) const
{
    auto result = to_categorical( _col, utils::Time::minute, _categories );

    result.set_name( "$GETML_MINUTE" + _col.name() );
    result.set_unit( "minute" );

    if ( PreprocessorImpl::has_warnings( result ) )
        {
            return std::nullopt;
        }

    return result;
}

// ----------------------------------------------------

containers::Column<Int> Seasonal::extract_minute(
    const containers::Encoding& _categories,
    const containers::Column<Float>& _col ) const
{
    auto result = to_categorical( _categories, _col, utils::Time::hour );

    result.set_name( "$GETML_MINUTE" + _col.name() );
    result.set_unit( "minute" );

    return result;
}

// ----------------------------------------------------

std::optional<containers::Column<Int>> Seasonal::extract_month(
    const containers::Column<Float>& _col,
    containers::Encoding* _categories ) const
{
    auto result = to_categorical( _col, utils::Time::month, _categories );

    result.set_name( "$GETML_MONTH" + _col.name() );
    result.set_unit( "month" );

    if ( PreprocessorImpl::has_warnings( result ) )
        {
            return std::nullopt;
        }

    return result;
}

// ----------------------------------------------------

containers::Column<Int> Seasonal::extract_month(
    const containers::Encoding& _categories,
    const containers::Column<Float>& _col ) const
{
    auto result = to_categorical( _categories, _col, utils::Time::month );

    result.set_name( "$GETML_MONTH" + _col.name() );
    result.set_unit( "month" );

    return result;
}

// ----------------------------------------------------

std::optional<containers::Column<Int>> Seasonal::extract_weekday(
    const containers::Column<Float>& _col,
    containers::Encoding* _categories ) const
{
    auto result = to_categorical( _col, utils::Time::weekday, _categories );

    result.set_name( "$GETML_WEEKDAY" + _col.name() );
    result.set_unit( "weekday" );

    if ( PreprocessorImpl::has_warnings( result ) )
        {
            return std::nullopt;
        }

    return result;
}

// ----------------------------------------------------

containers::Column<Int> Seasonal::extract_weekday(
    const containers::Encoding& _categories,
    const containers::Column<Float>& _col ) const
{
    auto result = to_categorical( _categories, _col, utils::Time::weekday );

    result.set_name( "$GETML_WEEKDAY" + _col.name() );
    result.set_unit( "weekday" );

    return result;
}

// ----------------------------------------------------

std::optional<containers::Column<Float>> Seasonal::extract_year(
    const containers::Column<Float>& _col )
{
    auto result = to_numerical( _col, utils::Time::year );

    result.set_name( "$GETML_YEAR" + _col.name() );
    result.set_unit( "year, comparison only" );

    if ( PreprocessorImpl::has_warnings( result ) )
        {
            return std::nullopt;
        }

    return result;
}

// ----------------------------------------------------

containers::Column<Float> Seasonal::extract_year(
    const containers::Column<Float>& _col ) const
{
    auto result = to_numerical( _col, utils::Time::year );

    result.set_name( "$GETML_YEAR" + _col.name() );
    result.set_unit( "year, comparison only" );

    return result;
}

// ----------------------------------------------------

void Seasonal::fit_transform(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<containers::Encoding>& _categories,
    std::map<std::string, containers::DataFrame>* _data_frames )
{
    assert_true( _categories );

    auto [population_df, peripheral_dfs] =
        PreprocessorImpl::extract_data_frames( _cmd, *_data_frames );

    population_df = fit_transform_df(
        population_df,
        helpers::ColumnDescription::POPULATION,
        0,
        _categories.get() );

    for ( size_t i = 0; i < peripheral_dfs.size(); ++i )
        {
            auto& df = peripheral_dfs.at( i );

            df = fit_transform_df(
                df,
                helpers::ColumnDescription::PERIPHERAL,
                i,
                _categories.get() );
        }

    PreprocessorImpl::insert_data_frames(
        _cmd, population_df, peripheral_dfs, _data_frames );
}

// ----------------------------------------------------

containers::DataFrame Seasonal::fit_transform_df(
    const containers::DataFrame& _df,
    const std::string& _marker,
    const size_t _table,
    containers::Encoding* _categories )
{
    auto df = _df;

    for ( size_t i = 0; i < _df.num_time_stamps(); ++i )
        {
            // -----------------------------------

            const auto& ts = _df.time_stamp( i );

            // -----------------------------------

            auto col = extract_hour( ts, _categories );

            if ( col )
                {
                    PreprocessorImpl::add( _marker, _table, ts.name(), &hour_ );
                    df.add_int_column(
                        *col, containers::DataFrame::ROLE_CATEGORICAL );
                }

            // -----------------------------------

            col = extract_minute( ts, _categories );

            if ( col )
                {
                    PreprocessorImpl::add(
                        _marker, _table, ts.name(), &minute_ );
                    df.add_int_column(
                        *col, containers::DataFrame::ROLE_CATEGORICAL );
                }

            // -----------------------------------

            col = extract_month( ts, _categories );

            if ( col )
                {
                    PreprocessorImpl::add(
                        _marker, _table, ts.name(), &month_ );
                    df.add_int_column(
                        *col, containers::DataFrame::ROLE_CATEGORICAL );
                }

            // -----------------------------------

            col = extract_weekday( ts, _categories );

            if ( col )
                {
                    PreprocessorImpl::add(
                        _marker, _table, ts.name(), &weekday_ );
                    df.add_int_column(
                        *col, containers::DataFrame::ROLE_CATEGORICAL );
                }

            // -----------------------------------

            const auto year = extract_year( ts );

            if ( year )
                {
                    PreprocessorImpl::add( _marker, _table, ts.name(), &year_ );
                    df.add_float_column(
                        *year, containers::DataFrame::ROLE_NUMERICAL );
                }

            // -----------------------------------
        }

    return df;
}

// ----------------------------------------------------

Seasonal Seasonal::from_json_obj( const Poco::JSON::Object& _obj ) const
{
    Seasonal that;

    if ( _obj.has( "hour_" ) )
        {
            that.hour_ = PreprocessorImpl::from_array(
                jsonutils::JSON::get_object_array( _obj, "hour_" ) );
        }

    if ( _obj.has( "minute_" ) )
        {
            that.minute_ = PreprocessorImpl::from_array(
                jsonutils::JSON::get_object_array( _obj, "minute_" ) );
        }

    if ( _obj.has( "month_" ) )
        {
            that.month_ = PreprocessorImpl::from_array(
                jsonutils::JSON::get_object_array( _obj, "month_" ) );
        }

    if ( _obj.has( "weekday_" ) )
        {
            that.weekday_ = PreprocessorImpl::from_array(
                jsonutils::JSON::get_object_array( _obj, "weekday_" ) );
        }

    if ( _obj.has( "year_" ) )
        {
            that.year_ = PreprocessorImpl::from_array(
                jsonutils::JSON::get_object_array( _obj, "year_" ) );
        }

    return that;
}

// ----------------------------------------------------

containers::Column<Int> Seasonal::to_int(
    const containers::Column<Float>& _col,
    containers::Encoding* _categories ) const
{
    const auto to_str = [_categories]( const Float val ) {
        return ( *_categories )[io::Parser::to_string( val )];
    };

    auto result = containers::Column<Int>( _col.nrows() );

    std::transform( _col.begin(), _col.end(), result.begin(), to_str );

    return result;
}

// ----------------------------------------------------

containers::Column<Int> Seasonal::to_int(
    const containers::Encoding& _categories,
    const containers::Column<Float>& _col ) const
{
    const auto to_str = [&_categories]( const Float val ) {
        return _categories[io::Parser::to_string( val )];
    };

    auto result = containers::Column<Int>( _col.nrows() );

    std::transform( _col.begin(), _col.end(), result.begin(), to_str );

    return result;
}

// ----------------------------------------------------

Poco::JSON::Object::Ptr Seasonal::to_json_obj() const
{
    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    obj->set( "hour_", PreprocessorImpl::to_array( hour_ ) );

    obj->set( "minute_", PreprocessorImpl::to_array( minute_ ) );

    obj->set( "month_", PreprocessorImpl::to_array( month_ ) );

    obj->set( "weekday_", PreprocessorImpl::to_array( weekday_ ) );

    obj->set( "year_", PreprocessorImpl::to_array( year_ ) );

    return obj;
}

// ----------------------------------------------------

void Seasonal::transform(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const containers::Encoding> _categories,
    std::map<std::string, containers::DataFrame>* _data_frames ) const
{
    assert_true( _categories );

    auto [population_df, peripheral_dfs] =
        PreprocessorImpl::extract_data_frames( _cmd, *_data_frames );

    population_df = transform_df(
        *_categories,
        population_df,
        helpers::ColumnDescription::POPULATION,
        0 );

    for ( size_t i = 0; i < peripheral_dfs.size(); ++i )
        {
            auto& df = peripheral_dfs.at( i );

            df = transform_df(
                *_categories, df, helpers::ColumnDescription::PERIPHERAL, i );
        }

    PreprocessorImpl::insert_data_frames(
        _cmd, population_df, peripheral_dfs, _data_frames );
}

// ----------------------------------------------------

containers::DataFrame Seasonal::transform_df(
    const containers::Encoding& _categories,
    const containers::DataFrame& _df,
    const std::string& _marker,
    const size_t _table ) const
{
    // ----------------------------------------------------

    auto df = _df;

    // ----------------------------------------------------

    auto names = PreprocessorImpl::retrieve_names( _marker, _table, hour_ );

    for ( const auto& name : names )
        {
            const auto col = extract_hour( _categories, df.time_stamp( name ) );

            df.add_int_column( col, containers::DataFrame::ROLE_CATEGORICAL );
        }

    // ----------------------------------------------------

    names = PreprocessorImpl::retrieve_names( _marker, _table, minute_ );

    for ( const auto& name : names )
        {
            const auto col =
                extract_minute( _categories, df.time_stamp( name ) );

            df.add_int_column( col, containers::DataFrame::ROLE_CATEGORICAL );
        }

    // ----------------------------------------------------

    names = PreprocessorImpl::retrieve_names( _marker, _table, month_ );

    for ( const auto& name : names )
        {
            const auto col =
                extract_month( _categories, df.time_stamp( name ) );

            df.add_int_column( col, containers::DataFrame::ROLE_CATEGORICAL );
        }

    // ----------------------------------------------------

    names = PreprocessorImpl::retrieve_names( _marker, _table, weekday_ );

    for ( const auto& name : names )
        {
            const auto col =
                extract_weekday( _categories, df.time_stamp( name ) );

            df.add_int_column( col, containers::DataFrame::ROLE_CATEGORICAL );
        }

    // ----------------------------------------------------

    names = PreprocessorImpl::retrieve_names( _marker, _table, year_ );

    for ( const auto& name : names )
        {
            const auto col = extract_year( df.time_stamp( name ) );

            df.add_float_column( col, containers::DataFrame::ROLE_NUMERICAL );
        }

    // ----------------------------------------------------

    return df;
}

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
