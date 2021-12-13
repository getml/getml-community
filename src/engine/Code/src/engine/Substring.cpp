#include "engine/preprocessors/preprocessors.hpp"

namespace engine
{
namespace preprocessors
{
// ----------------------------------------------------

std::optional<containers::Column<Int>> Substring::extract_substring(
    const containers::Column<strings::String>& _col,
    containers::Encoding* _categories ) const
{
    auto str_col = extract_substring_string( _col );

    auto int_col = containers::Column<Int>( _col.pool(), str_col.nrows() );

    for ( size_t i = 0; i < str_col.nrows(); ++i )
        {
            int_col[i] = ( *_categories )[str_col[i]];
        }

    int_col.set_name( make_name( _col.name() ) );

    int_col.set_unit( make_unit( _col.unit() ) );

    if ( PreprocessorImpl::has_warnings( int_col ) )
        {
            return std::nullopt;
        }

    return int_col;
}

// ----------------------------------------------------

containers::Column<Int> Substring::extract_substring(
    const containers::Encoding& _categories,
    const containers::Column<strings::String>& _col ) const
{
    auto str_col = extract_substring_string( _col );

    auto int_col = containers::Column<Int>( _col.pool(), str_col.nrows() );

    for ( size_t i = 0; i < str_col.nrows(); ++i )
        {
            int_col[i] = _categories[str_col[i]];
        }

    int_col.set_name( make_name( _col.name() ) );

    int_col.set_unit( make_unit( _col.unit() ) );

    return int_col;
}

// ----------------------------------------------------

containers::Column<strings::String> Substring::extract_substring_string(
    const containers::Column<strings::String>& _col ) const
{
    auto result = containers::Column<strings::String>( _col.pool() );

    for ( size_t i = 0; i < _col.nrows(); ++i )
        {
            const auto str = _col[i].str();

            const auto substr = str.substr( begin_, length_ );

            result.push_back( strings::String( substr ) );
        }

    return result;
}

// ----------------------------------------------------

Poco::JSON::Object::Ptr Substring::fingerprint() const
{
    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    obj->set( "type_", type() );

    obj->set( "begin_", begin_ );

    obj->set( "length_", length_ );

    obj->set( "unit_", unit_ );

    obj->set( "dependencies_", JSON::vector_to_array_ptr( dependencies_ ) );

    return obj;
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
Substring::fit_transform( const FitParams& _params )
{
    assert_true( _params.categories_ );

    const auto population_df = fit_transform_df(
        _params.population_df_,
        helpers::ColumnDescription::POPULATION,
        0,
        _params.categories_.get() );

    auto peripheral_dfs = std::vector<containers::DataFrame>();

    for ( size_t i = 0; i < _params.peripheral_dfs_.size(); ++i )
        {
            const auto& df = _params.peripheral_dfs_.at( i );

            const auto new_df = fit_transform_df(
                df,
                helpers::ColumnDescription::PERIPHERAL,
                i,
                _params.categories_.get() );

            peripheral_dfs.push_back( new_df );
        }

    return std::make_pair( population_df, peripheral_dfs );
}

// ----------------------------------------------------

containers::DataFrame Substring::fit_transform_df(
    const containers::DataFrame& _df,
    const std::string& _marker,
    const size_t _table,
    containers::Encoding* _categories )
{
    auto df = _df;

    for ( size_t i = 0; i < _df.num_categoricals(); ++i )
        {
            const auto& original_col = _df.categorical( i );

            extract_and_add( _marker, _table, original_col, _categories, &df );
        }

    for ( size_t i = 0; i < _df.num_text(); ++i )
        {
            const auto& original_col = _df.text( i );

            extract_and_add( _marker, _table, original_col, _categories, &df );
        }

    return df;
}

// ----------------------------------------------------

Substring Substring::from_json_obj( const Poco::JSON::Object& _obj ) const
{
    Substring that;

    that.begin_ = jsonutils::JSON::get_value<size_t>( _obj, "begin_" );

    that.length_ = jsonutils::JSON::get_value<size_t>( _obj, "length_" );

    that.unit_ = jsonutils::JSON::get_value<std::string>( _obj, "unit_" );

    if ( _obj.has( "cols_" ) )
        {
            that.cols_ = PreprocessorImpl::from_array(
                jsonutils::JSON::get_object_array( _obj, "cols_" ) );
        }

    return that;
}

// ----------------------------------------------------

containers::Column<strings::String> Substring::make_str_col(
    const containers::Encoding& _categories,
    const containers::Column<Int>& _col ) const
{
    auto result = containers::Column<strings::String>( _col.pool() );

    for ( size_t i = 0; i < _col.nrows(); ++i )
        {
            result.push_back( _categories[_col[i]] );
        }

    result.set_name( _col.name() );
    result.set_unit( _col.unit() );

    return result;
}

// ----------------------------------------------------

Poco::JSON::Object::Ptr Substring::to_json_obj() const
{
    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    obj->set( "type_", type() );

    obj->set( "cols_", PreprocessorImpl::to_array( cols_ ) );

    obj->set( "begin_", begin_ );

    obj->set( "length_", length_ );

    obj->set( "unit_", unit_ );

    return obj;
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
Substring::transform( const TransformParams& _params ) const
{
    assert_true( _params.categories_ );

    const auto population_df = transform_df(
        *_params.categories_,
        _params.population_df_,
        helpers::ColumnDescription::POPULATION,
        0 );

    auto peripheral_dfs = std::vector<containers::DataFrame>();

    for ( size_t i = 0; i < _params.peripheral_dfs_.size(); ++i )
        {
            const auto& df = _params.peripheral_dfs_.at( i );

            const auto new_df = transform_df(
                *_params.categories_,
                df,
                helpers::ColumnDescription::PERIPHERAL,
                i );

            peripheral_dfs.push_back( new_df );
        }

    return std::make_pair( population_df, peripheral_dfs );
}

// ----------------------------------------------------

containers::DataFrame Substring::transform_df(
    const containers::Encoding& _categories,
    const containers::DataFrame& _df,
    const std::string& _marker,
    const size_t _table ) const
{
    // ----------------------------------------------------

    auto df = _df;

    // ----------------------------------------------------

    auto names = PreprocessorImpl::retrieve_names( _marker, _table, cols_ );

    for ( const auto& name : names )
        {
            if ( _df.has_categorical( name ) )
                {
                    const auto col = extract_substring(
                        _categories, _df.categorical( name ) );

                    df.add_int_column(
                        col, containers::DataFrame::ROLE_CATEGORICAL );
                }
            else if ( _df.has_text( name ) )
                {
                    const auto col =
                        extract_substring( _categories, _df.text( name ) );

                    df.add_int_column(
                        col, containers::DataFrame::ROLE_CATEGORICAL );
                }
            else
                {
                    throw std::invalid_argument(
                        "'" + _df.name() +
                        "' has no categorical or text column named '" + name +
                        "'!" );
                }
        }

    // ----------------------------------------------------

    return df;
}

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
