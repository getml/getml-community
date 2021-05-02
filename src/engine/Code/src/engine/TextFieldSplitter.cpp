#include "engine/preprocessors/preprocessors.hpp"

namespace engine
{
namespace preprocessors
{
// ----------------------------------------------------

containers::DataFrame TextFieldSplitter::add_rowid(
    const containers::DataFrame& _df ) const
{
    const auto range = std::views::iota(
        static_cast<Int>( 0 ), static_cast<Int>( _df.nrows() ) );

    const auto ptr = std::make_shared<std::vector<Int>>(
        stl::collect::vector<Int>( range ) );

    const auto rowid = containers::Column<Int>( ptr, helpers::Macros::rowid() );

    auto df = _df;

    df.add_int_column( rowid, containers::DataFrame::ROLE_JOIN_KEY );

    return df;
}

// ----------------------------------------------------

containers::DataFrame TextFieldSplitter::remove_text_fields(
    const containers::DataFrame& _df ) const
{
    const auto get_name = [&_df]( const size_t _i ) -> std::string {
        return _df.text( _i ).name();
    };

    const auto range =
        std::views::iota( static_cast<size_t>( 0 ), _df.num_text() );

    const auto names = range | std::views::transform( get_name );

    auto df = _df;

    for ( const auto name : names )
        {
            df.remove_column( name );
        }

    return df;
}

// ----------------------------------------------------

Poco::JSON::Object::Ptr TextFieldSplitter::fingerprint() const
{
    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    obj->set( "type_", type() );

    obj->set( "dependencies_", JSON::vector_to_array_ptr( dependencies_ ) );

    return obj;
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
TextFieldSplitter::fit_transform(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<containers::Encoding>& _categories,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const helpers::Placeholder& _placeholder,
    const std::vector<std::string>& _peripheral_names )
{
    cols_ = fit_df( _population_df, helpers::ColumnDescription::POPULATION, 0 );

    for ( size_t i = 0; i < _peripheral_dfs.size(); ++i )
        {
            const auto& df = _peripheral_dfs.at( i );

            const auto new_cols =
                fit_df( df, helpers::ColumnDescription::PERIPHERAL, i );

            cols_.insert( cols_.end(), new_cols.begin(), new_cols.end() );
        }

    return transform(
        _cmd,
        _categories,
        _population_df,
        _peripheral_dfs,
        _placeholder,
        _peripheral_names );
}

// ----------------------------------------------------

std::vector<std::shared_ptr<helpers::ColumnDescription>>
TextFieldSplitter::fit_df(
    const containers::DataFrame& _df,
    const std::string& _marker,
    const size_t _table ) const
{
    const auto to_column_description =
        [&_df, &_marker, _table]( const size_t _i ) {
            return std::make_shared<helpers::ColumnDescription>(
                _marker, std::to_string( _table ), _df.text( _i ).name() );
        };

    const auto iota =
        std::views::iota( static_cast<size_t>( 0 ), _df.num_text() );

    const auto range = iota | std::views::transform( to_column_description );

    return stl::collect::vector<std::shared_ptr<helpers::ColumnDescription>>(
        range );
}

// ----------------------------------------------------

TextFieldSplitter TextFieldSplitter::from_json_obj(
    const Poco::JSON::Object& _obj ) const
{
    TextFieldSplitter that;

    if ( _obj.has( "cols_" ) )
        {
            that.cols_ = PreprocessorImpl::from_array(
                jsonutils::JSON::get_object_array( _obj, "cols_" ) );
        }

    return that;
}

// ----------------------------------------------------

containers::DataFrame TextFieldSplitter::make_new_df(
    const std::string& _df_name,
    const containers::Column<strings::String>& _col ) const
{
    const auto [rownums, words] = split_text_fields_on_col( _col );

    auto df = containers::DataFrame();

    df.set_name( _df_name + helpers::Macros::text_field() + _col.name() );

    df.add_int_column( rownums, containers::DataFrame::ROLE_JOIN_KEY );

    df.add_string_column( words, containers::DataFrame::ROLE_TEXT );

    return df;
}

// ----------------------------------------------------

std::pair<containers::Column<Int>, containers::Column<strings::String>>
TextFieldSplitter::split_text_fields_on_col(
    const containers::Column<strings::String>& _col ) const
{
    const auto rownums_ptr = std::make_shared<std::vector<Int>>();

    const auto words_ptr = std::make_shared<std::vector<strings::String>>();

    for ( size_t i = 0; i < _col.nrows(); ++i )
        {
            const auto splitted =
                textmining::Vocabulary::split_text_field( _col[i] );

            for ( const auto& word : splitted )
                {
                    rownums_ptr->push_back( i );
                    words_ptr->push_back( strings::String( word ) );
                }
        }

    const auto rownums = containers::Column<Int>( rownums_ptr, "rownum" );

    const auto words = containers::Column<strings::String>( words_ptr, "word" );

    return std::make_pair( rownums, words );
}

// ----------------------------------------------------

Poco::JSON::Object::Ptr TextFieldSplitter::to_json_obj() const
{
    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    obj->set( "type_", type() );

    obj->set( "cols_", PreprocessorImpl::to_array( cols_ ) );

    return obj;
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
TextFieldSplitter::transform(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const containers::Encoding> _categories,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const helpers::Placeholder& _placeholder,
    const std::vector<std::string>& _peripheral_names ) const
{
    const auto modify_if_applicable =
        [this]( const containers::DataFrame& _df ) -> containers::DataFrame {
        return _df.num_text() == 0 ? _df
                                   : remove_text_fields( add_rowid( _df ) );
    };

    const auto population_df = modify_if_applicable( _population_df );

    const auto range =
        _peripheral_dfs | std::views::transform( modify_if_applicable );

    auto peripheral_dfs = stl::collect::vector<containers::DataFrame>( range );

    transform_df(
        helpers::ColumnDescription::POPULATION,
        0,
        _population_df,
        &peripheral_dfs );

    for ( size_t i = 0; i < _peripheral_dfs.size(); ++i )
        {
            const auto& df = _peripheral_dfs.at( i );

            transform_df(
                helpers::ColumnDescription::PERIPHERAL,
                i,
                df,
                &peripheral_dfs );
        }

    return std::make_pair( population_df, peripheral_dfs );
}

// ----------------------------------------------------

void TextFieldSplitter::transform_df(
    const std::string& _marker,
    const size_t _table,
    const containers::DataFrame& _df,
    std::vector<containers::DataFrame>* _peripheral_dfs ) const
{
    // ----------------------------------------------------

    const auto table = std::to_string( _table );

    const auto matching_description =
        [&_marker, table](
            const std::shared_ptr<helpers::ColumnDescription>& _desc ) -> bool {
        assert_true( _desc );
        return _desc->marker_ == _marker && _desc->table_ == table;
    };

    // ----------------------------------------------------

    const auto get_col =
        [&_df]( const std::shared_ptr<helpers::ColumnDescription>& _desc )
        -> containers::Column<strings::String> {
        assert_true( _desc );
        return _df.text( _desc->name_ );
    };

    // ----------------------------------------------------

    const auto make_df =
        [this, &_df]( const containers::Column<strings::String>& _col )
        -> containers::DataFrame {
        const auto df = make_new_df( _df.name(), _col );
        return df;
    };

    // ----------------------------------------------------

    auto data_frames = cols_ | std::views::filter( matching_description ) |
                       std::views::transform( get_col ) |
                       std::views::transform( make_df );

    for ( const auto df : data_frames )
        {
            _peripheral_dfs->push_back( df );
        }

    // ----------------------------------------------------
}

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
