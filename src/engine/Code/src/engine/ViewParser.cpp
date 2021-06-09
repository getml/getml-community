#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

void ViewParser::add_column(
    const Poco::JSON::Object& _obj, containers::DataFrame* _df )
{
    if ( !_obj.has( "added_" ) )
        {
            return;
        }

    const auto added = *JSON::get_object( _obj, "added_" );

    const auto json_col = *JSON::get_object( added, "col_" );

    const auto name = JSON::get_value<std::string>( added, "name_" );

    const auto role = JSON::get_value<std::string>( added, "role_" );

    const auto subroles = JSON::array_to_vector<std::string>(
        JSON::get_array( added, "subroles_" ) );

    const auto unit = JSON::get_value<std::string>( added, "unit_" );

    const auto type = JSON::get_value<std::string>( json_col, "type_" );

    if ( type == FLOAT_COLUMN || type == FLOAT_COLUMN_VIEW )
        {
            const auto column_view =
                NumOpParser( categories_, join_keys_encoding_, data_frames_ )
                    .parse( json_col );

            auto col = column_view.to_column( 0, _df->nrows(), true );

            col.set_name( name );

            col.set_subroles( subroles );

            col.set_unit( unit );

            _df->add_float_column( col, role );
        }

    if ( type == STRING_COLUMN || type == STRING_COLUMN_VIEW )
        {
            const auto column_view =
                CatOpParser( categories_, join_keys_encoding_, data_frames_ )
                    .parse( json_col );

            const auto vec = column_view.to_vector( 0, _df->nrows(), true );

            assert_true( vec );

            if ( role == containers::DataFrame::ROLE_CATEGORICAL ||
                 role == containers::DataFrame::ROLE_JOIN_KEY )
                {
                    add_int_column_to_df(
                        name, role, subroles, unit, *vec, _df );
                }

            if ( role == containers::DataFrame::ROLE_UNUSED ||
                 role == containers::DataFrame::ROLE_TEXT ||
                 role == containers::DataFrame::ROLE_UNUSED_FLOAT )
                {
                    add_string_column_to_df(
                        name, role, subroles, unit, *vec, _df );
                }
        }
}

// ------------------------------------------------------------------------

void ViewParser::add_int_column_to_df(
    const std::string& _name,
    const std::string& _role,
    const std::vector<std::string>& _subroles,
    const std::string& _unit,
    const std::vector<std::string>& _vec,
    containers::DataFrame* _df )
{
    const auto encoding = _role == containers::DataFrame::ROLE_CATEGORICAL
                              ? categories_
                              : join_keys_encoding_;

    auto col = containers::Column<Int>( _vec.size() );

    for ( size_t i = 0; i < _vec.size(); ++i )
        {
            col[i] = ( *encoding )[_vec[i]];
        }

    col.set_name( _name );

    col.set_subroles( _subroles );

    col.set_unit( _unit );

    _df->add_int_column( col, _role );

    if ( _role == containers::DataFrame::ROLE_JOIN_KEY )
        {
            _df->create_indices();
        }
}

// ----------------------------------------------------------------------------

void ViewParser::add_string_column_to_df(
    const std::string& _name,
    const std::string& _role,
    const std::vector<std::string>& _subroles,
    const std::string& _unit,
    const std::vector<std::string>& _vec,
    containers::DataFrame* _df )
{
    auto col = containers::Column<strings::String>( _vec.size() );

    for ( size_t i = 0; i < _vec.size(); ++i )
        {
            col[i] = strings::String( _vec[i] );
        }

    col.set_name( _name );

    col.set_subroles( _subroles );

    col.set_unit( _unit );

    _df->add_string_column( col, _role );
}

// ----------------------------------------------------------------------------

void ViewParser::drop_columns(
    const Poco::JSON::Object& _obj, containers::DataFrame* _df ) const
{
    const auto dropped = JSON::array_to_vector<std::string>(
        JSON::get_array( _obj, "dropped_" ) );

    for ( const auto& name : dropped )
        {
            _df->remove_column( name );
        }
}

// ----------------------------------------------------------------------------

typename ViewParser::ColumnViewVariant ViewParser::make_column_view(
    Poco::JSON::Object::Ptr _ptr ) const
{
    assert_true( _ptr );

    const auto type = JSON::get_value<std::string>( *_ptr, "type_" );

    if ( type == STRING_COLUMN || type == STRING_COLUMN_VIEW )
        {
            return CatOpParser( categories_, join_keys_encoding_, data_frames_ )
                .parse( *_ptr );
        }

    return NumOpParser( categories_, join_keys_encoding_, data_frames_ )
        .parse( *_ptr );
}

// ----------------------------------------------------------------------------

Poco::JSON::Array::Ptr ViewParser::make_data(
    const std::vector<std::vector<std::string>>& _string_vectors ) const
{
    auto data = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    if ( _string_vectors.size() == 0 )
        {
            return data;
        }

    for ( size_t i = 0; i < _string_vectors.at( 0 ).size(); ++i )
        {
            auto row = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

            for ( size_t j = 0; j < _string_vectors.size(); ++j )
                {
                    if ( i == 0 && _string_vectors.at( j ).size() !=
                                       _string_vectors.at( 0 ).size() )
                        {
                            throw std::runtime_error(
                                "ViewParser: Resulting columns do not all have "
                                "the same length!" );
                        }

                    row->add( _string_vectors.at( j ).at( i ) );
                }

            data->add( row );
        }

    return data;
}

// ----------------------------------------------------------------------------

std::optional<size_t> ViewParser::make_nrows(
    const std::vector<ColumnViewVariant>& _column_views ) const
{
    const auto has_nrows = []( const ColumnViewVariant& _column_view ) -> bool {
        if ( std::holds_alternative<containers::ColumnView<Float>>(
                 _column_view ) )
            {
                const auto float_col =
                    std::get<containers::ColumnView<Float>>( _column_view );
                return std::holds_alternative<size_t>( float_col.nrows() );
            }
        const auto str_col =
            std::get<containers::ColumnView<std::string>>( _column_view );
        return std::holds_alternative<size_t>( str_col.nrows() );
    };

    const auto get_nrows =
        []( const ColumnViewVariant& _column_view ) -> size_t {
        if ( std::holds_alternative<containers::ColumnView<Float>>(
                 _column_view ) )
            {
                const auto float_col =
                    std::get<containers::ColumnView<Float>>( _column_view );
                return std::get<size_t>( float_col.nrows() );
            }

        const auto str_col =
            std::get<containers::ColumnView<std::string>>( _column_view );
        return std::get<size_t>( str_col.nrows() );
    };

    const auto it =
        std::find_if( _column_views.begin(), _column_views.end(), has_nrows );

    return it == _column_views.end() ? std::optional<size_t>()
                                     : std::make_optional( get_nrows( *it ) );
}

// ----------------------------------------------------------------------------

std::vector<std::string> ViewParser::make_string_vector(
    const size_t _start,
    const size_t _length,
    const ColumnViewVariant& _column_view ) const
{
    const bool is_string_column =
        std::holds_alternative<containers::ColumnView<std::string>>(
            _column_view );

    if ( is_string_column )
        {
            const auto str_col =
                std::get<containers::ColumnView<std::string>>( _column_view );

            return *str_col.to_vector( _start, _length, false );
        }

    const auto float_col =
        std::get<containers::ColumnView<Float>>( _column_view );

    const auto float_vec = *float_col.to_vector( _start, _length, false );

    if ( float_col.unit().find( "time stamp" ) != std::string::npos )
        {
            return stl::collect::vector<std::string>(
                float_vec | std::views::transform( io::Parser::ts_to_string ) );
        }

    const auto to_string = []( const Float _val ) {
        return io::Parser::to_string( _val );
    };

    return stl::collect::vector<std::string>(
        float_vec | std::views::transform( to_string ) );
}

// ----------------------------------------------------------------------------

Poco::JSON::Object ViewParser::get_content(
    const Poco::JSON::Object& _cmd ) const
{
    // ------------------------------------------------------------------------

    const auto to_column_view =
        [this]( Poco::JSON::Object::Ptr _ptr ) -> ColumnViewVariant {
        return make_column_view( _ptr );
    };

    // ------------------------------------------------------------------------

    const auto draw = JSON::get_value<size_t>( _cmd, "draw_" );

    const auto start = JSON::get_value<size_t>( _cmd, "start_" );

    const auto length = JSON::get_value<size_t>( _cmd, "length_" );

    // ------------------------------------------------------------------------

    const auto to_string_vector =
        [this, start, length]( const ColumnViewVariant& _column_view )
        -> std::vector<std::string> {
        return make_string_vector( start, length, _column_view );
    };

    // ------------------------------------------------------------------------

    const auto cols = jsonutils::JSON::get_object_array( _cmd, "cols_" );

    const auto get_object = [cols]( const size_t _i ) {
        return cols->getObject( _i );
    };

    const auto iota = stl::iota<size_t>( 0, cols->size() );

    const auto column_views = stl::collect::vector<ColumnViewVariant>(
        iota | std::views::transform( get_object ) |
        std::views::transform( to_column_view ) );

    const auto string_vectors = stl::collect::vector<std::vector<std::string>>(
        column_views | std::views::transform( to_string_vector ) );

    // ------------------------------------------------------------------------

    const auto data = make_data( string_vectors );

    const auto nrows = make_nrows( column_views );

    // ------------------------------------------------------------------------

    Poco::JSON::Object obj;

    obj.set( "draw", draw );

    obj.set( "data", data );

    if ( nrows )
        {
            obj.set( "recordsTotal", *nrows );

            obj.set( "recordsFiltered", *nrows );
        }

    return obj;
}

// ----------------------------------------------------------------------------

containers::DataFrame ViewParser::parse( const Poco::JSON::Object& _obj )
{
    const auto type = JSON::get_value<std::string>( _obj, "type_" );

    if ( type == "DataFrame" )
        {
            const auto name = JSON::get_value<std::string>( _obj, "name_" );
            return utils::Getter::get( name, *data_frames_ );
        }

    const auto base = *JSON::get_object( _obj, "base_" );

    auto df = parse( base );

    add_column( _obj, &df );

    drop_columns( _obj, &df );

    subselection( _obj, &df );

    df.set_build_history(
        Poco::JSON::Object::Ptr( new Poco::JSON::Object( _obj ) ) );

    return df;
}

// ----------------------------------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
ViewParser::parse_all( const Poco::JSON::Object& _cmd )
{
    const auto to_df =
        [this]( const Poco::JSON::Object::Ptr& _obj ) -> containers::DataFrame {
        assert_true( _obj );
        return parse( *_obj );
    };

    const auto population_obj = JSON::get_object( _cmd, "population_df_" );

    const auto peripheral_objs =
        JSON::array_to_obj_vector( JSON::get_array( _cmd, "peripheral_dfs_" ) );

    const auto population = to_df( population_obj );

    const auto peripheral = stl::collect::vector<containers::DataFrame>(
        peripheral_objs | std::views::transform( to_df ) );

    return std::make_pair( population, peripheral );
}

// ----------------------------------------------------------------------------

void ViewParser::subselection(
    const Poco::JSON::Object& _obj, containers::DataFrame* _df ) const
{
    if ( !_obj.has( "subselection_" ) )
        {
            return;
        }

    const auto json_col = *JSON::get_object( _obj, "subselection_" );

    const auto type = JSON::get_value<std::string>( json_col, "type_" );

    if ( type == BOOLEAN_COLUMN_VIEW )
        {
            const auto column_view =
                BoolOpParser( categories_, join_keys_encoding_, data_frames_ )
                    .parse( json_col );

            const auto data_ptr =
                column_view.to_vector( 0, _df->nrows(), true );

            assert_true( data_ptr );

            _df->where( *data_ptr );
        }
    else
        {
            const auto data_ptr =
                NumOpParser( categories_, join_keys_encoding_, data_frames_ )
                    .parse( json_col )
                    .to_vector( 0, std::nullopt, false );

            assert_true( data_ptr );

            const auto& key_float = *data_ptr;

            auto key = std::vector<size_t>( key_float.size() );

            for ( size_t i = 0; i < key_float.size(); ++i )
                {
                    const auto ix_float = key_float[i];

                    if ( ix_float < 0.0 )
                        {
                            throw std::runtime_error(
                                "Index on a numerical subselection cannot be "
                                "smaller than zero!" );
                        }

                    const auto ix = static_cast<size_t>( ix_float );

                    if ( ix >= _df->nrows() )
                        {
                            throw std::runtime_error(
                                "Index on a numerical subselection out of "
                                "bounds!" );
                        }

                    key[i] = ix;
                }

            _df->sort_by_key( key );
        }
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine
