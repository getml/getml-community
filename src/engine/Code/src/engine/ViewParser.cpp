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
