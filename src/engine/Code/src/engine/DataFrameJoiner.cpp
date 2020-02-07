#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

void DataFrameJoiner::add_all(
    const containers::DataFrame& _df,
    const std::vector<size_t>& _rindices,
    containers::DataFrame* _joined_df )
{
    for ( size_t i = 0; i < _df.num_categoricals(); ++i )
        {
            if ( _joined_df->has( _df.categorical( i ).name() ) )
                {
                    throw std::invalid_argument(
                        "Duplicate column: '" + _df.categorical( i ).name() +
                        "'." );
                }

            _joined_df->add_int_column(
                _df.categorical( i ).sort_by_key( _rindices ), "categorical" );
        }

    for ( size_t i = 0; i < _df.num_join_keys(); ++i )
        {
            if ( _joined_df->has( _df.join_key( i ).name() ) )
                {
                    throw std::invalid_argument(
                        "Duplicate column: '" + _df.join_key( i ).name() +
                        "'." );
                }

            _joined_df->add_int_column(
                _df.join_key( i ).sort_by_key( _rindices ), "join_key" );
        }

    for ( size_t i = 0; i < _df.num_numericals(); ++i )
        {
            if ( _joined_df->has( _df.numerical( i ).name() ) )
                {
                    throw std::invalid_argument(
                        "Duplicate column: '" + _df.numerical( i ).name() +
                        "'." );
                }

            _joined_df->add_float_column(
                _df.numerical( i ).sort_by_key( _rindices ), "numerical" );
        }

    for ( size_t i = 0; i < _df.num_targets(); ++i )
        {
            if ( _joined_df->has( _df.target( i ).name() ) )
                {
                    throw std::invalid_argument(
                        "Duplicate column: '" + _df.target( i ).name() + "'." );
                }

            _joined_df->add_float_column(
                _df.target( i ).sort_by_key( _rindices ), "target" );
        }

    for ( size_t i = 0; i < _df.num_time_stamps(); ++i )
        {
            if ( _joined_df->has( _df.time_stamp( i ).name() ) )
                {
                    throw std::invalid_argument(
                        "Duplicate column: '" + _df.time_stamp( i ).name() +
                        "'." );
                }

            _joined_df->add_float_column(
                _df.time_stamp( i ).sort_by_key( _rindices ), "time_stamp" );
        }

    for ( size_t i = 0; i < _df.num_unused_floats(); ++i )
        {
            if ( _joined_df->has( _df.unused_float( i ).name() ) )
                {
                    throw std::invalid_argument(
                        "Duplicate column: '" + _df.unused_float( i ).name() +
                        "'." );
                }

            _joined_df->add_float_column(
                _df.unused_float( i ).sort_by_key( _rindices ), "unused" );
        }

    for ( size_t i = 0; i < _df.num_unused_strings(); ++i )
        {
            if ( _joined_df->has( _df.unused_string( i ).name() ) )
                {
                    throw std::invalid_argument(
                        "Duplicate column: '" + _df.unused_string( i ).name() +
                        "'." );
                }

            _joined_df->add_string_column(
                _df.unused_string( i ).sort_by_key( _rindices ) );
        }
}

// ----------------------------------------------------------------------------

void DataFrameJoiner::add_col(
    const containers::DataFrame& _df,
    const std::vector<size_t>& _rindices,
    const std::string& _name,
    const std::string& _role,
    const std::string& _as,
    containers::DataFrame* _joined_df )
{
    if ( _role == "categorical" )
        {
            if ( _joined_df->has( _as ) )
                {
                    throw std::invalid_argument(
                        "Duplicate column: '" + _name + "'." );
                }
            auto col = _df.categorical( _name ).sort_by_key( _rindices );
            col.set_name( _as );
            _joined_df->add_int_column( col, _role );
        }
    else if ( _role == "join_key" )
        {
            if ( _joined_df->has( _as ) )
                {
                    throw std::invalid_argument(
                        "Duplicate column: '" + _name + "'." );
                }
            auto col = _df.join_key( _name ).sort_by_key( _rindices );
            col.set_name( _as );
            _joined_df->add_int_column( col, _role );
        }
    else if ( _role == "numerical" )
        {
            if ( _joined_df->has( _as ) )
                {
                    throw std::invalid_argument(
                        "Duplicate column: '" + _name + "'." );
                }
            auto col = _df.numerical( _name ).sort_by_key( _rindices );
            col.set_name( _as );
            _joined_df->add_float_column( col, _role );
        }
    else if ( _role == "target" )
        {
            if ( _joined_df->has( _as ) )
                {
                    throw std::invalid_argument(
                        "Duplicate column: '" + _name + "'." );
                }
            auto col = _df.target( _name ).sort_by_key( _rindices );
            col.set_name( _as );
            _joined_df->add_float_column( col, _role );
        }
    else if ( _role == "time_stamp" )
        {
            if ( _joined_df->has( _as ) )
                {
                    throw std::invalid_argument(
                        "Duplicate column: '" + _name + "'." );
                }
            auto col = _df.time_stamp( _name ).sort_by_key( _rindices );
            col.set_name( _as );
            _joined_df->add_float_column( col, _role );
        }
    else if ( _role == "unused" )
        {
            if ( _joined_df->has( _as ) )
                {
                    throw std::invalid_argument(
                        "Duplicate column: '" + _name + "'." );
                }

            if ( _df.has_unused_float( _name ) )
                {
                    auto col =
                        _df.unused_float( _name ).sort_by_key( _rindices );
                    col.set_name( _as );
                    _joined_df->add_float_column( col, _role );
                }
            else
                {
                    auto col =
                        _df.unused_string( _name ).sort_by_key( _rindices );
                    col.set_name( _as );
                    _joined_df->add_string_column( col );
                }
        }
    else
        {
            throw std::invalid_argument(
                "Role '" + _role + "' not recognized." );
        }
}

// ----------------------------------------------------------------------------

void DataFrameJoiner::add_cols(
    const containers::DataFrame& _df,
    const std::vector<size_t>& _rindices,
    const Poco::JSON::Array& _cols,
    containers::DataFrame* _joined_df )
{
    for ( size_t i = 0; i < _cols.size(); ++i )
        {
            const auto obj = *_cols.getObject( i );

            const auto name =
                jsonutils::JSON::get_value<std::string>( obj, "name_" );

            const auto df_name =
                jsonutils::JSON::get_value<std::string>( obj, "df_name_" );

            if ( df_name != _df.name() )
                {
                    throw std::invalid_argument(
                        "Column '" + name +
                        "' is expected to be from DataFrame '" + _df.name() +
                        "', but is from DataFrame '" + df_name + "'." );
                }

            const auto role =
                jsonutils::JSON::get_value<std::string>( obj, "role_" );

            const auto as =
                obj.has( "as_" )
                    ? jsonutils::JSON::get_value<std::string>( obj, "as_" )
                    : name;

            add_col( _df, _rindices, name, role, as, _joined_df );
        }
}

// ----------------------------------------------------------------------------

void DataFrameJoiner::build_temp_dfs(
    const containers::DataFrame& _df1,
    const containers::DataFrame& _df2,
    const std::vector<size_t>& _rindices1,
    const std::vector<size_t>& _rindices2,
    const Poco::JSON::Object& _col,
    containers::DataFrame* _temp_df1,
    containers::DataFrame* _temp_df2 )
{
    // ------------------------------------------------------------------------

    const auto type = JSON::get_value<std::string>( _col, "type_" );

    // ------------------------------------------------------------------------

    if ( type == "FloatColumn" || type == "StringColumn" )
        {
            const auto name = JSON::get_value<std::string>( _col, "name_" );

            const auto role = JSON::get_value<std::string>( _col, "role_" );

            const auto df_name =
                JSON::get_value<std::string>( _col, "df_name_" );

            if ( df_name == _df1.name() )
                {
                    try
                        {
                            add_col(
                                _df1, _rindices1, name, role, name, _temp_df1 );
                        }
                    catch ( std::exception& e )
                        {
                        }
                }
            else if ( df_name == _df2.name() )
                {
                    try
                        {
                            add_col(
                                _df2, _rindices2, name, role, name, _temp_df2 );
                        }
                    catch ( std::exception& e )
                        {
                        }
                }
            else
                {
                    throw std::invalid_argument(
                        "Column '" + df_name + "' is part of DataFrame '" +
                        df_name + "'." );
                }
        }

    // ------------------------------------------------------------------------

    if ( _col.has( "operand1_" ) )
        {
            build_temp_dfs(
                _df1,
                _df2,
                _rindices1,
                _rindices2,
                *JSON::get_object( _col, "operand1_" ),
                _temp_df1,
                _temp_df2 );
        }

    if ( _col.has( "operand2_" ) )
        {
            build_temp_dfs(
                _df1,
                _df2,
                _rindices1,
                _rindices2,
                *JSON::get_object( _col, "operand2_" ),
                _temp_df1,
                _temp_df2 );
        }

    if ( _col.has( "condition_" ) )
        {
            build_temp_dfs(
                _df1,
                _df2,
                _rindices1,
                _rindices2,
                *JSON::get_object( _col, "condition_" ),
                _temp_df1,
                _temp_df2 );
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DataFrameJoiner::filter(
    const containers::DataFrame& _df1,
    const containers::DataFrame& _df2,
    const std::vector<size_t>& _rindices1,
    const std::vector<size_t>& _rindices2,
    const Poco::JSON::Object& _where,
    const std::shared_ptr<containers::Encoding>& _categories,
    const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
    containers::DataFrame* _temp_df )
{
    // ------------------------------------------------------------------------

    auto temp_df1 =
        containers::DataFrame( _df1.name(), _categories, _join_keys_encoding );

    auto temp_df2 =
        containers::DataFrame( _df2.name(), _categories, _join_keys_encoding );

    build_temp_dfs(
        _df1, _df2, _rindices1, _rindices2, _where, &temp_df1, &temp_df2 );

    // ------------------------------------------------------------------------

    const auto condition = BoolOpParser::parse(
        *_categories, *_join_keys_encoding, {temp_df1, temp_df2}, _where );

    // ------------------------------------------------------------------------

    _temp_df->where( condition );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

containers::DataFrame DataFrameJoiner::join(
    const std::string& _name,
    const containers::DataFrame& _df1,
    const containers::DataFrame& _df2,
    const Poco::JSON::Array& _cols1,
    const Poco::JSON::Array& _cols2,
    const std::string& _join_key_used,
    const std::string& _other_join_key_used,
    const std::string& _how,
    const std::optional<const Poco::JSON::Object>& _where,
    const std::shared_ptr<containers::Encoding>& _categories,
    const std::shared_ptr<containers::Encoding>& _join_keys_encoding )
{
    // ------------------------------------------------------------------------

    if ( _how == "right" )
        {
            return join(
                _name,
                _df2,
                _df1,
                _cols2,
                _cols1,
                _other_join_key_used,
                _join_key_used,
                "left",
                _where,
                _categories,
                _join_keys_encoding );
        }

    // ------------------------------------------------------------------------

    auto joined_df =
        containers::DataFrame( _name, _categories, _join_keys_encoding );

    // ------------------------------------------------------------------------

    size_t begin = 0;

    while ( begin < _df1.nrows() )
        {
            const auto [rindices1, rindices2] = make_row_indices(
                _df1,
                _df2,
                _join_key_used,
                _other_join_key_used,
                _how,
                &begin );

            assert_true( rindices1.size() == rindices2.size() );

            auto temp_df = containers::DataFrame(
                _name, _categories, _join_keys_encoding );

            if ( _cols1.size() > 0 )
                {
                    add_cols( _df1, rindices1, _cols1, &temp_df );
                }
            else
                {
                    add_all( _df1, rindices1, &temp_df );
                }

            if ( _cols2.size() > 0 )
                {
                    add_cols( _df2, rindices2, _cols2, &temp_df );
                }
            else
                {
                    add_all( _df2, rindices2, &temp_df );
                }

            if ( _where )
                {
                    filter(
                        _df1,
                        _df2,
                        rindices1,
                        rindices2,
                        *_where,
                        _categories,
                        _join_keys_encoding,
                        &temp_df );
                }

            if ( joined_df.nrows() > 0 )
                {
                    joined_df.append( temp_df );
                }
            else
                {
                    joined_df = std::move( temp_df );
                }
        }

    // ------------------------------------------------------------------------

    joined_df.create_indices();

    // ------------------------------------------------------------------------

    return joined_df;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::pair<const containers::Column<Int>, const containers::DataFrameIndex>
DataFrameJoiner::find_join_key(
    const containers::DataFrame& _df, const std::string& _name )
{
    for ( size_t i = 0; i < _df.num_join_keys(); ++i )
        {
            if ( _df.join_key( i ).name() == _name )
                {
                    return std::pair( _df.join_key( i ), _df.index( i ) );
                }
        }

    throw std::invalid_argument(
        "DataFrame '" + _df.name() + "' contains no join key named '" + _name +
        "'." );

    return std::pair( _df.join_key( 0 ), _df.index( 0 ) );
}

// ----------------------------------------------------------------------------

std::pair<std::vector<size_t>, std::vector<size_t>>
DataFrameJoiner::make_row_indices(
    const containers::DataFrame& _df1,
    const containers::DataFrame& _df2,
    const std::string& _join_key_used,
    const std::string& _other_join_key_used,
    const std::string& _how,
    size_t* _begin )
{
    const size_t batch_size = 100000;

    const auto [join_key1, index1] = find_join_key( _df1, _join_key_used );

    const auto [join_key2, index2] =
        find_join_key( _df2, _other_join_key_used );

    std::vector<size_t> rindices1, rindices2;

    for ( size_t& ix1 = *_begin; ix1 < _df1.nrows(); ++ix1 )
        {
            assert_true( rindices1.size() == rindices2.size() );

            if ( rindices1.size() >= batch_size )
                {
                    break;
                }

            const auto jk = join_key1[ix1];

            const auto it = index2.map()->find( jk );

            if ( it == index2.map()->end() )
                {
                    if ( _how == "left" )
                        {
                            rindices1.push_back( ix1 );
                            rindices2.push_back( _df2.nrows() );
                            continue;
                        }
                    else if ( _how == "inner" )
                        {
                            continue;
                        }
                    else
                        {
                            throw std::invalid_argument(
                                "Join '" + _how + "' not recognized." );
                        }
                }

            for ( const auto ix2 : it->second )
                {
                    rindices1.push_back( ix1 );
                    rindices2.push_back( ix2 );
                }
        }

    return std::make_pair( rindices1, rindices2 );
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine
