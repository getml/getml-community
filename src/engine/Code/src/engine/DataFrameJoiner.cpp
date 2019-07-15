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
            if ( _joined_df->has_categorical( _df.categorical( i ).name() ) )
                {
                    throw std::invalid_argument(
                        "Duplicate categorical column: '" +
                        _df.categorical( i ).name() + "'." );
                }

            _joined_df->add_int_column(
                _df.categorical( i ).sort_by_key( _rindices ), "categorical" );
        }

    for ( size_t i = 0; i < _df.num_discretes(); ++i )
        {
            if ( _joined_df->has_discrete( _df.discrete( i ).name() ) )
                {
                    throw std::invalid_argument(
                        "Duplicate discrete column: '" +
                        _df.discrete( i ).name() + "'." );
                }

            _joined_df->add_float_column(
                _df.discrete( i ).sort_by_key( _rindices ), "discrete" );
        }

    for ( size_t i = 0; i < _df.num_join_keys(); ++i )
        {
            if ( _joined_df->has_join_key( _df.join_key( i ).name() ) )
                {
                    throw std::invalid_argument(
                        "Duplicate join key: '" + _df.join_key( i ).name() +
                        "'." );
                }

            _joined_df->add_int_column(
                _df.join_key( i ).sort_by_key( _rindices ), "join_key" );
        }

    for ( size_t i = 0; i < _df.num_numericals(); ++i )
        {
            if ( _joined_df->has_numerical( _df.numerical( i ).name() ) )
                {
                    throw std::invalid_argument(
                        "Duplicate numerical column: '" +
                        _df.numerical( i ).name() + "'." );
                }

            _joined_df->add_float_column(
                _df.numerical( i ).sort_by_key( _rindices ), "numerical" );
        }

    for ( size_t i = 0; i < _df.num_targets(); ++i )
        {
            if ( _joined_df->has_target( _df.target( i ).name() ) )
                {
                    throw std::invalid_argument(
                        "Duplicate target column: '" + _df.target( i ).name() +
                        "'." );
                }

            _joined_df->add_float_column(
                _df.target( i ).sort_by_key( _rindices ), "target" );
        }

    for ( size_t i = 0; i < _df.num_time_stamps(); ++i )
        {
            if ( _joined_df->has_time_stamp( _df.time_stamp( i ).name() ) )
                {
                    throw std::invalid_argument(
                        "Duplicate time stamp column: '" +
                        _df.time_stamp( i ).name() + "'." );
                }

            _joined_df->add_float_column(
                _df.time_stamp( i ).sort_by_key( _rindices ), "time_stamp" );
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

            if ( role == "categorical" )
                {
                    if ( _joined_df->has_categorical( as ) )
                        {
                            throw std::invalid_argument(
                                "Duplicate categorical column: '" + name +
                                "'." );
                        }
                    auto col = _df.categorical( name ).sort_by_key( _rindices );
                    col.set_name( as );
                    _joined_df->add_int_column( col, role );
                }
            else if ( role == "discrete" )
                {
                    if ( _joined_df->has_discrete( as ) )
                        {
                            throw std::invalid_argument(
                                "Duplicate discrete column: '" + name + "'." );
                        }
                    auto col = _df.discrete( name ).sort_by_key( _rindices );
                    col.set_name( as );
                    _joined_df->add_float_column( col, role );
                }
            else if ( role == "join_key" )
                {
                    if ( _joined_df->has_join_key( as ) )
                        {
                            throw std::invalid_argument(
                                "Duplicate join key: '" + name + "'." );
                        }
                    auto col = _df.join_key( name ).sort_by_key( _rindices );
                    col.set_name( as );
                    _joined_df->add_int_column( col, role );
                }
            else if ( role == "numerical" )
                {
                    if ( _joined_df->has_numerical( as ) )
                        {
                            throw std::invalid_argument(
                                "Duplicate numerical: '" + name + "'." );
                        }
                    auto col = _df.numerical( name ).sort_by_key( _rindices );
                    col.set_name( as );
                    _joined_df->add_float_column( col, role );
                }
            else if ( role == "target" )
                {
                    if ( _joined_df->has_target( as ) )
                        {
                            throw std::invalid_argument(
                                "Duplicate target: '" + name + "'." );
                        }
                    auto col = _df.target( name ).sort_by_key( _rindices );
                    col.set_name( as );
                    _joined_df->add_float_column( col, role );
                }
            else if ( role == "time_stamp" )
                {
                    if ( _joined_df->has_time_stamp( as ) )
                        {
                            throw std::invalid_argument(
                                "Duplicate time_stamp: '" + name + "'." );
                        }
                    auto col = _df.time_stamp( name ).sort_by_key( _rindices );
                    col.set_name( as );
                    _joined_df->add_float_column( col, role );
                }
            else
                {
                    throw std::invalid_argument(
                        "Role '" + role + "' not recognized." );
                }
        }
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
                _categories,
                _join_keys_encoding );
        }

    // ------------------------------------------------------------------------

    auto joined_df = std::optional<containers::DataFrame>();

    // ------------------------------------------------------------------------

    if ( _df1.nrows() == 0 )
        {
            throw std::invalid_argument(
                "DataFrame '" + _df1.name() + "' contains no rows!" );
        }

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

            if ( joined_df )
                {
                    joined_df = std::make_optional<containers::DataFrame>(
                        std::move( temp_df ) );
                }
            else
                {
                    joined_df->append( temp_df );
                }
        }

    // ------------------------------------------------------------------------

    joined_df->create_indices();

    // ------------------------------------------------------------------------

    return *joined_df;

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
            assert( rindices1.size() == rindices2.size() );

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

    return std::pair( rindices1, rindices2 );
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine
