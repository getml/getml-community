#include "engine/pipelines/pipelines.hpp"

namespace engine
{
namespace pipelines
{
// ----------------------------------------------------------------------------

containers::DataFrame ManyToOneJoiner::find_peripheral(
    const std::string& _name,
    const std::vector<std::string>& _peripheral_names,
    const std::vector<containers::DataFrame>& _peripheral_dfs )
{
    if ( _peripheral_dfs.size() != _peripheral_names.size() )
        {
            throw std::invalid_argument(
                "The number of peripheral tables must match the number of "
                "placeholders passed. This is the point of having "
                "placeholders!" );
        }

    for ( size_t i = 0; i < _peripheral_names.size(); ++i )
        {
            if ( _peripheral_names.at( i ) == _name )
                {
                    return _peripheral_dfs.at( i );
                }
        }

    throw std::invalid_argument(
        "Could not find any placeholder named '" + _name +
        "' among the peripheral placeholders!" );

    return _peripheral_dfs.at( 0 );
}

// ----------------------------------------------------------------------------

std::string ManyToOneJoiner::get_param(
    const std::string& _splitted, const std::string& _key )
{
    const auto begin = _splitted.find( _key ) + _key.size();

    assert_true( begin != std::string::npos );

    const auto end = _splitted.find( containers::Macros::join_param(), begin );

    assert_true( end != std::string::npos );

    assert_true( end >= begin );

    return _splitted.substr( begin, end - begin );
}

// ----------------------------------------------------------------------------

containers::DataFrame ManyToOneJoiner::join_all(
    const bool _use_timestamps,
    const bool _is_population,
    const std::string& _joined_name,
    const std::vector<std::string>& _origin_peripheral_names,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs )
{
    const auto splitted = split_joined_name( _joined_name );

    assert_true( splitted.size() != 0 );

    auto population = _population_df;

    if ( !_is_population )
        {
            population = find_peripheral(
                splitted.at( 0 ), _origin_peripheral_names, _peripheral_dfs );
        }

    for ( size_t i = 1; i < splitted.size(); ++i )
        {
            population = join_one(
                _use_timestamps,
                splitted.at( i ),
                population,
                _peripheral_dfs,
                _origin_peripheral_names );
        }

    return population;
}

// ----------------------------------------------------------------------------

containers::DataFrame ManyToOneJoiner::join_one(
    const bool _use_timestamps,
    const std::string& _splitted,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const std::vector<std::string>& _peripheral_names )
{
    auto joined = _population;

    const auto
        [name,
         join_key,
         other_join_key,
         time_stamp,
         other_time_stamp,
         upper_time_stamp] = parse_splitted( _splitted );

    const auto peripheral =
        find_peripheral( name, _peripheral_names, _peripheral_dfs );

    const auto index = make_index(
        _use_timestamps,
        join_key,
        other_join_key,
        time_stamp,
        other_time_stamp,
        upper_time_stamp,
        _population,
        peripheral );

    for ( size_t i = 0; i < peripheral.num_categoricals(); ++i )
        {
            auto col = peripheral.categorical( i ).sort_by_key( index );
            col.set_name(
                containers::Macros::table() + "=" + name +
                containers::Macros::column() + "=" + col.name() );
            joined.add_int_column(
                col, containers::DataFrame::ROLE_CATEGORICAL );
        }

    for ( size_t i = 0; i < peripheral.num_join_keys(); ++i )
        {
            auto col = peripheral.join_key( i ).sort_by_key( index );
            col.set_name(
                containers::Macros::table() + "=" + name +
                containers::Macros::column() + "=" + col.name() );
            joined.add_int_column( col, containers::DataFrame::ROLE_JOIN_KEY );
        }

    for ( size_t i = 0; i < peripheral.num_numericals(); ++i )
        {
            auto col = peripheral.numerical( i ).sort_by_key( index );
            col.set_name(
                containers::Macros::table() + "=" + name +
                containers::Macros::column() + "=" + col.name() );
            joined.add_float_column(
                col, containers::DataFrame::ROLE_NUMERICAL );
        }

    for ( size_t i = 0; i < peripheral.num_time_stamps(); ++i )
        {
            auto col = peripheral.time_stamp( i ).sort_by_key( index );
            col.set_name(
                containers::Macros::table() + "=" + name +
                containers::Macros::column() + "=" + col.name() );
            joined.add_float_column(
                col, containers::DataFrame::ROLE_TIME_STAMP );
        }

    return joined;
}

// ----------------------------------------------------------------------------

void ManyToOneJoiner::join_tables(
    const bool _use_timestamps,
    const std::vector<std::string>& _origin_peripheral_names,
    const std::string& _joined_population_name,
    const std::vector<std::string>& _joined_peripheral_names,
    containers::DataFrame* _population_df,
    std::vector<containers::DataFrame>* _peripheral_dfs )
{
    const auto population_df = join_all(
        _use_timestamps,
        true,
        _joined_population_name,
        _origin_peripheral_names,
        *_population_df,
        *_peripheral_dfs );

    auto peripheral_dfs =
        std::vector<containers::DataFrame>( _joined_peripheral_names.size() );

    for ( size_t i = 0; i < peripheral_dfs.size(); ++i )
        {
            peripheral_dfs.at( i ) = join_all(
                _use_timestamps,
                false,
                _joined_peripheral_names.at( i ),
                _origin_peripheral_names,
                *_population_df,
                *_peripheral_dfs );
        }

    *_population_df = population_df;

    *_peripheral_dfs = peripheral_dfs;
}

// ----------------------------------------------------------------------------

std::vector<size_t> ManyToOneJoiner::make_index(
    const bool _use_timestamps,
    const std::string& _join_key,
    const std::string& _other_join_key,
    const std::string& _time_stamp,
    const std::string& _other_time_stamp,
    const std::string& _upper_time_stamp,
    const containers::DataFrame& _population,
    const containers::DataFrame& _peripheral )
{
    // -------------------------------------------------------

    const auto join_key = _population.join_key( _join_key );

    const auto peripheral_index = _peripheral.index( _other_join_key ).map();

    const auto time_stamp = extract_time_stamp( _population, _time_stamp );

    const auto other_time_stamp =
        extract_time_stamp( _peripheral, _other_time_stamp );

    const auto upper_time_stamp =
        extract_time_stamp( _peripheral, _upper_time_stamp );

    // -------------------------------------------------------

    if ( ( time_stamp && true ) != ( other_time_stamp && true ) )
        {
            throw std::invalid_argument(
                "If you pass a time stamp, there must also be another time "
                "stamp and vice versa!" );
        }

    // -------------------------------------------------------

    assert_true( peripheral_index );

    std::vector<size_t> index( _population.nrows() );

    for ( size_t i = 0; i < _population.nrows(); ++i )
        {
            const auto ts = time_stamp ? ( *time_stamp )[i] : 0.0;

            const auto [ix, ok] = retrieve_index(
                _population.nrows(),
                join_key[i],
                ts,
                *peripheral_index,
                other_time_stamp,
                upper_time_stamp );

            if ( !ok )
                {
                    throw std::invalid_argument(
                        "The join of '" + _population.name() + "' and '" +
                        _peripheral.name() +
                        "' was marked many-to-one, but there is more than one "
                        "match." );
                }

            index[i] = ix;
        }

    // -------------------------------------------------------

    return index;

    // -------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::tuple<
    std::string,
    std::string,
    std::string,
    std::string,
    std::string,
    std::string>
ManyToOneJoiner::parse_splitted( const std::string& _splitted )
{
    const auto name = _splitted.substr(
        0, _splitted.find( containers::Macros::join_param() ) );

    const auto join_key =
        get_param( _splitted, containers::Macros::join_key() + "=" );

    const auto other_join_key =
        get_param( _splitted, containers::Macros::other_join_key() + "=" );

    const auto time_stamp =
        get_param( _splitted, containers::Macros::time_stamp() + "=" );

    const auto other_time_stamp =
        get_param( _splitted, containers::Macros::other_time_stamp() + "=" );

    const auto upper_time_stamp =
        get_param( _splitted, containers::Macros::upper_time_stamp() + "=" );

    return std::make_tuple(
        name,
        join_key,
        other_join_key,
        time_stamp,
        other_time_stamp,
        upper_time_stamp );
}

// ----------------------------------------------------------------------------

std::pair<size_t, bool> ManyToOneJoiner::retrieve_index(
    const size_t _nrows,
    const Int _jk,
    const Float _ts,
    const containers::DataFrameIndex::MapType& _peripheral_index,
    const std::optional<containers::Column<Float>>& _other_time_stamp,
    const std::optional<containers::Column<Float>>& _upper_time_stamp )
{
    const auto it = _peripheral_index.find( _jk );

    if ( it == _peripheral_index.end() )
        {
            return std::make_pair( _nrows, true );
        }

    std::vector<size_t> local_indices;

    for ( size_t ix : it->second )
        {
            const auto lower =
                _other_time_stamp ? ( *_other_time_stamp )[ix] : 0.0;

            const auto upper =
                _upper_time_stamp ? ( *_upper_time_stamp )[ix] : NAN;

            const bool match_in_range =
                lower <= _ts && ( std::isnan( upper ) || upper > _ts );

            if ( match_in_range )
                {
                    local_indices.push_back( ix );
                }
        }

    if ( local_indices.size() == 0 )
        {
            return std::make_pair( _nrows, true );
        }

    if ( local_indices.size() == 1 )
        {
            return std::make_pair( local_indices[0], true );
        }

    return std::make_pair( _nrows, false );
}

// ----------------------------------------------------------------------------

std::vector<std::string> ManyToOneJoiner::split_joined_name(
    const std::string& _joined_name )
{
    auto splitted = std::vector<std::string>();

    auto joined_name = _joined_name;

    const auto delimiter = std::string( containers::Macros::name() + "=" );

    while ( true )
        {
            const auto pos = joined_name.find( delimiter );

            if ( pos == std::string::npos )
                {
                    splitted.push_back( joined_name );
                    break;
                }

            splitted.push_back( joined_name.substr( 0, pos ) );

            joined_name.erase( 0, pos + delimiter.size() );
        }

    if ( splitted.size() == 0 )
        {
            splitted.push_back( _joined_name );
        }

    return splitted;
}

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine
