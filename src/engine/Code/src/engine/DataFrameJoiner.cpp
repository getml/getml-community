#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

containers::DataFrame DataFrameJoiner::join(
    const std::string& _name,
    const containers::DataFrame& _df1,
    const containers::DataFrame& _df2,
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
                _other_join_key_used,
                _join_key_used,
                "left",
                _categories,
                _join_keys_encoding );
        }

    // ------------------------------------------------------------------------

    const auto [rindices1, rindices2] = make_row_indices(
        _df1, _df2, _join_key_used, _other_join_key_used, _how );

    // ------------------------------------------------------------------------

    auto joined_df = containers::DataFrame( _categories, _join_keys_encoding );

    // ------------------------------------------------------------------------

    for ( size_t i = 0; i < _df1.num_categoricals(); ++i )
        joined_df.add_int_column(
            _df1.categorical( i ).sort_by_key( rindices1 ), "categorical" );

    for ( size_t i = 0; i < _df1.num_discretes(); ++i )
        joined_df.add_float_column(
            _df1.discrete( i ).sort_by_key( rindices1 ), "discrete" );

    for ( size_t i = 0; i < _df1.num_join_keys(); ++i )
        joined_df.add_int_column(
            _df1.join_key( i ).sort_by_key( rindices1 ), "join_key" );

    for ( size_t i = 0; i < _df1.num_numericals(); ++i )
        joined_df.add_float_column(
            _df1.numerical( i ).sort_by_key( rindices1 ), "numerical" );

    for ( size_t i = 0; i < _df1.num_targets(); ++i )
        joined_df.add_float_column(
            _df1.target( i ).sort_by_key( rindices1 ), "target" );

    for ( size_t i = 0; i < _df1.num_time_stamps(); ++i )
        joined_df.add_float_column(
            _df1.time_stamp( i ).sort_by_key( rindices1 ), "time_stamp" );

    // ------------------------------------------------------------------------

    for ( size_t i = 0; i < _df2.num_categoricals(); ++i )
        joined_df.add_int_column(
            _df2.categorical( i ).sort_by_key( rindices2 ), "categorical" );

    for ( size_t i = 0; i < _df1.num_discretes(); ++i )
        joined_df.add_float_column(
            _df2.discrete( i ).sort_by_key( rindices2 ), "discrete" );

    for ( size_t i = 0; i < _df2.num_join_keys(); ++i )
        joined_df.add_int_column(
            _df2.join_key( i ).sort_by_key( rindices2 ), "join_key" );

    for ( size_t i = 0; i < _df2.num_numericals(); ++i )
        joined_df.add_float_column(
            _df2.numerical( i ).sort_by_key( rindices2 ), "numerical" );

    for ( size_t i = 0; i < _df2.num_targets(); ++i )
        joined_df.add_float_column(
            _df2.target( i ).sort_by_key( rindices2 ), "target" );

    for ( size_t i = 0; i < _df2.num_time_stamps(); ++i )
        joined_df.add_float_column(
            _df2.time_stamp( i ).sort_by_key( rindices2 ), "time_stamp" );

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
    const std::string& _how )
{
    const auto [join_key1, index1] = find_join_key( _df1, _join_key_used );

    const auto [join_key2, index2] =
        find_join_key( _df2, _other_join_key_used );

    std::vector<size_t> rindices1, rindices2;

    for ( size_t ix1 = 0; ix1 < _df1.nrows(); ++ix1 )
        {
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
