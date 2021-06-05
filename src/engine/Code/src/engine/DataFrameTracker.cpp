#include "engine/dependency/dependency.hpp"

namespace engine
{
namespace dependency
{
// -------------------------------------------------------------------------

void DataFrameTracker::add( const containers::DataFrame& _df )
{
    clean_up();

    const auto build_history = _df.build_history();

    assert_true( build_history );

    const auto b_str = JSON::stringify( *build_history );

    const auto b_hash = std::hash<std::string>()( b_str );

    const auto df_pair = std::make_pair( _df.name(), _df.last_change() );

    pairs_.insert_or_assign( b_hash, df_pair );
}

// -------------------------------------------------------------------------

void DataFrameTracker::clean_up()
{
    std::vector<size_t> remove_keys;

    for ( const auto [key, _] : pairs_ )
        {
            if ( !get_df( key ) )
                {
                    remove_keys.push_back( key );
                }
        }

    for ( const auto key : remove_keys )
        {
            pairs_.erase( key );
        }
}

// -------------------------------------------------------------------------

void DataFrameTracker::clear()
{
    pairs_ = std::map<size_t, std::pair<std::string, std::string>>();
}

// ----------------------------------------------------------------------

// TODO: Use fingerprint, because it retrieves the build history if there is
// one.
std::optional<containers::DataFrame> DataFrameTracker::get_df(
    const size_t _b_hash ) const
{
    const auto it = pairs_.find( _b_hash );

    if ( it == pairs_.end() )
        {
            return std::nullopt;
        }

    const auto name = it->second.first;

    const auto last_change = it->second.second;

    const auto [df, exists] =
        utils::Getter::get_if_exists( name, data_frames_.get() );

    if ( !exists )
        {
            return std::nullopt;
        }

    if ( df->last_change() != last_change )
        {
            return std::nullopt;
        }

    return *df;
}

// ----------------------------------------------------------------------

Poco::JSON::Object::Ptr DataFrameTracker::make_build_history(
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies,
    const std::vector<Poco::JSON::Object::Ptr>& _df_fingerprints ) const
{
    auto dependencies = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    for ( auto ptr : _dependencies )
        {
            dependencies->add( ptr );
        }

    auto df_fingerprints = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    for ( auto ptr : _df_fingerprints )
        {
            df_fingerprints->add( ptr );
        }

    auto build_history = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    build_history->set( "dependencies_", dependencies );

    build_history->set( "df_fingerprints_", df_fingerprints );

    return build_history;
}

// -------------------------------------------------------------------------

std::optional<containers::DataFrame> DataFrameTracker::retrieve(
    const Poco::JSON::Object::Ptr _build_history ) const
{
    assert_true( _build_history );

    const auto b_str = JSON::stringify( *_build_history );

    const auto b_hash = std::hash<std::string>()( b_str );

    return get_df( b_hash );
}

// -------------------------------------------------------------------------
}  // namespace dependency
}  // namespace engine
