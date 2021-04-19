#include "engine/hyperparam/hyperparam.hpp"

namespace engine
{
namespace hyperparam
{
// ------------------------------------------------------------------------

void Hyperopt::save(
    const std::string& _temp_dir,
    const std::string& _path,
    const std::string& _name ) const
{
    // ------------------------------------------------------------------

    auto tfile = Poco::TemporaryFile( _temp_dir );

    tfile.createDirectories();

    // ------------------------------------------------------------------

    std::ofstream fs( tfile.path() + "/obj.json", std::ofstream::out );

    Poco::JSON::Stringifier::stringify( obj(), fs );

    fs.close();

    // ------------------------------------------------------------------

    auto file = Poco::File( _path + _name );

    // Create all parent directories, if necessary.
    file.createDirectories();

    // If the actual folder already exists, delete it.
    file.remove( true );

    tfile.renameTo( file.path() );

    tfile.keep();

    // ------------------------------------------------------------------
}

// ------------------------------------------------------------------------

Poco::JSON::Object Hyperopt::to_monitor() const
{
    Poco::JSON::Object monitor;
    monitor.set( "cmd_", obj() );
    return monitor;
}

// ------------------------------------------------------------------------
}  // namespace hyperparam
}  // namespace engine
