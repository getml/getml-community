#include "engine/hyperparam/hyperparam.hpp"

namespace engine
{
namespace hyperparam
{
// ------------------------------------------------------------------------

void Hyperopt::save( const std::string& _path, const std::string& _name ) const
{
    // ------------------------------------------------------------------

    auto tfile = Poco::TemporaryFile();

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
}  // namespace hyperparam
}  // namespace engine
