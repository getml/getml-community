#include "engine/engine.hpp"

namespace autosql
{
namespace engine
{
namespace licensing
{
// ------------------------------------------------------------------------

void LicenseChecker::check_memory_size(
    std::map<std::string, containers::DataFrame>& _data_frames,
    containers::DataFrame& _most_recent_data_frame )
{
    assert( token_ );

    if ( token_->mem_size_ == 0.0 )
        {
            return;
        }

    auto memory_size =
        _most_recent_data_frame.nbytes() +
        std::accumulate(
            _data_frames.begin(),
            _data_frames.end(),
            static_cast<SQLNET_UNSIGNED_LONG>( 0 ),
            []( const SQLNET_UNSIGNED_LONG& init,
                std::pair<std::string, containers::DataFrame> df ) {
                return init + df.second.nbytes();
            } );

    if ( memory_size > token_->mem_size_ )
        {
            throw std::runtime_error(
                "Memory size limit of " + std::to_string( token_->mem_size_ ) +
                " was exceeded (" + std::to_string( memory_size ) +
                " bytes)! The batch of data that was most recently uploaded "
                "has been removed!" );
        }
}

// ------------------------------------------------------------------------

void LicenseChecker::receive_token()
{
    // -------------------------------------------------------------

    Poco::JSON::Parser parser;

    // -------------------------------------------------------------
    // Get token

    std::string response;

    const bool success = send( &response );

    if ( !success )
        {
            return;
        }

    // --------------------------------------------------------------
    // Initialize a write lock.

    autosql::multithreading::WriteLock write_lock( read_write_lock_ );

    // --------------------------------------------------------------
    // Parse JSON token

    try
        {
            auto json_obj =
                parser.parse( response ).extract<Poco::JSON::Object::Ptr>();

            token_.reset( new Token( *json_obj ) );
        }
    catch ( std::exception& e )
        {
            logger_->log( std::string( "Error: " ) + e.what() );

            token_.reset( new Token() );

            return;
        }

    // -------------------------------------------------------------
    // Verify token

    if ( Token( *token_ ).verification_ != token_->verification_ )
        {
            logger_->log(
                "Verification of the token failed. It appears that "
                "someone is attempting a 'man-in-the-middle' attack!" );

            token_.reset( new Token() );

            return;
        }

    // --------------------------------------------------------------
    // Print message, if necessary

    if ( token_->message_ != "" )
        {
            logger_->log( token_->message_ );
        }

    // --------------------------------------------------------------
}

// ------------------------------------------------------------------------

bool LicenseChecker::send( std::string* _response )
{
    // --------------------------------------------------------------
    // Send request

    *_response = monitor_->send( "gettoken", "" );

    // --------------------------------------------------------------

    if ( _response->size() == 0 || ( *_response )[0] != '{' )
        {
            return false;
        }

    // --------------------------------------------------------------

    return true;

    // --------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace licensing
}  // namespace engine
}  // namespace autosql
