#include "engine/licensing/licensing.hpp"

namespace engine
{
namespace licensing
{
// ------------------------------------------------------------------------

ULong LicenseChecker::calc_mem_size(
    const std::map<std::string, containers::DataFrame>& _data_frames ) const
{
    ULong mem_size = 0;

    for ( const auto& [name, df] : _data_frames )
        {
            mem_size += df.nbytes();
        }

    return mem_size;
}

// ------------------------------------------------------------------------

void LicenseChecker::check_mem_size(
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const ULong _new_df_size ) const
{
    const auto msize = token().mem_;

    if ( msize <= 0 )
        {
            return;
        }

    const auto allowed_mem_size = static_cast<ULong>( msize ) * 1000000;

    if ( calc_mem_size( _data_frames ) + _new_df_size > allowed_mem_size )
        {
            throw std::runtime_error(
                "Adding this data frame would result in "
                "memory usage that exceeds the memory limit "
                "of " +
                std::to_string( msize ) +
                " MB. Please "
                "upgrade to the getML enterprise version." );
        }
}

// ------------------------------------------------------------------------

void LicenseChecker::receive_token( const std::string& _caller_id )
{
    // -------------------------------------------------------------

    Poco::JSON::Parser parser;

    logger_->log( "Attempting to receive a token..." );

    // -------------------------------------------------------------
    // Make request

    const std::string request = "{\"caller_id_\":\"" + _caller_id +
                                "\",\"product_id_\":\"getml-0.8-beta-linux\"}";

    // -------------------------------------------------------------
    // Get token

    auto [response, success] = send( request );

    while ( !success )
        {
            std::this_thread::sleep_for( std::chrono::seconds( 1 ) );
            std::tie( response, success ) = send( request );
        }

    // --------------------------------------------------------------
    // Initialize a write lock.

    multithreading::WriteLock write_lock( read_write_lock_ );

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

    if ( Token( *token_ ).signature_ != token_->signature_ )
        {
            logger_->log(
                "Verification of the token failed. It appears that "
                "someone is attempting a 'man-in-the-middle' attack!" );

            token_.reset( new Token() );

            return;
        }

    // --------------------------------------------------------------
    // Print message, if necessary

    if ( token_->msg_title_ != "" || token_->msg_body_ != "" )
        {
            logger_->log( token_->msg_title_ + ": " + token_->msg_body_ );
        }

    // --------------------------------------------------------------
}

// ------------------------------------------------------------------------

std::pair<std::string, bool> LicenseChecker::send( const std::string& _request )
{
    // --------------------------------------------------------------
    // Send request

    const auto [status, response] = monitor_->send( "gettoken", _request );

    // --------------------------------------------------------------

    if ( status != Poco::Net::HTTPResponse::HTTPStatus::HTTP_OK )
        {
            return std::make_pair( response, false );
        }

    // --------------------------------------------------------------

    if ( response.size() == 0 || response[0] != '{' )
        {
            return std::make_pair( response, false );
        }

    // --------------------------------------------------------------

    return std::make_pair( response, true );

    // --------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace licensing
}  // namespace engine
