#include "engine/monitoring/monitoring.hpp"

namespace engine
{
namespace monitoring
{
// ------------------------------------------------------------------------

bool Monitor::get_start_message() const
{
    // --------------------------------------------------------------
    // Create HTTPRequest

    const std::string url =
        "127.0.0.1:" + std::to_string( options_.monitor_.port_ );

    const std::string path = "/getstartmessage/";

    Poco::Net::HTTPRequest req(
        Poco::Net::HTTPRequest::HTTP_POST,
        path,
        Poco::Net::HTTPMessage::HTTP_1_1 );

    // --------------------------------------------------------------
    // Send HTTPRequest and receive response

    Poco::Net::HTTPResponse res;

    std::string response_content;

    size_t attempts = 0;

    while ( true )
        {
            try
                {
                    send_and_receive( "", &req, &res, &response_content );

                    break;
                }
            catch ( std::exception& e )
                {
                    if ( attempts++ < 21 )
                        {
                            std::this_thread::sleep_for(
                                std::chrono::milliseconds( 10 ) );
                        }
                    else
                        {
                            log( "getML has not started." );

                            return false;
                        }
                }
        }

    // --------------------------------------------------------------

    log( response_content );

    // --------------------------------------------------------------

    return true;

    // --------------------------------------------------------------
}

// ------------------------------------------------------------------------

std::pair<Poco::Net::HTTPResponse::HTTPStatus, std::string> Monitor::send(
    const std::string& _type, const std::string& _json ) const
{
    // --------------------------------------------------------------
    // Create HTTPRequest

    const std::string url =
        "127.0.0.1:" + std::to_string( options_.monitor_.port_ );

    const std::string path = "/" + _type + "/";

    Poco::Net::HTTPRequest req(
        Poco::Net::HTTPRequest::HTTP_POST,
        path,
        Poco::Net::HTTPMessage::HTTP_1_1 );

    req.setContentType( _type );

    req.setContentLength( _json.length() );

    // --------------------------------------------------------------
    // Send HTTPRequest and receive response

    Poco::Net::HTTPResponse res;

    std::string response_content;

    try
        {
            send_and_receive( _json, &req, &res, &response_content );
        }
    catch ( std::exception& e )
        {
            log( "Communication with getML monitor failed: " +
                 std::string( e.what() ) + "." );

            return std::pair<Poco::Net::HTTPResponse::HTTPStatus, std::string>(
                Poco::Net::HTTPResponse::HTTPStatus::HTTP_MOVED_PERMANENTLY,
                "" );
        }

    // --------------------------------------------------------------
    // Print HTTPRequest as status message, if not successful

    if ( res.getStatus() != Poco::Net::HTTPResponse::HTTPStatus::HTTP_OK )
        {
            std::stringstream stream;

            req.write( stream );

            stream << res.getStatus() << " " << res.getReason() << std::endl;

            log( stream.str() );
        }

    // --------------------------------------------------------------

    return std::pair<Poco::Net::HTTPResponse::HTTPStatus, std::string>(
        res.getStatus(), response_content );

    // --------------------------------------------------------------
}

// ------------------------------------------------------------------------

void Monitor::send_and_receive(
    const std::string& _json,
    Poco::Net::HTTPRequest* _req,
    Poco::Net::HTTPResponse* _res,
    std::string* _response_content ) const
{
    if ( options_.monitor_.tls_encryption_ )
        {
            const Poco::Net::Context::Ptr context( new Poco::Net::Context(
                Poco::Net::Context::Usage::CLIENT_USE,
                "",
                Poco::Net::Context::VerificationMode::VERIFY_NONE ) );

            Poco::Net::HTTPSClientSession session(
                "127.0.0.1",
                static_cast<Poco::UInt16>( options_.monitor_.port_ ),
                context );

            const auto one_year = Poco::Timespan( 365, 0, 0, 0, 0 );

            session.setTimeout( one_year );

            auto& req_stream = session.sendRequest( *_req );

            req_stream << _json;

            std::istream& rs = session.receiveResponse( *_res );

            Poco::StreamCopier::copyToString( rs, *_response_content );
        }
    else
        {
            Poco::Net::HTTPClientSession session(
                "127.0.0.1",
                static_cast<Poco::UInt16>( options_.monitor_.port_ ) );

            const auto one_year = Poco::Timespan( 365, 0, 0, 0, 0 );

            session.setTimeout( one_year );

            auto& req_stream = session.sendRequest( *_req );

            req_stream << _json;

            std::istream& rs = session.receiveResponse( *_res );

            Poco::StreamCopier::copyToString( rs, *_response_content );
        }
}

// ------------------------------------------------------------------------

bool Monitor::shutdown() const
{
    // --------------------------------------------------------------
    // Create HTTPRequest

    const std::string path = "/exit/";

    Poco::Net::HTTPRequest req(
        Poco::Net::HTTPRequest::HTTP_GET,
        path,
        Poco::Net::HTTPMessage::HTTP_1_1 );

    // --------------------------------------------------------------
    // Send HTTPRequest

    try
        {
            if ( options_.monitor_.tls_encryption_ )
                {
                    const Poco::Net::Context::Ptr context(
                        new Poco::Net::Context(
                            Poco::Net::Context::Usage::CLIENT_USE,
                            "",
                            Poco::Net::Context::VerificationMode::
                                VERIFY_NONE ) );

                    Poco::Net::HTTPSClientSession session(
                        "127.0.0.1",
                        static_cast<Poco::UInt16>( options_.monitor_.port_ ),
                        context );

                    session.sendRequest( req );
                }
            else
                {
                    Poco::Net::HTTPClientSession session(
                        "127.0.0.1",
                        static_cast<Poco::UInt16>( options_.monitor_.port_ ) );

                    session.sendRequest( req );
                }
        }
    catch ( std::exception& e )
        {
            log( "Communication with getML monitor failed: " +
                 std::string( e.what() ) + "." );

            return false;
        }

    // --------------------------------------------------------------

    return true;

    // --------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace monitoring
}  // namespace engine
