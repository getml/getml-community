#include "engine/monitoring/monitoring.hpp"

namespace engine
{
namespace monitoring
{
// ------------------------------------------------------------------------

std::shared_ptr<Poco::Net::StreamSocket> Monitor::connect() const
{
    // --------------------------------------------------------------

    const auto host_and_port =
        "127.0.0.1:" + std::to_string( options_.monitor().tcp_port() );

    const auto addr = Poco::Net::SocketAddress( host_and_port );

    const auto socket = std::make_shared<Poco::Net::StreamSocket>( addr );

    // --------------------------------------------------------------

    return socket;

    // --------------------------------------------------------------
}

// ------------------------------------------------------------------------

bool Monitor::get_start_message() const
{
    // --------------------------------------------------------------
    // Create HTTPRequest

    const std::string url =
        "127.0.0.1:" + std::to_string( options_.monitor().http_port() );

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

std::string Monitor::make_cmd(
    const std::string& _type, const Poco::JSON::Object& _body ) const
{
    auto cmd = Poco::JSON::Object();

    cmd.set( "type_", _type );

    cmd.set( "body_", _body );

    return JSON::stringify( cmd );
}

// ------------------------------------------------------------------------

std::pair<Poco::Net::HTTPResponse::HTTPStatus, std::string> Monitor::send(
    const std::string& _type, const std::string& _json ) const
{
    // --------------------------------------------------------------
    // Create HTTPRequest

    const std::string url =
        "127.0.0.1:" + std::to_string( options_.monitor().http_port() );

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
    Poco::Net::HTTPClientSession session(
        "127.0.0.1",
        static_cast<Poco::UInt16>( options_.monitor().http_port() ) );

    const auto one_year = Poco::Timespan( 365, 0, 0, 0, 0 );

    session.setTimeout( one_year );

    auto& req_stream = session.sendRequest( *_req );

    req_stream << _json;

    std::istream& rs = session.receiveResponse( *_res );

    Poco::StreamCopier::copyToString( rs, *_response_content );
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
            Poco::Net::HTTPClientSession session(
                "127.0.0.1",
                static_cast<Poco::UInt16>( options_.monitor().http_port() ) );

            session.sendRequest( req );
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

void Monitor::shutdown_when_monitor_dies( const Monitor _monitor )
{
    std::int32_t num_failed = 0;

    while ( true )
        {
            std::this_thread::sleep_for( std::chrono::seconds( 3 ) );

            const auto [status, res] = _monitor.send( "isalive", "" );

            if ( status == Poco::Net::HTTPResponse::HTTP_OK && res == "yes" )
                {
                    num_failed = 0;
                }
            else
                {
                    ++num_failed;

                    if ( num_failed > 2 )
                        {
                            std::cout << "Monitor seems to have to died. "
                                         "Shutting down..."
                                      << std::endl;
                            std::exit( 0 );
                        }
                }
        }
}

// ------------------------------------------------------------------------
}  // namespace monitoring
}  // namespace engine
