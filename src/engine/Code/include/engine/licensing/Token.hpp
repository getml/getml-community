#ifndef AUTOSQL_ENGINE_LICENSING_TOKEN_HPP_
#define AUTOSQL_ENGINE_LICENSING_TOKEN_HPP_

namespace engine
{
namespace licensing
{
// ------------------------------------------------------------------------

struct Token
{
    Token(
        const Int _cores,
        const std::string& _expiry_date,
        const std::string& _function_set_id,
        const Int _mem,
        const std::string& _msg_body,
        const std::string& _msg_title,
        const std::string& _request_date,
        const Int _response_id )
        : cores_( _cores ),
          expiry_date_( _expiry_date ),
          function_set_id_( _function_set_id ),
          mem_( _mem ),
          msg_body_( _msg_body ),
          msg_title_( _msg_title ),
          request_date_( _request_date ),
          response_id_( _response_id ),
          signature_( crypto::SHA256( "AsgharGhorbaniIsVerySexy!" )
                          .encrypt(
                              std::to_string( cores_ ) +
                              std::to_string( mem_ ) + function_set_id_ +
                              std::to_string( response_id_ ) + msg_title_ +
                              msg_body_ + request_date_ + expiry_date_ ) )
    {
    }

    Token() : Token( 0, "", "none", 0, "", "", "", 255 ) {}

    Token( const Token& _other )
        : Token(
              _other.cores_,
              _other.expiry_date_,
              _other.function_set_id_,
              _other.mem_,
              _other.msg_body_,
              _other.msg_title_,
              _other.request_date_,
              _other.response_id_ )
    {
    }

    Token( const Poco::JSON::Object& _json_obj )
        : cores_( _json_obj.getValue<Int>( "cores_" ) ),
          expiry_date_( _json_obj.getValue<std::string>( "expiry_date_" ) ),
          function_set_id_(
              _json_obj.getValue<std::string>( "function_set_id_" ) ),
          mem_( _json_obj.getValue<Int>( "mem_" ) ),
          msg_body_( _json_obj.getValue<std::string>( "msg_body_" ) ),
          msg_title_( _json_obj.getValue<std::string>( "msg_title_" ) ),
          request_date_( _json_obj.getValue<std::string>( "request_date_" ) ),
          response_id_( _json_obj.getValue<Int>( "response_id_" ) ),
          signature_( _json_obj.getValue<std::string>( "signature_" ) )

    {
    }

    ~Token() = default;

    // --------------------------------

    /// Whether the token is currently active.
    bool currently_active() const { return function_set_id_ != "none"; }

    /// Expresses the Token in JSON format
    std::string to_json() const { return JSON::stringify( to_json_obj() ); }

    // --------------------------------

    /// Turns the Token into a JSON object
    Poco::JSON::Object to_json_obj() const
    {
        Poco::JSON::Object obj;

        /*obj.set( "currently_active_", currently_active_ );

        obj.set( "expiry_date_", expiry_date_ );

        obj.set( "mem_size_", mem_size_ );

        obj.set( "message_", message_ );

        obj.set( "request_date_", request_date_ );

         obj.set( "verification_", verification_ );*/

        return obj;
    }

    // --------------------------------

    /// Maximum number of cores allowed.
    const Int cores_;

    /// Date and time at which the token expires.
    const std::string expiry_date_;

    /// The functions that are allowed.
    const std::string function_set_id_;

    /// Maximum memory usage allowed, in MB.
    const Int mem_;

    /// The body of the message from the license server.
    const std::string msg_body_;

    /// Title of the message from the license server.
    const std::string msg_title_;

    /// Date and time at which the request was sent.
    const std::string request_date_;

    /// ID of the response.
    const Int response_id_;

    /// Signature used to ensure that the token actually originated
    /// from the license server.
    const std::string signature_;
};

// ------------------------------------------------------------------------
}  // namespace licensing
}  // namespace engine

#endif  // AUTOSQL_ENGINE_LICENSING_TOKEN_HPP_
