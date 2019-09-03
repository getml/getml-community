#include "engine/licensing/licensing.hpp"

namespace engine
{
namespace licensing
{
// ------------------------------------------------------------------------

Token::Token(
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
                          std::to_string( cores_ ) + std::to_string( mem_ ) +
                          function_set_id_ + std::to_string( response_id_ ) +
                          msg_title_ + msg_body_ + request_date_ +
                          expiry_date_ ) )
{
}

// ------------------------------------------------------------------------

Token::Token() : Token( 0, "", "none", 0, "", "", "", 255 ) {}

// ------------------------------------------------------------------------

Token::Token( const Token& _other )
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

// ------------------------------------------------------------------------

Token::Token( const Poco::JSON::Object& _json_obj )
    : cores_( _json_obj.getValue<Int>( "cores_" ) ),
      expiry_date_( _json_obj.getValue<std::string>( "expiry_date_" ) ),
      function_set_id_( _json_obj.getValue<std::string>( "function_set_id_" ) ),
      mem_( _json_obj.getValue<Int>( "mem_" ) ),
      msg_body_( _json_obj.getValue<std::string>( "msg_body_" ) ),
      msg_title_( _json_obj.getValue<std::string>( "msg_title_" ) ),
      request_date_( _json_obj.getValue<std::string>( "request_date_" ) ),
      response_id_( _json_obj.getValue<Int>( "response_id_" ) ),
      signature_( _json_obj.getValue<std::string>( "signature_" ) )

{
}

// ------------------------------------------------------------------------

Poco::JSON::Object Token::to_json_obj() const
{
    Poco::JSON::Object obj;

    obj.set( "cores_", cores_ );

    obj.set( "expiry_date_", expiry_date_ );

    obj.set( "function_set_id_", function_set_id_ );

    obj.set( "mem_", mem_ );

    obj.set( "msg_body_", msg_body_ );

    obj.set( "msg_title_", msg_title_ );

    obj.set( "request_date_", request_date_ );

    obj.set( "response_id_", response_id_ );

    obj.set( "signature_", signature_ );

    return obj;
}

// ------------------------------------------------------------------------
}  // namespace licensing
}  // namespace engine
