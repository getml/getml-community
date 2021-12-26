#include "engine/licensing/Token.hpp"

// ------------------------------------------------------------------------

#include "engine/crypto/crypto.hpp"

// ------------------------------------------------------------------------

namespace engine {
namespace licensing {

Token::Token(const Int _cores, const bool _currently_active,
             const Int _expires_in, const std::string& _function_set_id,
             const Int _mem, const std::string& _msg_body,
             const std::string& _msg_title, const std::string& _request_date)
    : cores_(_cores),
      currently_active_(_currently_active),
      expires_in_(_expires_in),
      function_set_id_(_function_set_id),
      mem_(_mem),
      msg_body_(_msg_body),
      msg_title_(_msg_title),
      request_date_(_request_date),
      signature_(crypto::SHA256("AsgharGhorbaniIsVerySexy!")
                     .encrypt(std::to_string(cores_) +
                              (currently_active_ ? "true" : "false") +
                              std::to_string(expires_in_) + function_set_id_ +
                              std::to_string(mem_) + msg_body_ + msg_title_ +
                              request_date_)) {}

// ------------------------------------------------------------------------

Token::Token() : Token(0, false, 0, "none", 0, "", "", "") {}

// ------------------------------------------------------------------------

Token::Token(const Token& _other)
    : Token(_other.cores_, _other.currently_active_, _other.expires_in_,
            _other.function_set_id_, _other.mem_, _other.msg_body_,
            _other.msg_title_, _other.request_date_) {}

// ------------------------------------------------------------------------

Token::Token(const Poco::JSON::Object& _json_obj)
    : cores_(_json_obj.getValue<Int>("cores_")),
      currently_active_(_json_obj.getValue<bool>("currently_active_")),
      expires_in_(_json_obj.getValue<Int>("expires_in_")),
      function_set_id_(_json_obj.getValue<std::string>("function_set_id_")),
      mem_(_json_obj.getValue<Int>("mem_")),
      msg_body_(_json_obj.getValue<std::string>("msg_body_")),
      msg_title_(_json_obj.getValue<std::string>("msg_title_")),
      request_date_(_json_obj.getValue<std::string>("request_date_")),
      signature_(_json_obj.getValue<std::string>("signature_"))

{}

// ------------------------------------------------------------------------

Poco::JSON::Object Token::to_json_obj() const {
  Poco::JSON::Object obj;

  obj.set("cores_", cores_);

  obj.set("currently_active_", currently_active_);

  obj.set("expires_in_", expires_in_);

  obj.set("function_set_id_", function_set_id_);

  obj.set("mem_", mem_);

  obj.set("msg_body_", msg_body_);

  obj.set("msg_title_", msg_title_);

  obj.set("request_date_", request_date_);

  obj.set("signature_", signature_);

  return obj;
}

// ------------------------------------------------------------------------
}  // namespace licensing
}  // namespace engine
