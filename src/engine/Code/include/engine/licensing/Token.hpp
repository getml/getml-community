#ifndef MULTIREL_ENGINE_LICENSING_TOKEN_HPP_
#define MULTIREL_ENGINE_LICENSING_TOKEN_HPP_

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
        const Int _response_id );

    Token();

    Token( const Token& _other );

    Token( const Poco::JSON::Object& _json_obj );

    ~Token() = default;

    // --------------------------------

    /// Whether the token is currently active.
    bool currently_active() const { return function_set_id_ != "none"; }

    /// Expresses the Token in JSON format
    std::string to_json() const { return JSON::stringify( to_json_obj() ); }

    // --------------------------------

    /// Turns the Token into a JSON object
    Poco::JSON::Object to_json_obj() const;

    // --------------------------------

    /// Maximum number of cores allowed.
    const Int cores_;

    /// Date and time at which the token expires.
    const std::string expiry_date_;

    /// The functions that are allowed.
    /// Possible values are "basic", "enterprise" and "none".
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

#endif  // MULTIREL_ENGINE_LICENSING_TOKEN_HPP_
