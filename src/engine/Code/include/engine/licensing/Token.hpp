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
        const bool _currently_active,
        const Int _expires_in,
        const std::string& _function_set_id,
        const Int _mem,
        const std::string& _msg_body,
        const std::string& _msg_title,
        const std::string& _request_date );

    Token();

    Token( const Token& _other );

    Token( const Poco::JSON::Object& _json_obj );

    ~Token() = default;

    // --------------------------------

    /// Whether the token is currently active.
    bool currently_active() const { return currently_active_; }

    /// Expresses the Token in JSON format
    std::string to_json() const { return JSON::stringify( to_json_obj() ); }

    // --------------------------------

    /// Turns the Token into a JSON object
    Poco::JSON::Object to_json_obj() const;

    // --------------------------------

    /// Maximum number of cores allowed.
    const Int cores_;

    /// Whether the token is currently active
    const bool currently_active_;

    /// Number of seconds until token expires.
    const Int expires_in_;

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

    /// Signature used to ensure that the token actually originated
    /// from the license server.
    const std::string signature_;
};

// ------------------------------------------------------------------------
}  // namespace licensing
}  // namespace engine

#endif  // MULTIREL_ENGINE_LICENSING_TOKEN_HPP_
