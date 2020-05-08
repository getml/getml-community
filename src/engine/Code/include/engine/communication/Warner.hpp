#ifndef ENGINE_COMMUNICATION_WARNER_HPP_
#define ENGINE_COMMUNICATION_WARNER_HPP_

namespace engine
{
namespace communication
{
// ------------------------------------------------------------------------

class Warner
{
    // ------------------------------------------------------------------------

   public:
    Warner() {}

    ~Warner() = default;

    // ------------------------------------------------------------------------

   public:
    /// Adds a new warning to the list of warnings.
    void add( const std::string& _warning ) { warnings_.push_back( _warning ); }

    /// Sends all warnings to the socket.
    void send( Poco::Net::StreamSocket* _socket ) const
    {
        Poco::JSON::Object obj;
        obj.set( "warnings_", JSON::vector_to_array_ptr( warnings_ ) );
        const auto json_str = JSON::stringify( obj );
        Sender::send_string( json_str, _socket );
    }

    /// Trivial (const) getter
    const std::vector<std::string>& warnings() { return warnings_; }

    // ------------------------------------------------------------------------

   private:
    /// The list of warnings to send.
    std::vector<std::string> warnings_;

    // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine

#endif  // ENGINE_COMMUNICATION_WARNER_HPP_
