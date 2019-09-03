#ifndef ENGINE_CRYPTO_SHA256_HPP_
#define ENGINE_CRYPTO_SHA256_HPP_

namespace engine
{
namespace crypto
{
// ------------------------------------------------------------------------

class SHA256
{
    // --------------------------------------------------------

   public:
    SHA256( const std::string _password ) : password_( _password ) {}

    ~SHA256() = default;

    // --------------------------------------------------------

    /// Uses the one-way SHA256 encryption algorithm.
    std::string encrypt( const std::string _msg ) const
    {
        Poco::HMACEngine<SHA256Engine> hmac( password_ );

        hmac.update( _msg );

        return Poco::DigestEngine::digestToHex( hmac.digest() );
    }

    // --------------------------------------------------------

   private:
    /// The password used for the encryption algorithm.
    const std::string password_;
};

// ------------------------------------------------------------------------
}  // namespace crypto
}  // namespace engine

#endif  // AUTOSQL_ENGINE_LICENSING_CRYPTO_SHA256_HPP_
