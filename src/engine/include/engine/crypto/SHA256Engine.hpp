#ifndef ENGINE_CRYPTO_SHA256ENGINE_HPP_
#define ENGINE_CRYPTO_SHA256ENGINE_HPP_

#include <Poco/Crypto/DigestEngine.h>

namespace engine {
namespace crypto {
// ------------------------------------------------------------------------

class SHA256Engine : public Poco::Crypto::DigestEngine {
 public:
  enum { BLOCK_SIZE = 64, DIGEST_SIZE = 32 };

  SHA256Engine() : Poco::Crypto::DigestEngine("SHA256") {}
};

// ------------------------------------------------------------------------
}  // namespace crypto
}  // namespace engine

#endif  // ENGINE_CRYPTO_SHA256ENGINE_HPP_
