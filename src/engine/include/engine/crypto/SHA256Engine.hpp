// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

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
