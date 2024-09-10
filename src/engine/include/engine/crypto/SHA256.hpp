// Copyright 2024 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef ENGINE_CRYPTO_SHA256_HPP_
#define ENGINE_CRYPTO_SHA256_HPP_

// ------------------------------------------------------------------------

#if (defined(_WIN32) || defined(_WIN64))
#define _WINSOCKAPI_  // stops windows.h including winsock.h
#include <windows.h>
#include <winsock2.h>
// --------------------------------------------------------
#include <openssl/ossl_typ.h>
#endif

// ------------------------------------------------------------------------

#include <Poco/Crypto/DigestEngine.h>
#include <Poco/HMACEngine.h>

// ------------------------------------------------------------------------

#include <string>

// ------------------------------------------------------------------------

#include "engine/crypto/SHA256Engine.hpp"

// ------------------------------------------------------------------------

namespace engine {
namespace crypto {

class SHA256 {
 public:
  SHA256(const std::string _password) : password_(_password) {}

  ~SHA256() = default;

  // --------------------------------------------------------

  /// Uses the one-way SHA256 encryption algorithm.
  std::string encrypt(const std::string _msg) const {
    Poco::HMACEngine<SHA256Engine> hmac(password_);

    hmac.update(_msg);

    return Poco::DigestEngine::digestToHex(hmac.digest());
  }

 private:
  /// The password used for the encryption algorithm.
  const std::string password_;
};

// ------------------------------------------------------------------------
}  // namespace crypto
}  // namespace engine

#endif  // MULTIREL_ENGINE_LICENSING_CRYPTO_SHA256_HPP_
