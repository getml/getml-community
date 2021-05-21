#ifndef ENGINE_CRYPTO_CRYPTO_HPP_
#define ENGINE_CRYPTO_CRYPTO_HPP_

#if ( defined( _WIN32 ) || defined( _WIN64 ) )
#define _WINSOCKAPI_  // stops windows.h including winsock.h
#include <windows.h>
#include <winsock2.h>

#include <openssl/ossl_typ.h>
#endif
// --------------------------------------------------------

#include <Poco/Crypto/DigestEngine.h>
#include <Poco/HMACEngine.h>

// --------------------------------------------------------

#include "engine/crypto/SHA256Engine.hpp"

#include "engine/crypto/SHA256.hpp"

// --------------------------------------------------------

#endif  // MULTIREL_ENGINE_LICENSING_CRYPTO_CRYPTO_HPP_
