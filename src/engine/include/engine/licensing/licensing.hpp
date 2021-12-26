#ifndef MULTIREL_ENGINE_LICENSING_HPP_
#define MULTIREL_ENGINE_LICENSING_HPP_

// ----------------------------------------------------------------------------

#if (defined(_WIN32) || defined(_WIN64))
#define _WINSOCKAPI_  // stops windows.h including winsock.h
#include <windows.h>
#include <winsock2.h>

// ----------------------------------------------------------------------------
#include <openssl/ossl_typ.h>
#endif

#include <Poco/Crypto/RSACipherImpl.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>

#include <chrono>
#include <sstream>
#include <string>
#include <thread>

#include "engine/Int.hpp"
#include "engine/ULong.hpp"
#include "engine/communication/communication.hpp"
#include "engine/config/config.hpp"
#include "engine/containers/containers.hpp"
#include "multithreading/multithreading.hpp"

// ----------------------------------------------------------------------------

#include "engine/crypto/crypto.hpp"
#include "engine/licensing/LicenseChecker.hpp"
#include "engine/licensing/Token.hpp"

// ----------------------------------------------------------------------------

#endif  // MULTIREL_ENGINE _LICENSING_HPP_
