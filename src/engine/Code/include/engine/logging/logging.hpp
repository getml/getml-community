#ifndef ENGINE_LOGGING_LOGGING_HPP_
#define ENGINE_LOGGING_LOGGING_HPP_

// ----------------------------------------------------
// Dependencies

#include <chrono>
#include <thread>
#include <vector>

#include <Poco/Crypto/DigestEngine.h>
#include <Poco/File.h>
#include <Poco/HMACEngine.h>
#include <Poco/MD5Engine.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Path.h>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPSClientSession.h>

// #include "config/config.hpp"

// ----------------------------------------------------
// Module files

// #include "logging/Monitor.hpp"

#include "engine/logging/Logger.hpp"

// ----------------------------------------------------

#endif  // ENGINE_LOGGING_LOGGING_HPP_
