#ifndef ENGINE_MONITORING_MONITORING_HPP_
#define ENGINE_MONITORING_MONITORING_HPP_

// ----------------------------------------------------
// Dependencies

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

#include "engine/config/config.hpp"

#include "logging/logging.hpp"

// ----------------------------------------------------
// Module files

#include "engine/monitoring/Monitor.hpp"

#include "engine/monitoring/Logger.hpp"

// ----------------------------------------------------

#endif  // ENGINE_MONITORING_MONITORING_HPP_
