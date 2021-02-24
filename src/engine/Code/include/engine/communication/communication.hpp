#ifndef ENGINE_COMMUNICATION_COMMUNICATION_HPP_
#define ENGINE_COMMUNICATION_COMMUNICATION_HPP_

// ----------------------------------------------------
// Dependencies

#if ( defined( _WIN32 ) || defined( _WIN64 ) )
#define _WINSOCKAPI_    // stops windows.h including winsock.h
#endif

#if ( defined( _WIN32 ) || defined( _WIN64 ) )
#include <windows.h>
#endif

#include <array>
#include <memory>

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

#include <Poco/JSON/Object.h>
#include <Poco/Net/StreamSocket.h>

#include "debug/debug.hpp"

#include "engine/Float.hpp"
#include "engine/Int.hpp"
#include "engine/ULong.hpp"

#include "engine/config/config.hpp"

#include "engine/Process.hpp"

#include "logging/logging.hpp"

#include "engine/JSON.hpp"

#include "engine/utils/utils.hpp"

#include "engine/containers/containers.hpp"

// ----------------------------------------------------
// Module files

#include "engine/communication/Monitor.hpp"

#include "engine/communication/Logger.hpp"

#include "engine/communication/Receiver.hpp"
#include "engine/communication/Sender.hpp"

#include "engine/communication/SocketLogger.hpp"
#include "engine/communication/Warner.hpp"

// ----------------------------------------------------

#endif  // ENGINE_COMMUNICATION_COMMUNICATION_HPP_
