#ifndef ENGINE_SRV_SRV_HPP_
#define ENGINE_SRV_SRV_HPP_

// ----------------------------------------------------
// Dependencies

#include <memory>

#include <Poco/Crypto/DigestEngine.h>
#include <Poco/File.h>
#include <Poco/HMACEngine.h>
#include <Poco/MD5Engine.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/TCPServer.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Poco/Net/TCPServerConnectionFactory.h>
#include <Poco/Path.h>

#include "engine/config/config.hpp"

#include "engine/logging/logging.hpp"

#include "engine/communication/communication.hpp"

#include "engine/handlers/handlers.hpp"

// ----------------------------------------------------
// Module files

#include "engine/srv/RequestHandler.hpp"

#include "engine/srv/ServerConnectionFactoryImpl.hpp"

// ----------------------------------------------------

#endif  // ENGINE_SRV_SRV_HPP_
