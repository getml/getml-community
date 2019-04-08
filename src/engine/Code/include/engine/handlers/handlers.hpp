#ifndef ENGINE_HANDLERS_HANDLERS_HPP_
#define ENGINE_HANDLERS_HANDLERS_HPP_

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

#include <multithreading/multithreading.hpp>

#include "engine/config/config.hpp"

#include "engine/utils/utils.hpp"

#include "engine/JSON.hpp"

#include "engine/containers/containers.hpp"

#include "engine/communication/communication.hpp"

// ----------------------------------------------------
// Module files

#include "engine/handlers/FileHandler.hpp"

#include "engine/handlers/DataFrameManager.hpp"

#include "engine/handlers/ProjectManager.hpp"

// ----------------------------------------------------

#endif  // ENGINE_HANDLERS_HANDLERS_HPP_
