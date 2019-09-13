#ifndef ENGINE_HANDLERS_HANDLERS_HPP_
#define ENGINE_HANDLERS_HANDLERS_HPP_

// ----------------------------------------------------
// Dependencies

#include <chrono>
#include <optional>
#include <thread>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include <Poco/Crypto/DigestEngine.h>
#include <Poco/DateTimeFormat.h>
#include <Poco/DateTimeFormatter.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/File.h>
#include <Poco/HMACEngine.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/MD5Engine.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Path.h>
#include <Poco/Timestamp.h>

#include <multithreading/multithreading.hpp>

#include "autosql/descriptors/descriptors.hpp"

#include "autosql/decisiontrees/decisiontrees.hpp"

#include "autosql/ensemble/ensemble.hpp"

#include "relboost/Hyperparameters.hpp"

#include "relboost/ensemble/ensemble.hpp"

#include "engine/config/config.hpp"

#include "engine/utils/utils.hpp"

#include "engine/JSON.hpp"

#include "engine/containers/containers.hpp"

#include "engine/communication/communication.hpp"

#include "engine/models/models.hpp"

#include "engine/licensing/licensing.hpp"

// ----------------------------------------------------
// Module files

#include "engine/handlers/CatOpParser.hpp"
#include "engine/handlers/FileHandler.hpp"
#include "engine/handlers/NumOpParser.hpp"

#include "engine/handlers/BoolOpParser.hpp"

#include "engine/handlers/AggOpParser.hpp"
#include "engine/handlers/GroupByParser.hpp"

#include "engine/handlers/DataFrameJoiner.hpp"

#include "engine/handlers/DatabaseManager.hpp"

#include "engine/handlers/DataFrameManager.hpp"
#include "engine/handlers/ModelManager.hpp"

#include "engine/handlers/AutoSQLModelManager.hpp"
#include "engine/handlers/RelboostModelManager.hpp"

#include "engine/handlers/ProjectManager.hpp"

// ----------------------------------------------------

#endif  // ENGINE_HANDLERS_HANDLERS_HPP_
