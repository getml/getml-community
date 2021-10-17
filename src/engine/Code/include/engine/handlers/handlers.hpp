#ifndef ENGINE_HANDLERS_HANDLERS_HPP_
#define ENGINE_HANDLERS_HANDLERS_HPP_

// ----------------------------------------------------
// Dependencies

#if ( defined( _WIN32 ) || defined( _WIN64 ) )
#define _WINSOCKAPI_  // stops windows.h including winsock.h
#include <windows.h>
#include <winsock2.h>

#include <openssl/ossl_typ.h>
#endif

#include <chrono>
#include <cstdint>
#include <iostream>
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

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <multithreading/multithreading.hpp>

#include "multirel/descriptors/descriptors.hpp"

#include "multirel/decisiontrees/decisiontrees.hpp"

#include "multirel/ensemble/ensemble.hpp"

#include "relboost/Hyperparameters.hpp"

#include "relboost/ensemble/ensemble.hpp"

#include "engine/config/config.hpp"

#include "engine/dependency/dependency.hpp"

#include "engine/utils/utils.hpp"

#include "engine/JSON.hpp"

#include "engine/containers/containers.hpp"

#include "engine/communication/communication.hpp"

#include "engine/hyperparam/hyperparam.hpp"

#include "engine/licensing/licensing.hpp"

#include "engine/pipelines/pipelines.hpp"

// ----------------------------------------------------
// Module files

#include "engine/handlers/CatOpParser.hpp"
#include "engine/handlers/FileHandler.hpp"
#include "engine/handlers/NumOpParser.hpp"

#include "engine/handlers/ArrowSocketInputStream.hpp"
#include "engine/handlers/ArrowSocketOutputStream.hpp"

#include "engine/handlers/ArrowHandler.hpp"

#include "engine/handlers/BoolOpParser.hpp"

#include "engine/handlers/AggOpParser.hpp"

#include "engine/handlers/ViewParser.hpp"

#include "engine/handlers/DatabaseManager.hpp"

#include "engine/handlers/DataFrameManager.hpp"

#include "engine/handlers/HyperoptManager.hpp"

#include "engine/handlers/PipelineManager.hpp"

#include "engine/handlers/ProjectManager.hpp"

// ----------------------------------------------------

#endif  // ENGINE_HANDLERS_HANDLERS_HPP_
