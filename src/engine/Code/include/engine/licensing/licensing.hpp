#ifndef AUTOSQL_ENGINE_LICENSING_HPP_
#define AUTOSQL_ENGINE_LICENSING_HPP_

// ----------------------------------------------------------------------------

#include <chrono>
#include <sstream>
#include <string>
#include <thread>

#include <Poco/Crypto/RSACipherImpl.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <Poco/StreamCopier.h>
#include <Poco/URI.h>

#include "engine/config/config.hpp"
#include "engine/monitoring/monitoring.hpp"
#include "multithreading/multithreading.hpp"

#include "engine/Int.hpp"

// ----------------------------------------------------------------------------

#include "engine/crypto/crypto.hpp"

#include "engine/licensing/Token.hpp"

#include "engine/licensing/LicenseChecker.hpp"

// ----------------------------------------------------------------------------

#endif  // AUTOSQL_ENGINE _LICENSING_HPP_
