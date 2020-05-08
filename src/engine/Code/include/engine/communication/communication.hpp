#ifndef ENGINE_COMMUNICATION_COMMUNICATION_HPP_
#define ENGINE_COMMUNICATION_COMMUNICATION_HPP_

// ----------------------------------------------------
// Dependencies

#include <array>
#include <memory>

#include <Poco/JSON/Object.h>
#include <Poco/Net/StreamSocket.h>

#include "debug/debug.hpp"

#include "engine/Float.hpp"
#include "engine/Int.hpp"
#include "engine/ULong.hpp"

#include "engine/JSON.hpp"

#include "engine/utils/utils.hpp"

#include "engine/containers/containers.hpp"

#include "engine/monitoring/monitoring.hpp"

// ----------------------------------------------------
// Module files

#include "engine/communication/Receiver.hpp"
#include "engine/communication/Sender.hpp"

#include "engine/communication/Warner.hpp"

// ----------------------------------------------------

#endif  // ENGINE_COMMUNICATION_COMMUNICATION_HPP_
