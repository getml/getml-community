#ifndef ENGINE_COMMUNICATION_COMMUNICATION_HPP_
#define ENGINE_COMMUNICATION_COMMUNICATION_HPP_

// ----------------------------------------------------
// Dependencies

#include <cassert>
#include <memory>

#include <Poco/Net/StreamSocket.h>

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

// ----------------------------------------------------

#endif  // ENGINE_COMMUNICATION_COMMUNICATION_HPP_
