#ifndef ENGINE_MODELS_MODELS_HPP_
#define ENGINE_MODELS_MODELS_HPP_

// ----------------------------------------------------
// Dependencies

#include <chrono>
#include <thread>
#include <vector>

#include <Poco/File.h>
#include <Poco/JSON/Object.h>
#include <Poco/Path.h>
#include <Poco/TemporaryFile.h>

#include "multithreading/multithreading.hpp"

#include "strings/strings.hpp"

#include "predictors/predictors.hpp"

#include "engine/config/config.hpp"

#include "engine/utils/utils.hpp"

#include "engine/JSON.hpp"

#include "engine/containers/containers.hpp"

#include "engine/communication/communication.hpp"

#include "multirel/ensemble/ensemble.hpp"

#include "relboost/ensemble/ensemble.hpp"

// ----------------------------------------------------
// Module files

#include "engine/models/AbstractModel.hpp"

#include "engine/models/Model.hpp"

#include "engine/models/MultirelModel.hpp"
#include "engine/models/RelboostModel.hpp"

// ----------------------------------------------------

#endif  // ENGINE_MODELS_MODELS_HPP_
