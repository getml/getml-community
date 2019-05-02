#ifndef ENGINE_PREDICTORS_PREDICTORS_HPP_
#define ENGINE_PREDICTORS_PREDICTORS_HPP_

// ----------------------------------------------------
// Dependencies

#include "Poco/JSON/Object.h"

#include <Poco/JSON/Parser.h>

#include <xgboost/c_api.h>

#include "engine/types.hpp"

#include "engine/JSON.hpp"

#include "engine/containers/containers.hpp"

#include "engine/logging/logging.hpp"

// ----------------------------------------------------
// Module files

#include "engine/predictors/Predictor.hpp"

#include "engine/predictors/XGBoostHyperparams.hpp"
#include "engine/predictors/XGBoostPredictor.hpp"

// ----------------------------------------------------

#endif  // ENGINE_PREDICTORS_PREDICTORS_HPP_
