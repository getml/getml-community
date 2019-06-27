#ifndef PREDICTORS_PREDICTORS_HPP_
#define PREDICTORS_PREDICTORS_HPP_

// ----------------------------------------------------
// Dependencies

#include <cstdint>

#include <fstream>
#include <memory>
#include <numeric>
#include <unordered_map>
#include <vector>

#include "Poco/JSON/Array.h"
#include "Poco/JSON/Object.h"

#include <Poco/JSON/Parser.h>

#include <Eigen/Dense>

#include <xgboost/c_api.h>

#include "logging/logging.hpp"

// ----------------------------------------------------
// Module files

#include "predictors/Float.hpp"
#include "predictors/Int.hpp"

#include "predictors/CFloatColumn.hpp"
#include "predictors/CIntColumn.hpp"
#include "predictors/FloatColumn.hpp"
#include "predictors/IntColumn.hpp"

#include "predictors/JSON.hpp"

#include "predictors/CSRMatrix.hpp"
#include "predictors/Encoding.hpp"

#include "predictors/Predictor.hpp"

#include "predictors/LinearRegression.hpp"

#include "predictors/XGBoostHyperparams.hpp"
#include "predictors/XGBoostPredictor.hpp"

#include "predictors/PredictorParser.hpp"

// ----------------------------------------------------

#endif  // PREDICTORS_PREDICTORS_HPP_
