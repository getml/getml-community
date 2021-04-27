#ifndef RELBOOST_ENSEMBLE_ENSEMBLE_HPP_
#define RELBOOST_ENSEMBLE_ENSEMBLE_HPP_

// ----------------------------------------------------------------------------
// Dependencies

#include <fstream>
#include <memory>
#include <optional>
#include <tuple>
#include <vector>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>

#include "multithreading/multithreading.hpp"

#include "logging/logging.hpp"

#include "metrics/metrics.hpp"

#include "strings/strings.hpp"

#include "relboost/Float.hpp"
#include "relboost/Int.hpp"

#include "debug/debug.hpp"

#include "relboost/utils/utils.hpp"

#include "relboost/Hyperparameters.hpp"

#include "relboost/lossfunctions/lossfunctions.hpp"

#include "relboost/decisiontrees/decisiontrees.hpp"

// ----------------------------------------------------------------------------

#include "relboost/ensemble/FitParams.hpp"
#include "relboost/ensemble/TransformParams.hpp"

#include "relboost/ensemble/TableHolder.hpp"

#include "relboost/ensemble/DecisionTreeEnsembleImpl.hpp"

#include "relboost/ensemble/DecisionTreeEnsemble.hpp"

#include "relboost/ensemble/SubtreeHelper.hpp"

#include "relboost/ensemble/ThreadutilsFitParams.hpp"
#include "relboost/ensemble/ThreadutilsTransformParams.hpp"

#include "relboost/ensemble/Threadutils.hpp"

// ----------------------------------------------------------------------------

#endif  // RELBOOST_ENSEMBLE_ENSEMBLE_HPP_
