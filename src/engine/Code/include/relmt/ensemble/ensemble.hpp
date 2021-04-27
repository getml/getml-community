#ifndef RELMT_ENSEMBLE_ENSEMBLE_HPP_
#define RELMT_ENSEMBLE_ENSEMBLE_HPP_

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

#include "relmt/Float.hpp"
#include "relmt/Int.hpp"

#include "debug/debug.hpp"

#include "relmt/utils/utils.hpp"

#include "relmt/Hyperparameters.hpp"

#include "relmt/lossfunctions/lossfunctions.hpp"

#include "relmt/decisiontrees/decisiontrees.hpp"

// ----------------------------------------------------------------------------

#include "relmt/ensemble/FitParams.hpp"
#include "relmt/ensemble/TransformParams.hpp"

#include "relmt/ensemble/TableHolder.hpp"

#include "relmt/ensemble/DecisionTreeEnsembleImpl.hpp"

#include "relmt/ensemble/DecisionTreeEnsemble.hpp"

#include "relmt/ensemble/SubtreeHelper.hpp"

#include "relmt/ensemble/ThreadutilsFitParams.hpp"
#include "relmt/ensemble/ThreadutilsTransformParams.hpp"

#include "relmt/ensemble/Threadutils.hpp"

// ----------------------------------------------------------------------------

#endif  // RELMT_ENSEMBLE_ENSEMBLE_HPP_
