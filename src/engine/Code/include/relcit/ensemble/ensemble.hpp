#ifndef RELBOOSTXX_ENSEMBLE_ENSEMBLE_HPP_
#define RELBOOSTXX_ENSEMBLE_ENSEMBLE_HPP_

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

#include "relcit/Float.hpp"
#include "relcit/Int.hpp"

#include "debug/debug.hpp"

#include "relcit/utils/utils.hpp"

#include "relcit/Hyperparameters.hpp"

#include "relcit/lossfunctions/lossfunctions.hpp"

#include "relcit/decisiontrees/decisiontrees.hpp"

// ----------------------------------------------------------------------------

#include "relcit/ensemble/TableHolder.hpp"

#include "relcit/ensemble/DecisionTreeEnsembleImpl.hpp"

#include "relcit/ensemble/DecisionTreeEnsemble.hpp"

#include "relcit/ensemble/SubtreeHelper.hpp"

#include "relcit/ensemble/Threadutils.hpp"

// ----------------------------------------------------------------------------

#endif  // RELBOOSTXX_ENSEMBLE_ENSEMBLE_HPP_
