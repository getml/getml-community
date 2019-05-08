#ifndef RELBOOST_ENSEMBLE_ENSEMBLE_HPP_
#define RELBOOST_ENSEMBLE_ENSEMBLE_HPP_

// ----------------------------------------------------------------------------
// Dependencies

#include <fstream>
#include <memory>
#include <tuple>
#include <vector>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>

#include "metrics/metrics.hpp"

#include "relboost/types.hpp"

#include "debug/debug.hpp"

#include "relboost/Hyperparameters.hpp"

#include "relboost/lossfunctions/lossfunctions.hpp"

#include "relboost/decisiontrees/decisiontrees.hpp"

// ----------------------------------------------------------------------------

#include "relboost/ensemble/Placeholder.hpp"

#include "relboost/ensemble/TableHolder.hpp"

#include "relboost/ensemble/DecisionTreeEnsembleImpl.hpp"

#include "relboost/ensemble/DecisionTreeEnsemble.hpp"

#include "relboost/ensemble/Threadutils.hpp"

// ----------------------------------------------------------------------------

#endif  // RELBOOST_ENSEMBLE_ENSEMBLE_HPP_