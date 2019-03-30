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

#include "types.hpp"

#include "debug/debug.hpp"

#include "Hyperparameters.hpp"

#include "lossfunctions/lossfunctions.hpp"

#include "decisiontrees/decisiontrees.hpp"

// ----------------------------------------------------------------------------

#include "ensemble/Placeholder.hpp"

#include "ensemble/TableHolder.hpp"

#include "ensemble/DecisionTreeEnsembleImpl.hpp"

#include "ensemble/DecisionTreeEnsemble.hpp"

// ----------------------------------------------------------------------------

#endif  // RELBOOST_ENSEMBLE_ENSEMBLE_HPP_