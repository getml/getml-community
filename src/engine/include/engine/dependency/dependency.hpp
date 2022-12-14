// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef ENGINE_DEPENDENCY_DEPENDENCY_HPP_
#define ENGINE_DEPENDENCY_DEPENDENCY_HPP_

// ----------------------------------------------------
// Dependencies

#include <map>
#include <optional>

#include <Poco/JSON/Object.h>

#include "engine/communication/communication.hpp"

#include "engine/preprocessors/preprocessors.hpp"

#include "engine/featurelearners/featurelearners.hpp"

#include "predictors/predictors.hpp"

#include "engine/JSON.hpp"

// ----------------------------------------------------
// Module files

#include "engine/dependency/DataFrameTracker.hpp"
#include "engine/dependency/Tracker.hpp"

#include "engine/dependency/FETracker.hpp"
#include "engine/dependency/PredTracker.hpp"
#include "engine/dependency/PreprocessorTracker.hpp"
#include "engine/dependency/WarningTracker.hpp"

// ----------------------------------------------------

#endif  // ENGINE_DEPENDENCY_DEPENDENCY_HPP_
