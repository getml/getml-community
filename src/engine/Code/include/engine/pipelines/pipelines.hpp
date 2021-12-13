#ifndef ENGINE_PIPELINES_PIPELINES_HPP_
#define ENGINE_PIPELINES_PIPELINES_HPP_

// ----------------------------------------------------
// Dependencies

#include <cstdint>

#include <algorithm>
#include <fstream>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <unordered_map>
#include <vector>

#include <Poco/DateTimeFormat.h>
#include <Poco/DateTimeFormatter.h>
#include <Poco/LocalDateTime.h>

#include "engine/communication/communication.hpp"
#include "engine/containers/containers.hpp"
#include "engine/utils/utils.hpp"

#include "metrics/metrics.hpp"

#include "engine/dependency/dependency.hpp"

#include "engine/featurelearners/featurelearners.hpp"

#include "engine/preprocessors/preprocessors.hpp"

#include "predictors/predictors.hpp"

// ----------------------------------------------------
// Module files

#include "engine/pipelines/PlaceholderMaker.hpp"

#include "engine/pipelines/DataFrameModifier.hpp"
#include "engine/pipelines/Staging.hpp"

#include "engine/pipelines/CheckParams.hpp"
#include "engine/pipelines/FitParams.hpp"
#include "engine/pipelines/TransformParams.hpp"

#include "engine/pipelines/PipelineImpl.hpp"

#include "engine/pipelines/Pipeline.hpp"

// ----------------------------------------------------

#endif  // ENGINE_PIPELINES_PIPELINES_HPP_
