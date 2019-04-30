#ifndef METRICS_METRICS_HPP_
#define METRICS_METRICS_HPP_

// ----------------------------------------------------
// Dependencies

#include <array>
#include <memory>
#include <numeric>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include "metrics/types.hpp"

#include "multithreading/multithreading.hpp"

// ----------------------------------------------------
// Module files

#include "metrics/JSON.hpp"

#include "metrics/Metric.hpp"

#include "metrics/AUC.hpp"
#include "metrics/Accuracy.hpp"
#include "metrics/CrossEntropy.hpp"
#include "metrics/MAE.hpp"
#include "metrics/RMSE.hpp"
#include "metrics/RSquared.hpp"

#include "metrics/MetricParser.hpp"

// ----------------------------------------------------

#endif  // METRICS_METRICS_HPP_
