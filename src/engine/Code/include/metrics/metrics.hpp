#ifndef METRICS_METRICS_HPP_
#define METRICS_METRICS_HPP_

// ----------------------------------------------------
// Dependencies

#include <array>
#include <fstream>
#include <memory>
#include <numeric>
#include <unordered_map>

#include <Poco/DateTime.h>
#include <Poco/DateTimeFormatter.h>
#include <Poco/Timezone.h>

#include "jsonutils/jsonutils.hpp"
#include "multithreading/multithreading.hpp"
#include "strings/strings.hpp"

// ----------------------------------------------------
// Module files

#include "metrics/Float.hpp"
#include "metrics/Int.hpp"

#include "metrics/Features.hpp"

#include "metrics/Metric.hpp"
#include "metrics/MetricImpl.hpp"

#include "metrics/AUC.hpp"
#include "metrics/Accuracy.hpp"
#include "metrics/CrossEntropy.hpp"
#include "metrics/MAE.hpp"
#include "metrics/RMSE.hpp"
#include "metrics/RSquared.hpp"

#include "metrics/MetricParser.hpp"

#include "metrics/Scorer.hpp"
#include "metrics/Scores.hpp"
#include "metrics/Summarizer.hpp"

// ----------------------------------------------------

#endif  // METRICS_METRICS_HPP_
