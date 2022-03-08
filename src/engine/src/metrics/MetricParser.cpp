#include "metrics/MetricParser.hpp"

#include "metrics/AUC.hpp"
#include "metrics/Accuracy.hpp"
#include "metrics/CrossEntropy.hpp"
#include "metrics/MAE.hpp"
#include "metrics/RMSE.hpp"
#include "metrics/RSquared.hpp"

namespace metrics {
// ----------------------------------------------------------------------------

std::shared_ptr<Metric> MetricParser::parse(
    const std::string& _type, multithreading::Communicator* _comm) {
  if (_type == "accuracy_") {
    return std::make_shared<Accuracy>(_comm);
  } else if (_type == "auc_") {
    return std::make_shared<AUC>(_comm);
  } else if (_type == "cross_entropy_") {
    return std::make_shared<CrossEntropy>(_comm);
  } else if (_type == "mae_") {
    return std::make_shared<MAE>(_comm);
  } else if (_type == "rmse_") {
    return std::make_shared<RMSE>(_comm);
  } else if (_type == "rsquared_") {
    return std::make_shared<RSquared>(_comm);
  } else {
    throw std::runtime_error("Metric of type '" + _type + "' not known!");
  }
}

// ----------------------------------------------------------------------------
}  // namespace metrics
