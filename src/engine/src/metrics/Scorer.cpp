#include "metrics/Scorer.hpp"

#include "metrics/MetricParser.hpp"

namespace metrics {
// ----------------------------------------------------------------------------

Poco::JSON::Object Scorer::get_metrics(const Poco::JSON::Object& _obj) {
  const auto retrieve_metric = [_obj](const std::string& name,
                                      Poco::JSON::Object* result) {
    if (_obj.has(name)) {
      const auto arr = jsonutils::JSON::get_array(_obj, name);

      if (arr->size() > 0) {
        result->set(name, arr);
      }
    }
  };

  Poco::JSON::Object result;

  retrieve_metric("accuracy_", &result);

  retrieve_metric("auc_", &result);

  retrieve_metric("cross_entropy_", &result);

  retrieve_metric("mae_", &result);

  retrieve_metric("rmse_", &result);

  retrieve_metric("rsquared_", &result);

  return result;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Scorer::score(const bool _is_classification,
                                 const Features _yhat, const Features _y) {
  // ------------------------------------------------------------------------
  // Build up names.

  std::vector<std::string> names;

  if (_is_classification) {
    names.push_back("accuracy_");
    names.push_back("auc_");
    names.push_back("cross_entropy_");
  } else {
    names.push_back("mae_");
    names.push_back("rmse_");
    names.push_back("rsquared_");
  }

  // ------------------------------------------------------------------------
  // Do the actual scoring.

  Poco::JSON::Object obj;

  for (const auto& name : names) {
    const auto metric = metrics::MetricParser::parse(name, nullptr);

    const auto scores = metric->score(_yhat, _y);

    for (auto it = scores.begin(); it != scores.end(); ++it) {
      obj.set(it->first, it->second);
    }
  }

  // ------------------------------------------------------------------------

  return obj;

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace metrics
