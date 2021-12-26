#include "engine/featurelearners/FeatureLearnerParser.hpp"

// ----------------------------------------------------------------------

#include "fastprop/algorithm/algorithm.hpp"
#include "multirel/ensemble/ensemble.hpp"
#include "relboost/ensemble/ensemble.hpp"
#include "relmt/ensemble/ensemble.hpp"

// ----------------------------------------------------------------------

#include "engine/featurelearners/FeatureLearner.hpp"

// ----------------------------------------------------------------------

namespace engine {
namespace featurelearners {
// ----------------------------------------------------------------------

std::shared_ptr<AbstractFeatureLearner> FeatureLearnerParser::parse(
    const FeatureLearnerParams& _params) {
  const auto type = JSON::get_value<std::string>(_params.cmd_, "type_");

  if (type == AbstractFeatureLearner::FASTPROP) {
    return std::make_shared<FeatureLearner<fastprop::algorithm::FastProp>>(
        _params);
  }

  if (type == AbstractFeatureLearner::MULTIREL) {
    return std::make_shared<
        FeatureLearner<multirel::ensemble::DecisionTreeEnsemble>>(_params);
  }

  if (type == AbstractFeatureLearner::RELBOOST) {
    return std::make_shared<
        FeatureLearner<relboost::ensemble::DecisionTreeEnsemble>>(_params);
  }

  if (type == AbstractFeatureLearner::RELMT) {
    return std::make_shared<
        FeatureLearner<relmt::ensemble::DecisionTreeEnsemble>>(_params);
  }

  throw std::invalid_argument("Feature learning algorithm of type '" + type +
                              "' not known!");

  return std::shared_ptr<AbstractFeatureLearner>();
}

// ----------------------------------------------------------------------
}  // namespace featurelearners
}  // namespace engine
