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

fct::Ref<AbstractFeatureLearner> FeatureLearnerParser::parse(
    const FeatureLearnerParams& _params) {
  const auto type = JSON::get_value<std::string>(_params.cmd_, "type_");

  if (type == AbstractFeatureLearner::FASTPROP) {
    return fct::Ref<FeatureLearner<fastprop::algorithm::FastProp>>::make(
        _params);
  }

  if (type == AbstractFeatureLearner::MULTIREL) {
    return fct::Ref<FeatureLearner<multirel::ensemble::DecisionTreeEnsemble>>::
        make(_params);
  }

  if (type == AbstractFeatureLearner::RELBOOST) {
    return fct::Ref<FeatureLearner<relboost::ensemble::DecisionTreeEnsemble>>::
        make(_params);
  }

  if (type == AbstractFeatureLearner::RELMT) {
    return fct::Ref<
        FeatureLearner<relmt::ensemble::DecisionTreeEnsemble>>::make(_params);
  }

  throw std::runtime_error("Feature learning algorithm of type '" + type +
                           "' not known!");
}

// ----------------------------------------------------------------------
}  // namespace featurelearners
}  // namespace engine
