#include "engine/featurelearners/FeatureLearnerParser.hpp"

// ----------------------------------------------------------------------

#include "fastprop/algorithm/algorithm.hpp"

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

  if (type == AbstractFeatureLearner::MULTIREL ||
      type == AbstractFeatureLearner::RELBOOST ||
      AbstractFeatureLearner::RELMT) {
    throw std::runtime_error(
        "'" + type + "' is not supported in the getML community edition!");
  }

  throw std::runtime_error("Feature learning algorithm of type '" + type +
                           "' not known!");
}

// ----------------------------------------------------------------------
}  // namespace featurelearners
}  // namespace engine
