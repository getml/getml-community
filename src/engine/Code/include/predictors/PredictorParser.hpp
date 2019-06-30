#ifndef PREDICTORS_PREDICTORPARSER_HPP_
#define PREDICTORS_PREDICTORPARSER_HPP_

namespace predictors
{
// ----------------------------------------------------------------------

struct PredictorParser
{
    /// Given the _tree, return a shared pointer containing the appropriate
    /// metric.
    static std::shared_ptr<Predictor> parse(
        const Poco::JSON::Object& _json_obj,
        const std::shared_ptr<const PredictorImpl>& _impl );
};

// ----------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_PREDICTORPARSER_HPP_
