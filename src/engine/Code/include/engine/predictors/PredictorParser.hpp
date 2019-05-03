#ifndef ENGINE_PREDICTORS_PREDICTORPARSER_HPP_
#define ENGINE_PREDICTORS_PREDICTORPARSER_HPP_

namespace engine
{
namespace predictors
{
// ----------------------------------------------------------------------

struct PredictorParser
{
    /// Given the _tree, return a shared pointer containing the appropriate
    /// metric.
    static std::shared_ptr<Predictor> parse(
        const Poco::JSON::Object &_json_obj );
};

// ----------------------------------------------------------------------
}  // namespace predictors
}  // namespace engine

#endif  // ENGINE_PREDICTORS_PREDICTORPARSER_HPP_
