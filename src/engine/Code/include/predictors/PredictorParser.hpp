#ifndef PREDICTORS_PREDICTORPARSER_HPP_
#define PREDICTORS_PREDICTORPARSER_HPP_

namespace predictors
{
// ----------------------------------------------------------------------

struct PredictorParser
{
    /// Parses the predictor.
    static std::shared_ptr<Predictor> parse(
        const Poco::JSON::Object& _json_obj,
        const std::shared_ptr<const PredictorImpl>& _impl,
        const std::vector<Poco::JSON::Object::Ptr>& _dependencies );
};

// ----------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_PREDICTORPARSER_HPP_
