#ifndef ENGINE_FEATURELEARNERS_FEATURELEARNERPARSER_HPP_
#define ENGINE_FEATURELEARNERS_FEATURELEARNERPARSER_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace featurelearners
{
// ----------------------------------------------------------------------------

struct FeatureLearnerParser
{
    /// Returns the correct feature learner
    static std::shared_ptr<AbstractFeatureLearner> parse(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const Poco::JSON::Object>& _placeholder,
        const std::shared_ptr<const std::vector<std::string>>& _peripheral,
        const std::vector<Poco::JSON::Object::Ptr>& _dependencies );
};

// ----------------------------------------------------------------------------

}  // namespace featurelearners
}  // namespace engine

// ----------------------------------------------------------------------------

#endif  // ENGINE_FEATURELEARNERS_FEATURELEARNERPARSER_HPP_

