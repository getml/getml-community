#ifndef ENGINE_FEATUREENGINEERERS_FEATUREENGINEERERPARSER_HPP_
#define ENGINE_FEATUREENGINEERERS_FEATUREENGINEERERPARSER_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace featureengineerers
{
// ----------------------------------------------------------------------------

struct FeatureEngineererParser
{
    /// Returns the correct feature engineerer
    static std::shared_ptr<AbstractFeatureEngineerer> parse(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const Poco::JSON::Object>& _placeholder,
        const std::shared_ptr<const std::vector<std::string>>& _peripheral,
        const std::shared_ptr<const std::vector<strings::String>>&
            _categories );
};

// ----------------------------------------------------------------------------

}  // namespace featureengineerers
}  // namespace engine

// ----------------------------------------------------------------------------

#endif  // ENGINE_FEATUREENGINEERERS_FEATUREENGINEERERPARSER_HPP_

