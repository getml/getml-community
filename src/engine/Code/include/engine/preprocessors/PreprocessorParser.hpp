#ifndef ENGINE_PREPROCESSORS_PREPROCESSORPARSER_HPP_
#define ENGINE_PREPROCESSORS_PREPROCESSORPARSER_HPP_

namespace engine
{
namespace preprocessors
{
// ----------------------------------------------------

struct PreprocessorParser
{
    /// Returns the correct preprocessor to use based on the JSON object.
    static std::shared_ptr<Preprocessor> parse(
        const std::shared_ptr<containers::Encoding>& _categories,
        const Poco::JSON::Object& _obj );
};

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_PREPROCESSORPARSER_HPP_

