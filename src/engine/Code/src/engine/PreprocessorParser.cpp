#include "engine/preprocessors/preprocessors.hpp"

namespace engine
{
namespace preprocessors
{
// ----------------------------------------------------

std::shared_ptr<Preprocessor> PreprocessorParser::parse(
    const Poco::JSON::Object& _obj )
{
    const auto type = jsonutils::JSON::get_value<std::string>( _obj, "type_" );

    if ( type == Preprocessor::SEASONAL )
        {
            return std::make_shared<Seasonal>( _obj );
        }

    throw std::invalid_argument(
        "Preprocessor of type '" + type + "' not known!" );

    return std::shared_ptr<Preprocessor>();
}

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
