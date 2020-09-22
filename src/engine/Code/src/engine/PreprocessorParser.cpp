#include "engine/preprocessors/preprocessors.hpp"

namespace engine
{
namespace preprocessors
{
// ----------------------------------------------------

std::shared_ptr<Preprocessor> PreprocessorParser::parse(
    const Poco::JSON::Object& _obj,
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies )
{
    const auto type = jsonutils::JSON::get_value<std::string>( _obj, "type_" );

    if ( type == Preprocessor::EMAILDOMAIN )
        {
            return std::make_shared<EMailDomain>( _obj, _dependencies );
        }

    if ( type == Preprocessor::IMPUTATION )
        {
            return std::make_shared<Imputation>( _obj, _dependencies );
        }

    if ( type == Preprocessor::SEASONAL )
        {
            return std::make_shared<Seasonal>( _obj, _dependencies );
        }

    if ( type == Preprocessor::SUBSTRING )
        {
            return std::make_shared<Substring>( _obj, _dependencies );
        }

    throw std::invalid_argument(
        "Preprocessor of type '" + type + "' not known!" );

    return std::shared_ptr<Preprocessor>();
}

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
