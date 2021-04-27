#ifndef ENGINE_FEATURELEARNERS_FEATURELEARNERPARAMS_HPP_
#define ENGINE_FEATURELEARNERS_FEATURELEARNERPARAMS_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace featurelearners
{
// ----------------------------------------------------------------------------

struct FeatureLearnerParams
{
    /// The aggregations, expressed in string form.
    const std::shared_ptr<const std::vector<std::string>>& aggregation_;

    /// The aggregations used for the mapping.
    const std::vector<helpers::MappingAggregation>& aggregation_enums_;

    /// The command used to initialize the feature learner.
    const Poco::JSON::Object& cmd_;

    /// The dependencies used for the fingerprint.
    const std::vector<Poco::JSON::Object::Ptr>& dependencies_;

    /// The minimum frequency used for the mappings.
    const size_t& min_freq_;

    /// The names of the peripheral tables.
    const std::shared_ptr<const std::vector<std::string>>& peripheral_;

    /// The placeholder representing the data model.
    const std::shared_ptr<const helpers::Placeholder>& placeholder_;
};

// ----------------------------------------------------------------------------
}  // namespace featurelearners
}  // namespace engine

// ----------------------------------------------------------------------------

#endif  // ENGINE_FEATURELEARNERS_FEATURELEARNERPARAMS_HPP_

