#ifndef ENGINE_PIPELINES_PIPELINE_HPP_
#define ENGINE_PIPELINES_PIPELINE_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace pipelines
{
// ----------------------------------------------------------------------------

class Pipeline
{
    // --------------------------------------------------------

   public:
    Pipeline(
        const std::shared_ptr<const std::vector<strings::String>>& _categories,
        const Poco::JSON::Object& _obj )
        : categories_( _categories ), obj_( _obj )
    {
        feature_engineerers_ = make_feature_engineerers();
    }

    ~Pipeline() = default;

    // --------------------------------------------------------

   public:
    // --------------------------------------------------------

   private:
    /// Prepares the feature engineerers from
    std::vector<
        containers::Optional<featureengineerers::AbstractFeatureEngineerer>>
    make_feature_engineerers() const;

    // --------------------------------------------------------

   private:
    /// The categories used for the mapping - needed by the feature engineerers.
    std::shared_ptr<const std::vector<strings::String>> categories_;

    /// The feature engineerers used in this pipeline.
    std::vector<
        containers::Optional<featureengineerers::AbstractFeatureEngineerer>>
        feature_engineerers_;

    /// The JSON Object used to construct the pipeline.
    Poco::JSON::Object obj_;

    /// Pimpl for the predictors.
    std::shared_ptr<predictors::PredictorImpl> predictor_impl_;

    /// The predictors used in this pipeline (every target has its own set of
    /// predictors).
    std::vector<std::vector<containers::Optional<predictors::Predictor>>>
        predictors_;

    /// The scores used to evaluate this pipeline
    metrics::Scores scores_;

    // -----------------------------------------------
};

//  ----------------------------------------------------------------------------

}  // namespace pipelines
}  // namespace engine

// ----------------------------------------------------------------------------
#endif  // ENGINE_PIPELINES_PIPELINE_HPP_
