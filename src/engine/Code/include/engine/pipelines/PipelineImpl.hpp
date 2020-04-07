#ifndef ENGINE_PIPELINES_PIPELINEIMPL_HPP_
#define ENGINE_PIPELINES_PIPELINEIMPL_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace pipelines
{
// ----------------------------------------------------------------------------

struct PipelineImpl
{
    // -----------------------------------------------

    PipelineImpl(
        const std::shared_ptr<const std::vector<strings::String>>& _categories,
        const Poco::JSON::Object& _obj )
        : allow_http_( false ),
          categories_( _categories ),
          obj_( _obj ),
          session_name_( JSON::get_value<std::string>( _obj, "session_name_" ) )
    {
    }

    PipelineImpl(
        const std::shared_ptr<const std::vector<strings::String>>& _categories )
        : allow_http_( false ),
          categories_( _categories ),
          obj_( Poco::JSON::Object() ),
          session_name_( "" )
    {
    }

    ~PipelineImpl() = default;

    // -----------------------------------------------

    /// Whether the pipeline is allowed to handle HTTP requests.
    bool allow_http_;

    /// The categories used for the mapping - needed by the feature engineerers.
    std::shared_ptr<const std::vector<strings::String>> categories_;

    /// The fingerprints of the data frames used for fitting.
    std::vector<Poco::JSON::Object::Ptr> df_fingerprints_;

    /// Pimpl for the feature selectors.
    std::shared_ptr<const predictors::PredictorImpl> feature_selector_impl_;

    /// The fingerprints of the feature engineerers used for fitting.
    std::vector<Poco::JSON::Object::Ptr> fe_fingerprints_;

    /// The fingerprints of the feature selectors used for fitting.
    std::vector<Poco::JSON::Object::Ptr> fs_fingerprints_;

    /// The JSON Object used to construct the pipeline.
    Poco::JSON::Object obj_;

    /// The schema of the peripheral tables.
    Poco::JSON::Array::Ptr peripheral_schema_;

    /// The schema of the population.
    Poco::JSON::Object::Ptr population_schema_;

    /// Pimpl for the predictors.
    std::shared_ptr<const predictors::PredictorImpl> predictor_impl_;

    /// The scores used to evaluate this pipeline
    metrics::Scores scores_;

    /// Allows us to associate the pipeline with a hyperparameter optimization
    /// routine.
    std::string session_name_;

    /// The names of the targets.
    std::vector<std::string> targets_;

    // -----------------------------------------------
};

//  ----------------------------------------------------------------------------

}  // namespace pipelines
}  // namespace engine

// ----------------------------------------------------------------------------
#endif  // ENGINE_PIPELINES_PIPELINEIMPL_HPP_
