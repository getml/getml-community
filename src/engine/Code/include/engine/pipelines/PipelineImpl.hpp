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

    PipelineImpl( const Poco::JSON::Object& _obj )
        : allow_http_( false ),
          creation_time_( make_creation_time() ),
          include_categorical_(
              JSON::get_value<bool>( _obj, "include_categorical_" ) ),
          obj_( _obj )
    {
    }

    PipelineImpl()
        : allow_http_( false ),
          creation_time_( make_creation_time() ),
          include_categorical_( false ),
          obj_( Poco::JSON::Object() )
    {
    }

    ~PipelineImpl() = default;

    // -----------------------------------------------

    // Helper function for the creation time.
    static std::string make_creation_time()
    {
        const auto now = Poco::LocalDateTime();
        return Poco::DateTimeFormatter::format(
            now, Poco::DateTimeFormat::SORTABLE_FORMAT );
    }

    // -----------------------------------------------

    /// Whether the pipeline is allowed to handle HTTP requests.
    bool allow_http_;

    /// Date and time of creation, expressed as a string
    std::string creation_time_;

    /// The fingerprints of the data frames used for fitting.
    std::vector<Poco::JSON::Object::Ptr> df_fingerprints_;

    /// Pimpl for the feature selectors.
    std::shared_ptr<const predictors::PredictorImpl> feature_selector_impl_;

    /// The fingerprints of the feature learners used for fitting.
    std::vector<Poco::JSON::Object::Ptr> fe_fingerprints_;

    /// The fingerprints of the feature selectors used for fitting.
    std::vector<Poco::JSON::Object::Ptr> fs_fingerprints_;

    /// Whether we want to include categorical features
    bool include_categorical_;

    /// The JSON Object used to construct the pipeline.
    Poco::JSON::Object obj_;

    /// The schema of the peripheral tables as they are inserted into the
    /// feature learners.
    std::shared_ptr<const std::vector<helpers::Schema>>
        modified_peripheral_schema_;

    /// The schema of the population as it is inserted into the feature
    /// learners.
    std::shared_ptr<const helpers::Schema> modified_population_schema_;

    /// The schema of the peripheral tables.
    std::shared_ptr<const std::vector<helpers::Schema>> peripheral_schema_;

    /// The schema of the population.
    std::shared_ptr<const helpers::Schema> population_schema_;

    /// Pimpl for the predictors.
    std::shared_ptr<const predictors::PredictorImpl> predictor_impl_;

    /// The fingerprints of the preprocessor used for fitting.
    std::vector<Poco::JSON::Object::Ptr> preprocessor_fingerprints_;

    /// The scores used to evaluate this pipeline
    metrics::Scores scores_;

    /// The names of the targets.
    std::vector<std::string> targets_;

    // -----------------------------------------------
};

//  ----------------------------------------------------------------------------

}  // namespace pipelines
}  // namespace engine

// ----------------------------------------------------------------------------
#endif  // ENGINE_PIPELINES_PIPELINEIMPL_HPP_
