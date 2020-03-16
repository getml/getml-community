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
        : allow_http_( false ),
          categories_( _categories ),
          obj_( _obj ),
          session_name_( JSON::get_value<std::string>( _obj, "session_name_" ) )
    {
        // This won't to anything - the point it to make sure that it can be
        // parsed correctly.
        init_feature_engineerers( 1 );
        init_predictors( "feature_selectors_", 1 );
        init_predictors( "predictors_", 1 );
    }

    ~Pipeline() = default;

    // --------------------------------------------------------

   public:
    /// Returns the feature names.
    std::tuple<
        std::vector<std::string>,
        std::vector<std::string>,
        std::vector<std::string>>
    feature_names() const;

    /// Fit the pipeline.
    void fit(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        Poco::Net::StreamSocket* _socket );

    /// Generate features.
    containers::Features transform(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        Poco::Net::StreamSocket* _socket );

    /// Return the pipeline as a JSON Object.
    Poco::JSON::Object to_json_obj( const bool _schema_only ) const;

    /// Expresses the Pipeline in a form the monitor can understand.
    Poco::JSON::Object to_monitor( const std::string& _name ) const;

    /// Express features as SQL code
    std::string to_sql() const;

    // --------------------------------------------------------

   public:
    /// Trivial accessor
    bool& allow_http() { return allow_http_; }

    /// Trivial (const) accessor
    bool allow_http() const { return allow_http_; }

    /// Whether the pipeline contains any premium_only feature engineerers
    bool premium_only() const
    {
        return std::any_of(
            feature_engineerers_.begin(),
            feature_engineerers_.end(),
            []( const std::shared_ptr<
                featureengineerers::AbstractFeatureEngineerer>& fe ) {
                assert_true( fe );
                return fe->premium_only();
            } );
    }

    /// Trivial (const) accessor
    const std::string& session_name() const { return session_name_; }

    /// Trivial (const) accessor
    const metrics::Scores& scores() const { return scores_; }

    // --------------------------------------------------------

   private:
    /// Add all numerical columns in the population table that
    /// haven't been explicitly marked "comparison only".
    void add_population_cols(
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        containers::Features* _features ) const;

    /// Calculates the feature statistics to be displayed in the monitor.
    void calculate_feature_stats(
        const containers::Features _features,
        const size_t _nrows,
        const size_t _ncols,
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames );

    /// Fits the predictors.
    void fit_predictors(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>*
            _predictors,
        Poco::Net::StreamSocket* _socket ) const;

    /// Generates the numerical features (which also includes numerical columns
    /// from the population table).
    containers::Features generate_numerical_features(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        Poco::Net::StreamSocket* _socket ) const;

    /// Generates the predictions based on the features.
    containers::Features generate_predictions(
        const containers::CategoricalFeatures& _categorical_features,
        const containers::Features& _numerical_features ) const;

    /// Gets the categorical columns in the population table that are to be
    /// included in the predictor.
    containers::CategoricalFeatures get_categorical_features(
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames )
        const;

    /// Prepares the feature engineerers from the JSON object.
    std::vector<std::shared_ptr<featureengineerers::AbstractFeatureEngineerer>>
    init_feature_engineerers( const size_t _num_targets ) const;

    /// Prepares the feature engineerers from the JSON object.
    std::vector<std::shared_ptr<featureengineerers::AbstractFeatureEngineerer>>
    init_feature_engineerers(
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames )
        const;

    /// Prepares the predictors or feature engineerers from the JSON object.
    std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>
    init_predictors(
        const std::string& _elem, const size_t _num_targets ) const;

    /// Prepares the predictors or feature engineerers from the JSON object.
    std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>
    init_predictors(
        const std::string& _elem,
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames )
        const;

    /// Figures out which columns from the population table we would like to
    /// add.
    void make_predictor_impl(
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames );

    // --------------------------------------------------------

   private:
    /// Trivial (private) accessor
    size_t num_predictors_per_set() const
    {
        if ( predictors_.size() == 0 )
            {
                return 0;
            }

        const auto n_expected = predictors_[0].size();

        for ( const auto& pset : predictors_ )
            {
                assert_true( pset.size() == n_expected );
            }

        return n_expected;
    }

    /// Trivial (private) accessor
    size_t num_predictor_sets() const { return predictors_.size(); }

    /// Trivial (private) accessor
    predictors::Predictor* predictor( size_t _i, size_t _j )
    {
        assert_true( _i < predictors_.size() );
        assert_true( _j < predictors_[_j].size() );
        assert_true( predictors_[_i][_j] );
        return predictors_[_i][_j].get();
    }

    /// Trivial (private) accessor
    const predictors::Predictor* predictor( size_t _i, size_t _j ) const
    {
        assert_true( _i < predictors_.size() );
        assert_true( _j < predictors_[_j].size() );
        assert_true( predictors_[_i][_j] );
        return predictors_[_i][_j].get();
    }

    /// Trivial (private) accessor
    predictors::PredictorImpl& predictor_impl()
    {
        throw_unless( predictor_impl_, "Pipeline has not been fitted." );
        return *predictor_impl_;
    }

    /// Trivial (private) accessor
    const predictors::PredictorImpl& predictor_impl() const
    {
        throw_unless( predictor_impl_, "Pipeline has not been fitted." );
        return *predictor_impl_;
    }

    // --------------------------------------------------------

   private:
    /// Whether the pipeline is allowed to handle HTTP requests.
    bool allow_http_;

    /// The categories used for the mapping - needed by the feature engineerers.
    std::shared_ptr<const std::vector<strings::String>> categories_;

    /// The feature engineerers used in this pipeline.
    std::vector<std::shared_ptr<featureengineerers::AbstractFeatureEngineerer>>
        feature_engineerers_;

    /// The JSON Object used to construct the pipeline.
    Poco::JSON::Object obj_;

    /// Pimpl for the predictors.
    std::shared_ptr<predictors::PredictorImpl> predictor_impl_;

    /// The predictors used in this pipeline (every target has its own set of
    /// predictors).
    std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>
        predictors_;

    /// The scores used to evaluate this pipeline
    metrics::Scores scores_;

    /// The name of the session used.
    std::string session_name_;

    // -----------------------------------------------
};

//  ----------------------------------------------------------------------------

}  // namespace pipelines
}  // namespace engine

// ----------------------------------------------------------------------------
#endif  // ENGINE_PIPELINES_PIPELINE_HPP_
