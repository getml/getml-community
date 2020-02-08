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

    /// Generates the numerical features (which also includes numerical columns
    /// from the population table)..
    containers::Features generate_numerical_features(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        Poco::Net::StreamSocket* _socket );

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

    /// Prepares the feature engineerers from
    std::vector<
        containers::Optional<featureengineerers::AbstractFeatureEngineerer>>
    make_feature_engineerers() const;

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
