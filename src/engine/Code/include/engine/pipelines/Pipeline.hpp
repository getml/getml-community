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
        const Poco::JSON::Object& _obj );

    Pipeline(
        const std::shared_ptr<const std::vector<strings::String>>& _categories,
        const std::string& _path );

    Pipeline( const Pipeline& _other );

    Pipeline( Pipeline&& _other ) noexcept;

    ~Pipeline();

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

    /// Copy assignment operator.
    Pipeline& operator=( const Pipeline& _other );

    /// Move assignment operator.
    Pipeline& operator=( Pipeline&& _other ) noexcept;

    /// Save the pipeline to disk.
    void save( const std::string& _path, const std::string& _name ) const;

    /// Score the pipeline.
    Poco::JSON::Object score(
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        const containers::Features& _yhat );

    /// Generate features.
    containers::Features transform(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        Poco::Net::StreamSocket* _socket );

    /// Expresses the Pipeline in a form the monitor can understand.
    Poco::JSON::Object to_monitor( const std::string& _name ) const;

    /// Express features as SQL code
    std::string to_sql() const;

    // --------------------------------------------------------

   public:
    /// Trivial accessor
    bool& allow_http() { return impl_.allow_http_; }

    /// Trivial (const) accessor
    bool allow_http() const { return impl_.allow_http_; }

    /// Trivial (const) accessor
    const Poco::JSON::Object& obj() const { return impl_.obj_; }

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
    const metrics::Scores& scores() const { return impl_.scores_; }

    /// Writes a JSON object to disc.
    void save_json_obj(
        const Poco::JSON::Object& _obj, const std::string& _path ) const
    {
        std::ofstream fs( _path, std::ofstream::out );
        Poco::JSON::Stringifier::stringify( _obj, fs );
        fs.close();
    }

    /// Trivial (const) accessor
    const std::string& session_name() const { return impl_.session_name_; }

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

    /// Prepares the predictors.
    std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>
    init_predictors(
        const std::string& _elem, const size_t _num_targets ) const;

    /// Loads a new Pipeline from disc.
    Pipeline load(
        const std::shared_ptr<const std::vector<strings::String>>& _categories,
        const std::string& _path ) const;

    /// Loads a JSON object from disc.
    Poco::JSON::Object load_json_obj( const std::string& _fname ) const;

    /// Figures out which columns from the population table we would like to
    /// add.
    void make_predictor_impl(
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames );

    /// Parses the peripheral names.
    std::shared_ptr<std::vector<std::string>> parse_peripheral() const;

    // --------------------------------------------------------

   private:
    /// Trivial accessor
    std::shared_ptr<const std::vector<strings::String>>& categories()
    {
        return impl_.categories_;
    }

    /// Trivial accessor
    std::shared_ptr<const std::vector<strings::String>> categories() const
    {
        return impl_.categories_;
    }

    /// Helper class used to clone the feature engineerers, selectors
    /// and predictors.
    template <class T>
    std::vector<std::shared_ptr<T>> clone(
        const std::vector<std::shared_ptr<T>>& _vec ) const
    {
        auto out = std::vector<std::shared_ptr<T>>();
        for ( const auto& elem : _vec )
            {
                assert_true( elem );
                out.push_back( elem->clone() );
            }
        return out;
    }

    /// Helper class used to clone the feature engineerers, selectors
    /// and predictors.
    template <class T>
    std::vector<std::vector<std::shared_ptr<T>>> clone(
        const std::vector<std::vector<std::shared_ptr<T>>>& _vec ) const
    {
        auto out = std::vector<std::vector<std::shared_ptr<T>>>();
        for ( const auto& elem : _vec )
            {
                out.push_back( clone( elem ) );
            }
        return out;
    }

    /// Infers the number of targets from the population table.
    size_t infer_num_targets(
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames ) const
    {
        const auto population_name =
            JSON::get_value<std::string>( _cmd, "population_name_" );
        const auto population_df =
            utils::Getter::get( population_name, _data_frames );
        return population_df.num_targets();
    }

    // TODO: This needs to be implemented more consistently.
    /// Whether the pipeline is used for classification problems
    bool is_classification() const
    {
        const auto is_cl =
            []( const std::shared_ptr<
                featureengineerers::AbstractFeatureEngineerer>& fe ) {
                assert_true( fe );
                return fe->is_classification();
            };
        return std::all_of(
            feature_engineerers_.begin(), feature_engineerers_.end(), is_cl );
    }

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

    /// Trivial accessor
    size_t& num_targets() { return impl_.num_targets_; }

    /// Trivial (const) accessor
    size_t num_targets() const { return impl_.num_targets_; }

    /// Trivial (const) accessor
    Poco::JSON::Object& obj() { return impl_.obj_; }

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
    const predictors::PredictorImpl& predictor_impl() const
    {
        throw_unless( impl_.predictor_impl_, "Pipeline has not been fitted." );
        return *impl_.predictor_impl_;
    }

    /// Trivial accessor
    metrics::Scores& scores() { return impl_.scores_; }

    // --------------------------------------------------------

   private:
    /// The feature engineerers used in this pipeline.
    std::vector<std::shared_ptr<featureengineerers::AbstractFeatureEngineerer>>
        feature_engineerers_;

    // TODO: Implement feature selectors
    /// The feature selectors used in this pipeline (every target has its own
    /// set of feature selectors).
    std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>
        feature_selectors_;

    /// Impl for the pipeline
    PipelineImpl impl_;

    /// The predictors used in this pipeline (every target has its own set of
    /// predictors).
    std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>
        predictors_;

    // -----------------------------------------------
};

//  ----------------------------------------------------------------------------

}  // namespace pipelines
}  // namespace engine

// ----------------------------------------------------------------------------
#endif  // ENGINE_PIPELINES_PIPELINE_HPP_
