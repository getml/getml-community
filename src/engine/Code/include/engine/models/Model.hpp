#ifndef ENGINE_MODELS_MODEL_HPP_
#define ENGINE_MODELS_MODEL_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace models
{
// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
class Model : public AbstractModel
{
    // --------------------------------------------------------

   public:
    Model(
        const FeatureEngineererType& _feature_engineerer,
        const Poco::JSON::Object& _hyperparameters );

    Model(
        const std::shared_ptr<const std::vector<std::string>>& _encoding,
        const std::string& _path );

    ~Model() = default;

    // --------------------------------------------------------

   public:
    /// Fits the model.
    void fit(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        Poco::Net::StreamSocket* _socket ) final;

    /// Save the model.
    void save( const std::string& _path ) const final;

    /// Score predictions.
    Poco::JSON::Object score(
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket ) final;

    /// Generate features.
    containers::Features transform(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        Poco::Net::StreamSocket* _socket ) final;

    // --------------------------------------------------------

   public:
    /// Returns model as JSON Object.
    Poco::JSON::Object to_json_obj() const final
    {
        return feature_engineerer().to_json_obj();
    }

    /// Returns model as JSON Object in a form that the monitor can understand.
    Poco::JSON::Object to_monitor( const std::string& _name ) const final
    {
        auto obj = feature_engineerer().to_monitor( _name );
        obj.set( "scores_", scores_.to_json_obj() );
        return obj;
    }

    /// Return feature engineerer as SQL code.
    std::string to_sql() const final { return feature_engineerer().to_sql(); }

    // --------------------------------------------------------

   private:
    /// Add all discrete and numerical columns in the population table that
    /// haven't been explicitly marked "comparison only".
    void add_population_cols(
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        containers::Features* _features ) const;

    /// Calculates the correlations of each feature with the targets.
    void calculate_feature_stats(
        const containers::Features _features,
        const size_t _nrows,
        const size_t _ncols,
        const typename FeatureEngineererType::DataFrameType& _df );

    /// Extract a data frame of type FeatureEngineererType::DataFrameType from
    /// an engine::containers::DataFrame.
    template <typename DataFrameType>
    DataFrameType extract_df(
        const std::string& _name,
        const std::map<std::string, containers::DataFrame>& _data_frames );

    /// Calculates the feature importances.
    Poco::JSON::Object feature_importances();

    /// Fit the predictors.
    void fit(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        std::vector<std::shared_ptr<predictors::Predictor>>* _predictors,
        Poco::Net::StreamSocket* _socket );

    /// Gets the categorical columns in the population table that are to be
    /// included in the predictor.
    containers::CategoricalFeatures get_categorical_features(
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames )
        const;

    /// Initialize feature_selectors before fitting.
    void init_feature_selectors(
        const size_t _num_targets,
        std::vector<std::shared_ptr<predictors::Predictor>>*
            _feature_selectors ) const;

    /// Initialize predictors before fitting.
    void init_predictors(
        const size_t _num_targets,
        std::vector<std::shared_ptr<predictors::Predictor>>* _predictors )
        const;

    /// Helper function for loading a json object.
    static Poco::JSON::Object load_json_obj( const std::string& _fname );

    /// Makes an array of colnames to be used for scoring.
    std::vector<std::string> make_feature_names() const;

    /// Gets the categorical, numerical and discrete colnames from the
    /// population table that haven't been marked "comparison only" and stores
    /// them in a PredictorImpl object.
    void make_predictor_impl(
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames );

    /// Helper function for saving a json object.
    void save_json_obj(
        const Poco::JSON::Object& _obj, const std::string& _fname ) const;

    /// Undertakes the feature selection, if applicable.
    void select_features(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        Poco::Net::StreamSocket* _socket );

    // --------------------------------------------------------

   private:
    /// Trivial (private) getter
    FeatureEngineererType& feature_engineerer() { return feature_engineerer_; }

    /// Trivial (private) getter
    const FeatureEngineererType& feature_engineerer() const
    {
        return feature_engineerer_;
    }

    /// Trivial (private) getter
    size_t num_predictors() const { return predictors_.size(); }

    /// Trivial (private) getter
    std::shared_ptr<predictors::Predictor>& predictor( const size_t _i )
    {
        assert( _i < predictors_.size() );
        assert( predictors_[_i] );
        return predictors_[_i];
    }

    /// Trivial (private) getter
    const std::shared_ptr<const predictors::Predictor> predictor(
        const size_t _i ) const
    {
        assert( _i < predictors_.size() );
        assert( predictors_[_i] );
        return predictors_[_i];
    }

    /// Trivial (private) accessor
    predictors::PredictorImpl& predictor_impl()
    {
        assert( predictor_impl_ );
        return *predictor_impl_;
    }

    /// Trivial (private) accessor
    const predictors::PredictorImpl& predictor_impl() const
    {
        assert( predictor_impl_ );
        return *predictor_impl_;
    }

    /// Trivial (private) getter
    metrics::Scores& scores() { return scores_; }

    /// Trivial (private) getter
    const metrics::Scores& scores() const { return scores_; }

    // --------------------------------------------------------

   private:
    /// The underlying feature engineering algorithm.
    FeatureEngineererType feature_engineerer_;

    /// The hyperparameters used for fitting.
    Poco::JSON::Object predictor_hyperparameters_;

    /// Pimpl for the predictors.
    std::shared_ptr<predictors::PredictorImpl> predictor_impl_;

    /// The algorithm used for prediction (one for every target).
    std::vector<std::shared_ptr<predictors::Predictor>> predictors_;

    /// The scores used to evaluate this model.
    metrics::Scores scores_;

    // --------------------------------------------------------
};  // namespace models

// ----------------------------------------------------------------------------

}  // namespace models
}  // namespace engine

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

namespace engine
{
namespace models
{
// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
Model<FeatureEngineererType>::Model(
    const FeatureEngineererType& _feature_engineerer,
    const Poco::JSON::Object& _hyperparameters )
    : feature_engineerer_( _feature_engineerer )
{
    if ( _hyperparameters.has( "feature_selector_" ) &&
         !_hyperparameters.isNull( "feature_selector_" ) )
        {
            predictor_hyperparameters_.set(
                "feature_selector_",
                JSON::get_object( _hyperparameters, "feature_selector_" ) );
        }

    if ( _hyperparameters.has( "predictor_" ) &&
         !_hyperparameters.isNull( "predictor_" ) )
        {
            predictor_hyperparameters_.set(
                "predictor_",
                JSON::get_object( _hyperparameters, "predictor_" ) );
        }
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
Model<FeatureEngineererType>::Model(
    const std::shared_ptr<const std::vector<std::string>>& _encoding,
    const std::string& _path )
    : feature_engineerer_(
          _encoding, load_json_obj( _path + "feature_engineerer.json" ) ),
      predictor_hyperparameters_(
          load_json_obj( _path + "predictor_hyperparameters.json" ) ),
      predictor_impl_( std::make_shared<predictors::PredictorImpl>(
          load_json_obj( _path + "impl.json" ) ) ),
      scores_( load_json_obj( _path + "scores.json" ) )

{
    // ------------------------------------------------------------
    // Initialize predictors

    size_t num_predictors = 0;

    while ( true )
        {
            auto name = _path + "predictor-" + std::to_string( num_predictors );

            if ( !Poco::File( name ).exists() )
                {
                    break;
                }

            ++num_predictors;
        }

    init_predictors( num_predictors, &predictors_ );

    // ------------------------------------------------------------
    // Load predictors

    for ( size_t i = 0; i < num_predictors; ++i )
        {
            auto name = _path + "predictor-" + std::to_string( i );

            predictor( i )->load( name );
        }

    // ------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
void Model<FeatureEngineererType>::add_population_cols(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    containers::Features* _features ) const
{
    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    for ( const auto& col : predictor_impl().discrete_colnames() )
        {
            _features->push_back( population_df.discrete( col ).data_ptr() );
        }

    for ( const auto& col : predictor_impl().numerical_colnames() )
        {
            _features->push_back( population_df.numerical( col ).data_ptr() );
        }
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
void Model<FeatureEngineererType>::calculate_feature_stats(
    const containers::Features _features,
    const size_t _nrows,
    const size_t _ncols,
    const typename FeatureEngineererType::DataFrameType& _df )
{
    const size_t num_bins = 50;

    std::vector<const Float*> targets;

    for ( size_t j = 0; j < _df.num_targets(); ++j )
        {
            targets.push_back( _df.target_col( j ).begin() );
        }

    scores_.from_json_obj( metrics::Summarizer::calculate_feature_correlations(
        _features, _nrows, _ncols, targets ) );

    scores_.from_json_obj( metrics::Summarizer::calculate_feature_plots(
        _features, _nrows, _ncols, num_bins, targets ) );
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
template <typename DataFrameType>
DataFrameType Model<FeatureEngineererType>::extract_df(
    const std::string& _name,
    const std::map<std::string, containers::DataFrame>& _data_frames )
{
    // ------------------------------------------------------------------------

    const auto df = utils::Getter::get( _name, _data_frames );

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::IntColumnType> categoricals;

    for ( size_t i = 0; i < df.num_categoricals(); ++i )
        {
            const auto& mat = df.categorical( i );

            categoricals.push_back( typename DataFrameType::IntColumnType(
                mat.data(), mat.name(), mat.nrows(), mat.unit() ) );
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::FloatColumnType> discretes;

    for ( size_t i = 0; i < df.num_discretes(); ++i )
        {
            const auto& mat = df.discrete( i );

            discretes.push_back( typename DataFrameType::FloatColumnType(
                mat.data(), mat.name(), mat.nrows(), mat.unit() ) );
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::IntColumnType> join_keys;

    for ( size_t i = 0; i < df.num_join_keys(); ++i )
        {
            const auto& mat = df.join_key( i );

            join_keys.push_back( typename DataFrameType::IntColumnType(
                mat.data(), mat.name(), mat.nrows(), mat.unit() ) );
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::FloatColumnType> numericals;

    for ( size_t i = 0; i < df.num_numericals(); ++i )
        {
            const auto& mat = df.numerical( i );

            numericals.push_back( typename DataFrameType::FloatColumnType(
                mat.data(), mat.name(), mat.nrows(), mat.unit() ) );
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::FloatColumnType> targets;

    for ( size_t i = 0; i < df.num_targets(); ++i )
        {
            const auto& mat = df.target( i );

            targets.push_back( typename DataFrameType::FloatColumnType(
                mat.data(), mat.name(), mat.nrows(), mat.unit() ) );
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::FloatColumnType> time_stamps;

    for ( size_t i = 0; i < df.num_time_stamps(); ++i )
        {
            const auto& mat = df.time_stamp( i );

            time_stamps.push_back( typename DataFrameType::FloatColumnType(
                mat.data(), mat.name(), mat.nrows(), mat.unit() ) );
        }

    // ------------------------------------------------------------------------

    return DataFrameType(
        categoricals,
        discretes,
        df.maps(),
        join_keys,
        _name,
        numericals,
        targets,
        time_stamps );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
Poco::JSON::Object Model<FeatureEngineererType>::feature_importances()
{
    // ----------------------------------------------------------------

    const auto num_features =
        feature_engineerer().num_features() + predictor_impl().num_columns();

    // ----------------------------------------------------------------
    // Extract feature importances

    std::vector<std::vector<Float>> feature_importances_transposed;

    for ( size_t i = 0; i < num_predictors(); ++i )
        {
            feature_importances_transposed.push_back(
                predictor( i )->feature_importances( num_features ) );
        }

    // ----------------------------------------------------------------
    // Transpose feature importances and transform to arrays.

    Poco::JSON::Array::Ptr feature_importances( new Poco::JSON::Array() );

    if ( feature_importances_transposed.size() == 0 )
        {
            return Poco::JSON::Object();
        }

    for ( std::size_t i = 0; i < num_features; ++i )
        {
            Poco::JSON::Array::Ptr temp( new Poco::JSON::Array() );

            for ( auto& feat : feature_importances_transposed )
                {
                    assert( feat.size() == num_features );
                    temp->add( feat[i] );
                }

            feature_importances->add( temp );
        }

    // ----------------------------------------------------------------
    // Insert array into object and return.

    Poco::JSON::Object obj;

    obj.set( "feature_importances_", feature_importances );

    return obj;

    // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
void Model<FeatureEngineererType>::fit(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const monitoring::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------
    // Extract the peripheral tables.

    auto peripheral_names = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "peripheral_names_" ) );

    std::vector<typename FeatureEngineererType::DataFrameType>
        peripheral_tables = {};

    for ( auto& name : peripheral_names )
        {
            const auto df =
                extract_df<typename FeatureEngineererType::DataFrameType>(
                    name, _data_frames );

            peripheral_tables.push_back( df );
        }

    // ------------------------------------------------
    // Extract the population table.

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_table =
        extract_df<typename FeatureEngineererType::DataFrameType>(
            population_name, _data_frames );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    // ------------------------------------------------
    // Fit the feature engineerer.

    feature_engineerer().fit( population_table, peripheral_tables, _logger );

    // ------------------------------------------------
    // Figure out which categorical, numerical and discrete columns in the
    // population table should be used as additional features.

    make_predictor_impl( _cmd, _data_frames );

    // ------------------------------------------------
    // Do feature selection, if applicable.

    select_features( _cmd, _logger, _data_frames, _socket );

    // ------------------------------------------------
    // Fit the predictors, if applicable.

    init_predictors( population_df.num_targets(), &predictors_ );

    fit( _cmd, _logger, _data_frames, &predictors_, _socket );

    // ------------------------------------------------
    // Set the feature names.

    scores_.feature_names() = make_feature_names();

    // ------------------------------------------------
    // Get the feature importances, if applicable.

    scores_.from_json_obj( feature_importances() );

    // ------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
void Model<FeatureEngineererType>::fit(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const monitoring::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    std::vector<std::shared_ptr<predictors::Predictor>>* _predictors,
    Poco::Net::StreamSocket* _socket )
{
    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    if ( _predictors->size() > 0 )
        {
            assert( _predictors->size() == population_df.num_targets() );

            auto categorical_features =
                get_categorical_features( _cmd, _data_frames );

            predictor_impl().fit_encodings( categorical_features );

            categorical_features =
                predictor_impl().transform_encodings( categorical_features );

            auto numerical_features =
                transform( _cmd, _logger, _data_frames, _socket );

            for ( size_t i = 0; i < _predictors->size(); ++i )
                {
                    ( *_predictors )[i]->fit(
                        _logger,
                        categorical_features,
                        numerical_features,
                        population_df.target( i ).data_ptr() );
                }
        }
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
containers::CategoricalFeatures
Model<FeatureEngineererType>::get_categorical_features(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames ) const
{
    auto categorical_features = containers::CategoricalFeatures();

    if ( !feature_engineerer().hyperparameters().include_categorical_ )
        {
            return categorical_features;
        }

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    for ( const auto& col : predictor_impl().categorical_colnames() )
        {
            categorical_features.push_back(
                population_df.categorical( col ).data_ptr() );
        }

    return categorical_features;
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
void Model<FeatureEngineererType>::init_feature_selectors(
    const size_t _num_targets,
    std::vector<std::shared_ptr<predictors::Predictor>>* _feature_selectors )
    const
{
    _feature_selectors->clear();

    if ( !predictor_hyperparameters_.has( "feature_selector_" ) ||
         predictor_hyperparameters_.isNull( "feature_selector_" ) )
        {
            return;
        }

    const auto obj =
        *JSON::get_object( predictor_hyperparameters_, "feature_selector_" );

    assert( predictor_impl_ );

    for ( size_t i = 0; i < _num_targets; ++i )
        {
            _feature_selectors->push_back(
                predictors::PredictorParser::parse( obj, predictor_impl_ ) );
        }
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
void Model<FeatureEngineererType>::init_predictors(
    const size_t _num_targets,
    std::vector<std::shared_ptr<predictors::Predictor>>* _predictors ) const
{
    _predictors->clear();

    if ( !predictor_hyperparameters_.has( "predictor_" ) ||
         predictor_hyperparameters_.isNull( "predictor_" ) )
        {
            return;
        }

    const auto obj =
        *JSON::get_object( predictor_hyperparameters_, "predictor_" );

    assert( predictor_impl_ );

    for ( size_t i = 0; i < _num_targets; ++i )
        {
            _predictors->push_back(
                predictors::PredictorParser::parse( obj, predictor_impl_ ) );
        }
}

// ------------------------------------------------------------------------

template <typename FeatureEngineererType>
Poco::JSON::Object Model<FeatureEngineererType>::load_json_obj(
    const std::string& _fname )
{
    std::ifstream input( _fname );

    std::stringstream json;

    std::string line;

    if ( input.is_open() )
        {
            while ( std::getline( input, line ) )
                {
                    json << line;
                }

            input.close();
        }
    else
        {
            throw std::invalid_argument( "File '" + _fname + "' not found!" );
        }

    return *Poco::JSON::Parser()
                .parse( json.str() )
                .extract<Poco::JSON::Object::Ptr>();
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
std::vector<std::string> Model<FeatureEngineererType>::make_feature_names()
    const
{
    std::vector<std::string> feature_names;

    for ( size_t i = 0; i < feature_engineerer().num_features(); ++i )
        {
            feature_names.push_back( "FEATURE_" + std::to_string( i + 1 ) );
        }

    for ( const auto& col : predictor_impl().discrete_colnames() )
        {
            feature_names.push_back( col );
        }

    for ( const auto& col : predictor_impl().numerical_colnames() )
        {
            feature_names.push_back( col );
        }

    return feature_names;
}

// ------------------------------------------------------------------------

template <typename FeatureEngineererType>
void Model<FeatureEngineererType>::make_predictor_impl(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames )
{
    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    auto categorical_colnames = std::vector<std::string>();

    for ( size_t i = 0; i < population_df.num_categoricals(); ++i )
        {
            if ( population_df.categorical( i ).unit().find(
                     "comparison only" ) != std::string::npos )
                {
                    continue;
                }

            categorical_colnames.push_back(
                population_df.categorical( i ).name() );
        }

    auto discrete_colnames = std::vector<std::string>();

    for ( size_t i = 0; i < population_df.num_discretes(); ++i )
        {
            if ( population_df.discrete( i ).unit().find( "comparison only" ) !=
                 std::string::npos )
                {
                    continue;
                }

            discrete_colnames.push_back( population_df.discrete( i ).name() );
        }

    auto numerical_colnames = std::vector<std::string>();

    for ( size_t i = 0; i < population_df.num_numericals(); ++i )
        {
            if ( population_df.numerical( i ).unit().find(
                     "comparison only" ) != std::string::npos )
                {
                    continue;
                }

            numerical_colnames.push_back( population_df.numerical( i ).name() );
        }

    predictor_impl_ = std::make_shared<predictors::PredictorImpl>(
        categorical_colnames, discrete_colnames, numerical_colnames );
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
void Model<FeatureEngineererType>::save( const std::string& _path ) const
{
    auto file = Poco::File( _path );

    if ( file.exists() )
        {
            file.remove( true );
        }

    file.createDirectories();

    feature_engineerer().save( _path + "feature_engineerer.json" );

    scores().save( _path + "scores.json" );

    save_json_obj(
        predictor_hyperparameters_, _path + "predictor_hyperparameters.json" );

    predictor_impl().save( _path + "impl.json" );

    for ( size_t i = 0; i < num_predictors(); ++i )
        {
            predictor( i )->save( _path + "predictor-" + std::to_string( i ) );
        }
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
void Model<FeatureEngineererType>::save_json_obj(
    const Poco::JSON::Object& _obj, const std::string& _fname ) const
{
    std::ofstream fs( _fname, std::ofstream::out );

    Poco::JSON::Stringifier::stringify( _obj, fs );

    fs.close();
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
Poco::JSON::Object Model<FeatureEngineererType>::score(
    const Poco::JSON::Object& _cmd, Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------
    // Get predictions

    debug_log( "Getting predictions..." );

    auto yhat = communication::Receiver::recv_features( _socket );

    // ------------------------------------------------
    // Get the target data

    debug_log( "Getting targets..." );

    auto y = communication::Receiver::recv_features( _socket );

    // ------------------------------------------------
    // Make sure input is plausible

    if ( yhat.size() != y.size() )
        {
            throw std::invalid_argument(
                "Number of columns in predictions and targets do not "
                "match! "
                "Number of columns in predictions: " +
                std::to_string( yhat.size() ) +
                ". Number of columns in targets: " +
                std::to_string( y.size() ) + "." );
        }

    for ( size_t i = 0; i < y.size(); ++i )
        {
            if ( yhat[i]->size() != y[i]->size() )
                {
                    throw std::invalid_argument(
                        "Number of rows in predictions and targets do not "
                        "match! "
                        "Number of rows in predictions: " +
                        std::to_string( yhat[i]->size() ) +
                        ". Number of rows in targets: " +
                        std::to_string( y[i]->size() ) + "." );
                }
        }

    // ------------------------------------------------
    // Calculate the score

    debug_log( "Calculating score..." );

    auto obj = metrics::Scorer::score(
        feature_engineerer().is_classification(), yhat, y );

    scores_.from_json_obj( obj );

    // ------------------------------------------------

    return metrics::Scorer::get_metrics( obj );

    // ------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
void Model<FeatureEngineererType>::select_features(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const monitoring::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------------------------------
    // Plausibility checks

    if ( !predictor_hyperparameters_.has( "feature_selector_" ) ||
         predictor_hyperparameters_.isNull( "feature_selector_" ) )
        {
            return;
        }

    if ( feature_engineerer().hyperparameters().num_selected_features_ <= 0 )
        {
            throw std::invalid_argument(
                "Number of features must be positive!" );
        }

    // ------------------------------------------------------------------------
    // Initialize feature selectors.

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    std::vector<std::shared_ptr<predictors::Predictor>> feature_selectors;

    init_feature_selectors( population_df.num_targets(), &feature_selectors );

    // ------------------------------------------------------------------------
    // Fit feature selectors.

    fit( _cmd, _logger, _data_frames, &feature_selectors, _socket );

    // ------------------------------------------------------------------------
    // Calculate sum of feature importances.

    const auto num_features =
        feature_engineerer().num_features() + predictor_impl().num_columns();

    std::vector<Float> feature_importances( num_features );

    for ( auto& fs : feature_selectors )
        {
            auto temp = fs->feature_importances( num_features );

            std::transform(
                temp.begin(),
                temp.end(),
                feature_importances.begin(),
                feature_importances.begin(),
                std::plus<Float>() );
        }

    // ------------------------------------------------------------------------
    // Build index and sort by sum of feature importances, in descending
    // order.

    std::vector<size_t> index( num_features );

    for ( size_t ix = 0; ix < index.size(); ++ix )
        {
            index[ix] = ix;
        }

    std::sort(
        index.begin(),
        index.end(),
        [feature_importances]( const size_t& ix1, const size_t& ix2 ) {
            return feature_importances[ix1] > feature_importances[ix2];
        } );

    // ------------------------------------------------------------------------
    // Select the categorical, discrete and numerical columns that have made the
    // cut.

    const auto n_selected =
        ( feature_engineerer().hyperparameters().num_selected_features_ > 0 &&
          feature_engineerer().hyperparameters().num_selected_features_ <
              index.size() )
            ? ( feature_engineerer().hyperparameters().num_selected_features_ )
            : ( index.size() );

    predictor_impl().select_cols(
        n_selected, feature_engineerer().num_features(), index );

    // ------------------------------------------------------------------------
    // Tell the feature engineerer to select these features.

    feature_engineerer().select_features( index );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
containers::Features Model<FeatureEngineererType>::transform(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const monitoring::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------------------------
    // Extract the peripheral tables

    auto peripheral_names = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "peripheral_names_" ) );

    std::vector<typename FeatureEngineererType::DataFrameType>
        peripheral_tables = {};

    for ( auto& name : peripheral_names )
        {
            const auto df =
                extract_df<typename FeatureEngineererType::DataFrameType>(
                    name, _data_frames );

            peripheral_tables.push_back( df );
        }

    // -------------------------------------------------------------------------
    // Extract the population table

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_table =
        extract_df<typename FeatureEngineererType::DataFrameType>(
            population_name, _data_frames );

    // -------------------------------------------------------------------------
    // Generate the features.

    auto numerical_features = feature_engineerer().transform(
        population_table, peripheral_tables, _logger );

    // -------------------------------------------------------------------------
    // Add the discrete and numerical columns from the population table.

    add_population_cols( _cmd, _data_frames, &numerical_features );

    // -------------------------------------------------------------------------
    // If we do not want to score or predict, then we can stop here.

    const bool score =
        _cmd.has( "score_" ) && JSON::get_value<bool>( _cmd, "score_" );

    const bool predict =
        _cmd.has( "predict_" ) && JSON::get_value<bool>( _cmd, "predict_" );

    if ( !score && !predict )
        {
            return numerical_features;
        }

    // -------------------------------------------------------------------------
    // Retrieve the categorical features.

    auto categorical_features = get_categorical_features( _cmd, _data_frames );

    categorical_features =
        predictor_impl().transform_encodings( categorical_features );

    // -------------------------------------------------------------------------
    // Get the feature importances, if applicable.

    const auto ncols = numerical_features.size();

    if ( score && ncols > 0 )
        {
            const auto nrows = numerical_features[0]->size();

            calculate_feature_stats(
                numerical_features, nrows, ncols, population_table );
        }

    // -------------------------------------------------------------------------
    // Generate predictions, if applicable.

    if ( predict && num_predictors() > 0 )
        {
            auto predictions = containers::Features();

            for ( size_t i = 0; i < num_predictors(); ++i )
                {
                    predictions.push_back( predictor( i )->predict(
                        categorical_features, numerical_features ) );
                }

            return predictions;
        }
    else
        {
            return numerical_features;
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace models
}  // namespace engine

#endif  // ENGINE_MODELS_MODEL_HPP_
