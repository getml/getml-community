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
    constexpr static bool premium_only_ = FeatureEngineererType::premium_only_;

    // --------------------------------------------------------

   public:
    Model(
        const FeatureEngineererType& _feature_engineerer,
        const Poco::JSON::Object& _hyperparameters );

    Model(
        const std::shared_ptr<const std::vector<strings::String>>& _encoding,
        const std::string& _path );

    ~Model() = default;

    // --------------------------------------------------------

   public:
    /// Returns the feature names.
    std::tuple<
        std::vector<std::string>,
        std::vector<std::string>,
        std::vector<std::string>,
        std::vector<std::string>>
    feature_names() const;

    /// Fits the model.
    void fit(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        Poco::Net::StreamSocket* _socket ) final;

    /// Save the model.
    void save( const std::string& _path, const std::string& _name ) const final;

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
    /// Trivial accessor
    bool& allow_http() { return feature_engineerer().allow_http(); }

    /// Trivial accessor
    bool allow_http() const { return feature_engineerer().allow_http(); }

    /// Returns model as JSON Object.
    Poco::JSON::Object to_json_obj(
        const bool _schema_only = false ) const final
    {
        return feature_engineerer().to_json_obj( _schema_only );
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

    /// Trivial (const) getter.
    const metrics::Scores& scores() const { return scores_; }

    /// Trivial (const) getter.
    const std::string& session_name() const
    {
        return feature_engineerer().hyperparameters().session_name_;
    }

    /// Returns the feature names.
    const std::vector<std::string>& target_names() const
    {
        return feature_engineerer().population_schema().targets();
    }

    // --------------------------------------------------------

   private:
    /// Add all discrete and numerical columns in the population table that
    /// haven't been explicitly marked "comparison only".
    void add_population_cols(
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        containers::Features* _features ) const;

    /// Whether we allow null values in the features passed to the predictor.
    bool allow_null_values() const;

    /// Calculates the correlations of each feature with the targets.
    void calculate_feature_stats(
        const containers::Features _features,
        const size_t _nrows,
        const size_t _ncols,
        const typename FeatureEngineererType::DataFrameType& _df );

    /// Makes an array of colnames to be used for scoring.
    std::vector<std::string> concat_feature_names() const;

    /// Extract a data frame of type FeatureEngineererType::DataFrameType from
    /// an engine::containers::DataFrame.
    template <typename DataFrameType>
    DataFrameType extract_df(
        const std::string& _name,
        const std::map<std::string, containers::DataFrame>& _data_frames );

    /// Extract a data frame of type FeatureEngineererType::DataFrameType from
    /// an engine::containers::DataFrame using the pre-stored schema.
    template <typename DataFrameType, typename SchemaType>
    DataFrameType extract_df_by_colnames(
        const std::string& _name,
        const SchemaType& _schema,
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
        assert_true( _i < predictors_.size() );
        assert_true( predictors_[_i] );
        return predictors_[_i];
    }

    /// Trivial (private) getter
    const std::shared_ptr<const predictors::Predictor> predictor(
        const size_t _i ) const
    {
        assert_true( _i < predictors_.size() );
        assert_true( predictors_[_i] );
        return predictors_[_i];
    }

    /// Trivial (private) accessor
    predictors::PredictorImpl& predictor_impl()
    {
        throw_unless( predictor_impl_, "Model has not been fitted." );
        return *predictor_impl_;
    }

    /// Trivial (private) accessor
    const predictors::PredictorImpl& predictor_impl() const
    {
        throw_unless( predictor_impl_, "Model has not been fitted." );
        return *predictor_impl_;
    }

    // --------------------------------------------------------

   private:
    /// The underlying feature engineering algorithm.
    FeatureEngineererType feature_engineerer_;

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
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
Model<FeatureEngineererType>::Model(
    const std::shared_ptr<const std::vector<strings::String>>& _encoding,
    const std::string& _path )
    : feature_engineerer_(
          _encoding, load_json_obj( _path + "feature_engineerer.json" ) ),
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
bool Model<FeatureEngineererType>::allow_null_values() const
{
    assert_true( predictor_impl_ );

    bool allow_null = true;

    if ( feature_engineerer().hyperparameters().feature_selector_ )
        {
            const auto obj =
                *feature_engineerer().hyperparameters().feature_selector_;

            allow_null =
                predictors::PredictorParser::parse( obj, predictor_impl_ )
                    ->accepts_null();
        }

    if ( !allow_null )
        {
            return false;
        }

    if ( feature_engineerer().hyperparameters().predictor_ )
        {
            const auto obj = *feature_engineerer().hyperparameters().predictor_;

            allow_null =
                predictors::PredictorParser::parse( obj, predictor_impl_ )
                    ->accepts_null();
        }

    return allow_null;
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
void Model<FeatureEngineererType>::calculate_feature_stats(
    const containers::Features _features,
    const size_t _nrows,
    const size_t _ncols,
    const typename FeatureEngineererType::DataFrameType& _df )
{
    const size_t num_bins = 200;

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
template <typename DataFrameType, typename SchemaType>
DataFrameType Model<FeatureEngineererType>::extract_df_by_colnames(
    const std::string& _name,
    const SchemaType& _schema,
    const std::map<std::string, containers::DataFrame>& _data_frames )
{
    // ------------------------------------------------------------------------

    const auto df = utils::Getter::get( _name, _data_frames );

    // ------------------------------------------------------------------------

    try
        {
            // ------------------------------------------------------------------------

            std::vector<typename DataFrameType::IntColumnType> categoricals;

            for ( size_t i = 0; i < _schema.num_categoricals(); ++i )
                {
                    const auto& name = _schema.categorical_name( i );

                    const auto& mat = df.categorical( name );

                    categoricals.push_back(
                        typename DataFrameType::IntColumnType(
                            mat.data(), name, mat.nrows(), mat.unit() ) );
                }

            // ------------------------------------------------------------------------

            std::vector<typename DataFrameType::FloatColumnType> discretes;

            for ( size_t i = 0; i < _schema.num_discretes(); ++i )
                {
                    const auto& name = _schema.discrete_name( i );

                    const auto& mat = df.discrete( name );

                    discretes.push_back(
                        typename DataFrameType::FloatColumnType(
                            mat.data(), name, mat.nrows(), mat.unit() ) );
                }

            // ------------------------------------------------------------------------

            std::vector<typename DataFrameType::IntColumnType> join_keys;

            for ( size_t i = 0; i < _schema.num_join_keys(); ++i )
                {
                    const auto& name = _schema.join_keys_name( i );

                    const auto& mat = df.join_key( name );

                    join_keys.push_back( typename DataFrameType::IntColumnType(
                        mat.data(), name, mat.nrows(), mat.unit() ) );
                }

            // ------------------------------------------------------------------------

            std::vector<typename DataFrameType::FloatColumnType> numericals;

            for ( size_t i = 0; i < _schema.num_numericals(); ++i )
                {
                    const auto& name = _schema.numerical_name( i );

                    const auto& mat = df.numerical( name );

                    numericals.push_back(
                        typename DataFrameType::FloatColumnType(
                            mat.data(), name, mat.nrows(), mat.unit() ) );
                }

            // ------------------------------------------------------------------------

            std::vector<typename DataFrameType::FloatColumnType> targets;

            for ( size_t i = 0; i < _schema.num_targets(); ++i )
                {
                    const auto& name = _schema.target_name( i );

                    if ( df.has_target( name ) )
                        {
                            const auto& mat = df.target( name );

                            targets.push_back(
                                typename DataFrameType::FloatColumnType(
                                    mat.data(),
                                    name,
                                    mat.nrows(),
                                    mat.unit() ) );
                        }
                }

            // ------------------------------------------------------------------------

            std::vector<typename DataFrameType::FloatColumnType> time_stamps;

            for ( size_t i = 0; i < _schema.num_time_stamps(); ++i )
                {
                    const auto& name = _schema.time_stamps_name( i );

                    const auto& mat = df.time_stamp( name );

                    time_stamps.push_back(
                        typename DataFrameType::FloatColumnType(
                            mat.data(), name, mat.nrows(), mat.unit() ) );
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
    catch ( std::exception& e )
        {
            throw std::invalid_argument(
                std::string( e.what() ) +
                " Is it possible that your peripheral tables are in the wrong "
                "order?" );
        }

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
                    assert_true( feat.size() == num_features );
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
std::tuple<
    std::vector<std::string>,
    std::vector<std::string>,
    std::vector<std::string>,
    std::vector<std::string>>
Model<FeatureEngineererType>::feature_names() const
{
    std::vector<std::string> autofeatures;

    for ( size_t i = 0; i < feature_engineerer().num_features(); ++i )
        {
            autofeatures.push_back( "feature_" + std::to_string( i + 1 ) );
        }

    if ( predictor_impl_ )
        {
            return std::make_tuple(
                autofeatures,
                predictor_impl().categorical_colnames(),
                predictor_impl().discrete_colnames(),
                predictor_impl().numerical_colnames() );
        }
    else
        {
            return std::make_tuple(
                autofeatures,
                std::vector<std::string>(),
                std::vector<std::string>(),
                std::vector<std::string>() );
        }
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

    scores_.feature_names() = concat_feature_names();

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
            auto categorical_features =
                get_categorical_features( _cmd, _data_frames );

            predictor_impl().fit_encodings( categorical_features );

            categorical_features =
                predictor_impl().transform_encodings( categorical_features );

            auto numerical_features =
                transform( _cmd, _logger, _data_frames, _socket );

            if constexpr ( FeatureEngineererType::supports_multiple_targets_ )
                {
                    assert_true(
                        _predictors->size() == population_df.num_targets() );

                    for ( size_t i = 0; i < _predictors->size(); ++i )
                        {
                            ( *_predictors )[i]->fit(
                                _logger,
                                categorical_features,
                                numerical_features,
                                population_df.target( i ).data_ptr() );
                        }
                }
            else
                {
                    throw_unless(
                        feature_engineerer().target_num() >= 0,
                        "target_num cannot be negative!" );

                    const auto target_num = static_cast<size_t>(
                        feature_engineerer().target_num() );

                    throw_unless(
                        target_num < population_df.num_targets(),
                        "target_num must be smaller than number of targets! "
                        "target_num: " +
                            std::to_string( target_num ) +
                            ", number of targets: " +
                            std::to_string( population_df.num_targets() ) +
                            "." );

                    assert_true( _predictors->size() == 1 );

                    ( *_predictors )[0]->fit(
                        _logger,
                        categorical_features,
                        numerical_features,
                        population_df.target( target_num ).data_ptr() );
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

    if ( !feature_engineerer().hyperparameters().feature_selector_ )
        {
            return;
        }

    const auto obj = *feature_engineerer().hyperparameters().feature_selector_;

    assert_true( predictor_impl_ );

    if constexpr ( FeatureEngineererType::supports_multiple_targets_ )
        {
            for ( size_t i = 0; i < _num_targets; ++i )
                {
                    _feature_selectors->push_back(
                        predictors::PredictorParser::parse(
                            obj, predictor_impl_ ) );
                }
        }
    else
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

    if ( !feature_engineerer().hyperparameters().predictor_ )
        {
            return;
        }

    const auto obj = *feature_engineerer().hyperparameters().predictor_;

    assert_true( predictor_impl_ );

    if constexpr ( FeatureEngineererType::supports_multiple_targets_ )
        {
            for ( size_t i = 0; i < _num_targets; ++i )
                {
                    _predictors->push_back( predictors::PredictorParser::parse(
                        obj, predictor_impl_ ) );
                }
        }
    else
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
std::vector<std::string> Model<FeatureEngineererType>::concat_feature_names()
    const
{
    std::vector<std::string> names;

    const auto [autofeatures, categorical, discrete, numerical] =
        feature_names();

    names.insert( names.end(), autofeatures.begin(), autofeatures.end() );

    names.insert( names.end(), discrete.begin(), discrete.end() );

    names.insert( names.end(), numerical.begin(), numerical.end() );

    if ( feature_engineerer().hyperparameters().include_categorical_ )
        {
            names.insert( names.end(), categorical.begin(), categorical.end() );
        }

    return names;
}

// ------------------------------------------------------------------------

template <typename FeatureEngineererType>
void Model<FeatureEngineererType>::make_predictor_impl(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames )
{
    // --------------------------------------------------------------------
    // Temporary impl, needed by allow_null_values.

    predictor_impl_ = std::make_shared<predictors::PredictorImpl>(
        std::vector<std::string>(),
        std::vector<std::string>(),
        std::vector<std::string>(),
        feature_engineerer().num_features() );

    // --------------------------------------------------------------------

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    const auto allow_null = allow_null_values();

    const auto is_null = []( const Float val ) {
        return ( std::isnan( val ) || std::isinf( val ) );
    };

    // --------------------------------------------------------------------

    auto categorical_colnames = std::vector<std::string>();

    if ( feature_engineerer().hyperparameters().include_categorical_ )
        {
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
        }

    // --------------------------------------------------------------------

    auto discrete_colnames = std::vector<std::string>();

    for ( size_t i = 0; i < population_df.num_discretes(); ++i )
        {
            if ( population_df.discrete( i ).unit().find( "comparison only" ) !=
                 std::string::npos )
                {
                    continue;
                }

            if ( !allow_null )
                {
                    const auto contains_null = std::any_of(
                        population_df.discrete( i ).begin(),
                        population_df.discrete( i ).end(),
                        is_null );

                    if ( contains_null )
                        {
                            continue;
                        }
                }

            discrete_colnames.push_back( population_df.discrete( i ).name() );
        }

    // --------------------------------------------------------------------

    auto numerical_colnames = std::vector<std::string>();

    for ( size_t i = 0; i < population_df.num_numericals(); ++i )
        {
            if ( population_df.numerical( i ).unit().find(
                     "comparison only" ) != std::string::npos )
                {
                    continue;
                }

            if ( !allow_null )
                {
                    const auto contains_null = std::any_of(
                        population_df.numerical( i ).begin(),
                        population_df.numerical( i ).end(),
                        is_null );

                    if ( contains_null )
                        {
                            continue;
                        }
                }

            numerical_colnames.push_back( population_df.numerical( i ).name() );
        }

    // --------------------------------------------------------------------

    predictor_impl_ = std::make_shared<predictors::PredictorImpl>(
        categorical_colnames,
        discrete_colnames,
        numerical_colnames,
        feature_engineerer().num_features() );

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------

template <typename FeatureEngineererType>
void Model<FeatureEngineererType>::save(
    const std::string& _path, const std::string& _name ) const
{
    auto tfile = Poco::TemporaryFile();

    tfile.createDirectories();

    feature_engineerer().save( tfile.path() + "/feature_engineerer.json" );

    scores().save( tfile.path() + "/scores.json" );

    predictor_impl().save( tfile.path() + "/impl.json" );

    for ( size_t i = 0; i < num_predictors(); ++i )
        {
            predictor( i )->save(
                tfile.path() + "/predictor-" + std::to_string( i ) );
        }

    auto file = Poco::File( _path + _name );

    if ( file.exists() )
        {
            file.remove( true );
        }

    tfile.renameTo( file.path() );

    tfile.keep();
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

    if ( !feature_engineerer().hyperparameters().feature_selector_ )
        {
            return;
        }

    if ( feature_engineerer().hyperparameters().num_selected_features() <= 0 )
        {
            throw std::invalid_argument(
                "Number of selected features must be positive!" );
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

    std::stable_sort(
        index.begin(),
        index.end(),
        [feature_importances]( const size_t& ix1, const size_t& ix2 ) {
            return feature_importances[ix1] > feature_importances[ix2];
        } );

    // ------------------------------------------------------------------------
    // Select the categorical, discrete and numerical columns that have made the
    // cut.

    const auto n_selected =
        ( feature_engineerer().hyperparameters().num_selected_features() > 0 &&
          feature_engineerer().hyperparameters().num_selected_features() <
              index.size() )
            ? ( feature_engineerer().hyperparameters().num_selected_features() )
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

    const auto peripheral_schema = feature_engineerer().peripheral_schema();

    const auto peripheral_names = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "peripheral_names_" ) );

    if ( peripheral_schema.size() != peripheral_names.size() )
        {
            throw std::invalid_argument(
                "Expected " + std::to_string( peripheral_schema.size() ) +
                " peripheral tables, got " +
                std::to_string( peripheral_names.size() ) + "." );
        }

    std::vector<typename FeatureEngineererType::DataFrameType>
        peripheral_tables = {};

    for ( size_t i = 0; i < peripheral_names.size(); ++i )
        {
            const auto df = extract_df_by_colnames<
                typename FeatureEngineererType::DataFrameType>(
                peripheral_names[i], peripheral_schema[i], _data_frames );

            peripheral_tables.push_back( df );
        }

    // -------------------------------------------------------------------------
    // Extract the population table

    const auto population_schema = feature_engineerer().population_schema();

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_table =
        extract_df_by_colnames<typename FeatureEngineererType::DataFrameType>(
            population_name, population_schema, _data_frames );

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
    // Get the feature correlations, if applicable.

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
