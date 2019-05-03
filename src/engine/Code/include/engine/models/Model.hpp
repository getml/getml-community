#ifndef ENGINE_MODELS_MODEL_HPP_
#define ENGINE_MODELS_MODEL_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace models
{
// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
class Model : public ModelBase
{
    // --------------------------------------------------------

   public:
    Model( const FeatureEngineererType& _feature_engineerer )
        : feature_engineerer_( _feature_engineerer )
    {
    }

    ~Model() = default;

    // --------------------------------------------------------

   public:
    /// Fits the model.
    void fit(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const logging::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        Poco::Net::StreamSocket* _socket ) final;

    /// Score predictions.
    Poco::JSON::Object score(
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket ) final;

    /// Generate features.
    containers::Matrix<ENGINE_FLOAT> transform(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const logging::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        Poco::Net::StreamSocket* _socket ) final;

    // --------------------------------------------------------

   public:
    /// Save the model.
    void save( const std::string& _fname ) const final
    {
        return feature_engineerer().save( _fname );
    }

    /// Return model as JSON Object.
    Poco::JSON::Object to_json_obj() const final
    {
        return feature_engineerer().to_json_obj();
    }

    /// Return feature engineerer as SQL code.
    std::string to_sql() const final { return feature_engineerer().to_sql(); }

    // --------------------------------------------------------

   private:
    /// Extract a data frame of type FeatureEngineererType::DataFrameType from
    /// an engine::containers::DataFrame.
    template <typename DataFrameType>
    static DataFrameType extract_df(
        const std::string& _name,
        const std::map<std::string, containers::DataFrame>& _data_frames );

    /// Trivial (private getter)
    FeatureEngineererType& feature_engineerer() { return feature_engineerer_; }

    /// Trivial (private getter)
    const FeatureEngineererType& feature_engineerer() const
    {
        return feature_engineerer_;
    }

    // --------------------------------------------------------

   private:
    /// The underlying feature engineering algorithm.
    FeatureEngineererType feature_engineerer_;

    // --------------------------------------------------------
};

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
template <typename DataFrameType>
DataFrameType Model<FeatureEngineererType>::extract_df(
    const std::string& _name,
    const std::map<std::string, containers::DataFrame>& _data_frames )
{
    // ------------------------------------------------------------------------

    auto it = _data_frames.find( _name );

    if ( it == _data_frames.end() )
        {
            throw std::runtime_error(
                "No data frame called '" + _name +
                "' is currently loaded in memory!" );
        }

    const auto& df = it->second;

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::IntColumnType> categoricals;

    for ( size_t i = 0; i < df.num_categoricals(); ++i )
        {
            const auto& mat = it->second.categorical( i );

            categoricals.push_back( typename DataFrameType::IntColumnType(
                mat.data(), mat.colname( 0 ), mat.nrows(), mat.unit( 0 ) ) );
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::FloatColumnType> discretes;

    for ( size_t i = 0; i < df.num_discretes(); ++i )
        {
            const auto& mat = it->second.discrete( i );

            discretes.push_back( typename DataFrameType::FloatColumnType(
                mat.data(), mat.colname( 0 ), mat.nrows(), mat.unit( 0 ) ) );
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::IntColumnType> join_keys;

    for ( size_t i = 0; i < df.num_join_keys(); ++i )
        {
            const auto& mat = it->second.join_key( i );

            join_keys.push_back( typename DataFrameType::IntColumnType(
                mat.data(), mat.colname( 0 ), mat.nrows(), mat.unit( 0 ) ) );
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::FloatColumnType> numericals;

    for ( size_t i = 0; i < df.num_numericals(); ++i )
        {
            const auto& mat = it->second.numerical( i );

            numericals.push_back( typename DataFrameType::FloatColumnType(
                mat.data(), mat.colname( 0 ), mat.nrows(), mat.unit( 0 ) ) );
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::FloatColumnType> targets;

    for ( size_t i = 0; i < df.num_targets(); ++i )
        {
            const auto& mat = it->second.target( i );

            targets.push_back( typename DataFrameType::FloatColumnType(
                mat.data(), mat.colname( 0 ), mat.nrows(), mat.unit( 0 ) ) );
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::FloatColumnType> time_stamps;

    for ( size_t i = 0; i < df.num_time_stamps(); ++i )
        {
            const auto& mat = it->second.time_stamp( i );

            time_stamps.push_back( typename DataFrameType::FloatColumnType(
                mat.data(), mat.colname( 0 ), mat.nrows(), mat.unit( 0 ) ) );
        }

    // ------------------------------------------------------------------------

    return DataFrameType(
        categoricals,
        discretes,
        it->second.indices(),
        join_keys,
        _name,
        numericals,
        targets,
        time_stamps );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
void Model<FeatureEngineererType>::fit(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const logging::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------
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

    // ------------------------------------------------
    // Extract the population table

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_table =
        extract_df<typename FeatureEngineererType::DataFrameType>(
            population_name, _data_frames );

    // ------------------------------------------------
    // Do the actual fitting.

    feature_engineerer().init( population_table, peripheral_tables );

    for ( size_t i = 0;
          i < feature_engineerer().hyperparameters().num_features_;
          ++i )
        {
            feature_engineerer().fit_new_feature();

            if ( !feature_engineerer().hyperparameters().silent_ )
                {
                    _logger->log(
                        "Trained FEATURE_" +
                        std::to_string( feature_engineerer().num_features() ) );
                }
        }

    feature_engineerer().clean_up();

    // ------------------------------------------------
    // Do feature selection, if applicable

    /*if ( _model.has_feature_selectors() )
        {
            auto features = Models::transform(
                _socket,
                _cmd,
                _logger,
                _data_frames,
                _model,
                false,  // _score,
                false   ///_predict
            );

            msg += _model.select_features(
                _logger, features, population_table.targets() );
        }*/

    // ------------------------------------------------
    // Fit predictors, if applicable

    /* if ( _model.has_predictors() )
         {
             auto features = Models::transform(
                 _socket,
                 _cmd,
                 _logger,
                 _data_frames,
                 _model,
                 false,  // _score,
                 false   ///_predict
             );

             msg += _model.fit_predictors(
                 _logger, features, population_table.targets() );
         }*/

    // ------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
Poco::JSON::Object Model<FeatureEngineererType>::score(
    const Poco::JSON::Object& _cmd, Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------
    // Get predictions

    debug_log( "Getting predictions..." );

    auto yhat = communication::Receiver::recv_matrix( _socket );

    // ------------------------------------------------
    // Get the target data

    debug_log( "Getting targets..." );

    auto y = communication::Receiver::recv_matrix( _socket );

    // ------------------------------------------------
    // Make sure input is plausible

    if ( yhat.nrows() != y.nrows() )
        {
            throw std::invalid_argument(
                "Number of rows in predictions and targets do not match! "
                "Number of rows in predictions: " +
                std::to_string( yhat.nrows() ) +
                ". Number of rows in targets: " + std::to_string( y.nrows() ) +
                "." );
        }

    if ( yhat.ncols() != y.ncols() )
        {
            throw std::invalid_argument(
                "Number of columns in predictions and targets do not match! "
                "Number of columns in predictions: " +
                std::to_string( yhat.ncols() ) +
                ". Number of columns in targets: " +
                std::to_string( y.ncols() ) + "." );
        }

    // ------------------------------------------------
    // Calculate the score

    debug_log( "Calculating score..." );

    auto result = feature_engineerer().score(
        yhat.data(),
        yhat.nrows(),
        yhat.ncols(),
        y.data(),
        y.nrows(),
        y.ncols() );

    // ------------------------------------------------

    return result;

    // ------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename FeatureEngineererType>
containers::Matrix<ENGINE_FLOAT> Model<FeatureEngineererType>::transform(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const logging::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------
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

    // ------------------------------------------------
    // Extract the population table

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_table =
        extract_df<typename FeatureEngineererType::DataFrameType>(
            population_name, _data_frames );

    // ------------------------------------------------

    const bool predict = JSON::get_value<bool>( _cmd, "predict_" );

    // ------------------------------------------------------------------------
    // This is a temporary solution - the idea is for a DecisionTreeEnsemble
    // to have its own predictors.

    const auto data = std::make_shared<std::vector<RELBOOST_FLOAT>>( 0 );

    if ( predict )
        {
            *data = feature_engineerer().predict(
                population_table, peripheral_tables );
        }
    else
        {
            *data = *feature_engineerer().transform(
                population_table, peripheral_tables );
        }

    // ------------------------------------------------------------------------
    // Build matrix.

    assert( data->size() % population_table.nrows() == 0 );

    const auto nrows = population_table.nrows();

    const auto ncols = data->size() / population_table.nrows();

    const auto mat = containers::Matrix<ENGINE_FLOAT>( nrows, ncols, data );

    // ------------------------------------------------------------------------

    return mat;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace models
}  // namespace engine

#endif  // ENGINE_MODELS_MODEL_HPP_
