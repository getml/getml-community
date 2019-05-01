#ifndef ENGINE_HANDLERS_MODELS_HPP_
#define ENGINE_HANDLERS_MODELS_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

class Models
{
   public:
    /// Fits the model.
    template <typename ModelType>
    static void fit(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const logging::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        ModelType* _model,
        Poco::Net::StreamSocket* _socket );

    /// Score predictions.
    template <typename ModelType>
    static Poco::JSON::Object score(
        const Poco::JSON::Object& _cmd,
        ModelType* _model,
        Poco::Net::StreamSocket* _socket );

    /// Generate features.
    template <typename ModelType>
    static containers::Matrix<ENGINE_FLOAT> transform(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const logging::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        const ModelType& _model,
        Poco::Net::StreamSocket* _socket );

   private:
    /// Extract a data frame of type ModelType::DataFrameType from an
    /// engine::containers::DataFrame.
    template <typename DataFrameType>
    static DataFrameType extract_df(
        const std::string& _name,
        const std::map<std::string, containers::DataFrame>& _data_frames );
};

// ----------------------------------------------------------------------------

}  // namespace handlers
}  // namespace engine

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

template <typename DataFrameType>
DataFrameType Models::extract_df(
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

template <typename ModelType>
void Models::fit(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const logging::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    ModelType* _model,
    Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------
    // Extract the peripheral tables

    auto peripheral_names = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "peripheral_names_" ) );

    std::vector<typename ModelType::DataFrameType> peripheral_tables = {};

    for ( auto& name : peripheral_names )
        {
            const auto df = extract_df<typename ModelType::DataFrameType>(
                name, _data_frames );

            peripheral_tables.push_back( df );
        }

    // ------------------------------------------------
    // Extract the population table

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_table = extract_df<typename ModelType::DataFrameType>(
        population_name, _data_frames );

    // ------------------------------------------------
    // Do the actual fitting.

    _model->init( population_table, peripheral_tables );

    for ( size_t i = 0; i < _model->hyperparameters().num_features_; ++i )
        {
            _model->fit_new_feature();

            if ( !_model->hyperparameters().silent_ )
                {
                    _logger->log(
                        "Trained FEATURE_" +
                        std::to_string( _model->num_features() ) );
                }
        }

    _model->clean_up();

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

template <typename ModelType>
Poco::JSON::Object Models::score(
    const Poco::JSON::Object& _cmd,
    ModelType* _model,
    Poco::Net::StreamSocket* _socket )
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

    auto result = _model->score(
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

template <typename ModelType>
containers::Matrix<ENGINE_FLOAT> Models::transform(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const logging::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const ModelType& _model,
    Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------
    // Extract the peripheral tables

    auto peripheral_names = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "peripheral_names_" ) );

    std::vector<typename ModelType::DataFrameType> peripheral_tables = {};

    for ( auto& name : peripheral_names )
        {
            const auto df = extract_df<typename ModelType::DataFrameType>(
                name, _data_frames );

            peripheral_tables.push_back( df );
        }

    // ------------------------------------------------
    // Extract the population table

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_table = extract_df<typename ModelType::DataFrameType>(
        population_name, _data_frames );

    // ------------------------------------------------

    const bool predict = JSON::get_value<bool>( _cmd, "predict_" );

    // ------------------------------------------------------------------------
    // This is a temporary solution - the idea is for a DecisionTreeEnsemble
    // to have its own predictors.

    const auto data = std::make_shared<std::vector<RELBOOST_FLOAT>>( 0 );

    if ( predict )
        {
            *data = _model.predict( population_table, peripheral_tables );
        }
    else
        {
            *data = *_model.transform( population_table, peripheral_tables );
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
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_MODELMANAGER_HPP_
