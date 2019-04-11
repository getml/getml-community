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

    // ------------------------------------------------------------------------

    const auto categorical = typename DataFrameType::IntMatrixType(
        *it->second.categorical().colnames(),
        it->second.categorical().data(),
        it->second.categorical().nrows(),
        *it->second.categorical().units() );

    // ------------------------------------------------------------------------

    const auto discrete = typename DataFrameType::FloatMatrixType(
        *it->second.discrete().colnames(),
        it->second.discrete().data(),
        it->second.discrete().nrows(),
        *it->second.discrete().units() );

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::IntMatrixType> join_keys;

    for ( auto& jk : it->second.join_keys() )
        {
            join_keys.push_back( typename DataFrameType::IntMatrixType(
                *jk.colnames(), jk.data(), jk.nrows(), *jk.units() ) );
        }

    // ------------------------------------------------------------------------

    const auto numerical = typename DataFrameType::FloatMatrixType(
        *it->second.numerical().colnames(),
        it->second.numerical().data(),
        it->second.numerical().nrows(),
        *it->second.numerical().units() );

    // ------------------------------------------------------------------------

    const auto target = typename DataFrameType::FloatMatrixType(
        *it->second.targets().colnames(),
        it->second.targets().data(),
        it->second.targets().nrows(),
        *it->second.targets().units() );

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::FloatMatrixType> time_stamps;

    for ( auto& ts : it->second.time_stamps_all() )
        {
            time_stamps.push_back( typename DataFrameType::FloatMatrixType(
                *ts.colnames(), ts.data(), ts.nrows(), *ts.units() ) );
        }

    // ------------------------------------------------------------------------

    return DataFrameType(
        categorical,
        discrete,
        it->second.indices(),
        join_keys,
        _name,
        numerical,
        target,
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

    _model->fit( population_table, peripheral_tables );

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
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_MODELMANAGER_HPP_
