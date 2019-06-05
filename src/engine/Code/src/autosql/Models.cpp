#include "engine/engine.hpp"

namespace autosql
{
namespace engine
{
// ------------------------------------------------------------------------

std::string Models::fit(
    Poco::Net::StreamSocket& _socket,
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const logging::Logger> _logger,
    std::map<std::string, containers::DataFrame>& _data_frames,
    decisiontrees::DecisionTreeEnsemble& _model )
{
    // ------------------------------------------------
    // Get the hyperparameters

    auto hyperparameters = descriptors::Hyperparameters( _cmd );

    // ------------------------------------------------
    // Extract the peripheral tables

    auto peripheral_names = JSON::array_to_vector<std::string>(
        _cmd.AUTOSQL_GET_ARRAY( "peripheral_names_" ) );

    std::vector<containers::DataFrame> peripheral_tables =
        Getter::get( _data_frames, peripheral_names );

    // ------------------------------------------------
    // Extract the population table

    std::string population_name = _cmd.AUTOSQL_GET( "population_name_" );

    containers::DataFrame population_table =
        Getter::get( _data_frames, population_name );

    // ------------------------------------------------
    // Rearrange tables - this is only necessary for the MPI version

#ifdef AUTOSQL_MULTINODE_MPI

    {
        auto rearranged = MPIutils::rearrange_tables_root(
            peripheral_tables, population_table, _model );

        peripheral_tables = rearranged.peripheral_tables;

        population_table = rearranged.population_table;
    }

#endif  // AUTOSQL_MULTINODE_MPI

    // ------------------------------------------------
    // Do the actual fitting

#ifdef AUTOSQL_MULTITHREADING

    auto num_threads = multithreading::Threadutils::get_num_threads(
        hyperparameters.num_threads );

    std::string msg = multithreading::Threadutils::fit(
        num_threads,
        _model,
        _logger,
        peripheral_tables,
        population_table,
        hyperparameters );

#else  // AUTOSQL_MULTITHREADING

    std::string msg = _model.fit(
        _logger,
        peripheral_tables,
        containers::DataFrameView( population_table ),
        hyperparameters );

#endif  // AUTOSQL_MULTITHREADING

    // ------------------------------------------------
    // Do feature selection, if applicable

    if ( _model.has_feature_selectors() )
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
        }

    // ------------------------------------------------
    // Fit predictors, if applicable

    if ( _model.has_predictors() )
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
        }

    // ------------------------------------------------

    return msg;
}

// ------------------------------------------------------------------------

#ifdef AUTOSQL_MULTINODE_MPI

void Models::fit(
    Poco::JSON::Object& _cmd,
    std::map<std::string, containers::DataFrame>& _data_frames,
    decisiontrees::DecisionTreeEnsemble& _model )
{
    // ------------------------------------------------
    // Get the hyperparameters

    auto hyperparameters = descriptors::Hyperparameters( _cmd );

    // ------------------------------------------------
    // Extract the peripheral tables

    std::vector<std::string> peripheral_names =
        Getter::get_names( _cmd, "peripheral_names_" );

    std::vector<containers::DataFrame> peripheral_tables =
        Getter::get( _data_frames, peripheral_names );

    // ------------------------------------------------
    // Extract the population table

    std::string population_name = _cmd.get<std::string>( "population_name_" );

    containers::DataFrame population_table =
        Getter::get( _data_frames, population_name );

    // ------------------------------------------------
    // Rearrange tables

    {
        auto rearranged = MPIutils::rearrange_tables(
            peripheral_tables, population_table, _model );

        peripheral_tables = rearranged.peripheral_tables;

        population_table = rearranged.population_table;
    }

    // ------------------------------------------------
    // Do the actual fitting

    _model.fit( peripheral_tables, population_table, hyperparameters );
}

#endif  // AUTOSQL_MULTINODE_MPI

// ------------------------------------------------------------------------

Poco::JSON::Object Models::score(
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket& _socket,
    decisiontrees::DecisionTreeEnsemble* _model )
{
    // ------------------------------------------------
    // Get predictions

    debug_message( "Getting predictions..." );

    auto yhat = Receiver::recv_matrix( _socket, true );

    // ------------------------------------------------
    // Get the target data

    debug_message( "Getting targets..." );

    auto y = Receiver::recv_matrix( _socket, true );

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

    debug_message( "Calculating score..." );

#ifdef AUTOSQL_MULTITHREADING

    auto num_threads = multithreading::Threadutils::get_num_threads(
        _cmd.AUTOSQL_GET( "num_threads_" ) );

    auto result =
        multithreading::Threadutils::score( num_threads, yhat, y, _model );

#else  // AUTOSQL_MULTITHREADING

    auto result = _model->score( yhat, y );

#endif  // AUTOSQL_MULTITHREADING

    // ------------------------------------------------

    return result;

    // ------------------------------------------------
}

// ------------------------------------------------------------------------

containers::Matrix<AUTOSQL_FLOAT> Models::transform(
    Poco::Net::StreamSocket& _socket,
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const logging::Logger> _logger,
    std::map<std::string, containers::DataFrame>& _data_frames,
    decisiontrees::DecisionTreeEnsemble& _model,
    bool _score,
    bool _predict )
{
    // ------------------------------------------------
    // Extract the peripheral tables

    auto peripheral_names = JSON::array_to_vector<std::string>(
        _cmd.AUTOSQL_GET_ARRAY( "peripheral_names_" ) );

    std::vector<containers::DataFrame> peripheral_tables =
        Getter::get( _data_frames, peripheral_names );

    // ------------------------------------------------
    // Extract the population table

    std::string population_name = _cmd.AUTOSQL_GET( "population_name_" );

    containers::DataFrame population_table =
        Getter::get( _data_frames, population_name );

    // ------------------------------------------------
    // Rearrange tables - this is only necessary for the MPI version

#ifdef AUTOSQL_MULTINODE_MPI

    auto rearranged = MPIutils::rearrange_tables_root(
        peripheral_tables, population_table, _model );

    peripheral_tables = rearranged.peripheral_tables;

    population_table = rearranged.population_table;

#endif  // AUTOSQL_MULTINODE_MPI

    // ------------------------------------------------
    // Do the actual transformation

#ifdef AUTOSQL_MULTITHREADING

    auto num_threads = multithreading::Threadutils::get_num_threads(
        _cmd.AUTOSQL_GET( "num_threads_" ) );

    auto yhat = multithreading::Threadutils::transform(
        num_threads,
        _model,
        _logger,
        peripheral_tables,
        population_table,
        _score );

#else  // AUTOSQL_MULTITHREADING

    auto yhat = _model.transform(
        _logger,
        peripheral_tables,
        containers::DataFrameView( population_table ),
        true,  // _transpose,
        _score );

#endif  // AUTOSQL_MULTITHREADING

    // ------------------------------------------------
    // Gather prediction at the root process

    {
#ifdef AUTOSQL_MULTINODE_MPI

        yhat = MPIutils::gather_matrix_by_key_root(
            yhat, rearranged.original_order );

#endif  // AUTOSQL_MULTINODE_MPI
    }

    // ------------------------------------------------
    // Generate predictions, if applicable

    if ( _predict && _model.has_fitted_predictors() )
        {
            yhat = _model.predict( yhat );
        }

    // ------------------------------------------------
    // Return features/predictions

    return yhat;
}

// ------------------------------------------------------------------------

#ifdef AUTOSQL_MULTINODE_MPI

void Models::transform(
    pt::ptree& _cmd,
    std::map<std::string, containers::DataFrame>& _data_frames,
    decisiontrees::DecisionTreeEnsemble& _model,
    bool _score )
{
    // ------------------------------------------------
    // Extract the peripheral tables

    std::vector<std::string> peripheral_names =
        Getter::get_names( _cmd, "peripheral_names_" );

    std::vector<containers::DataFrame> peripheral_tables =
        Getter::get( _data_frames, peripheral_names );

    // ------------------------------------------------
    // Extract the population table

    std::string population_name = _cmd.get<std::string>( "population_name_" );

    containers::DataFrame population_table =
        Getter::get( _data_frames, population_name );

    // ------------------------------------------------
    // Rearrange tables

    auto rearranged = MPIutils::rearrange_tables(
        peripheral_tables, population_table, _model );

    peripheral_tables = rearranged.peripheral_tables;

    population_table = rearranged.population_table;

    // ------------------------------------------------
    // Do the actual transformation

    auto yhat = _model.transform(
        peripheral_tables,
        population_table,
        true,  // _transpose
        _score );

    // ------------------------------------------------
    // Gather prediction at the root process

    MPIutils::gather_matrix_by_key( yhat, rearranged.original_order );
}

#endif  // AUTOSQL_MULTINODE_MPI

// ------------------------------------------------------------------------
}  // namespace engine
}  // namespace autosql
