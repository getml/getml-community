#include "engine/engine.hpp"

#ifdef AUTOSQL_MULTITHREADING

namespace autosql
{
namespace engine
{
namespace multithreading
{
// ----------------------------------------------------------------------------

void Threadutils::calculate_displs(
    const AUTOSQL_INT _nrows, std::vector<AUTOSQL_INT>& _displs )
{
    AUTOSQL_INT num_threads = static_cast<AUTOSQL_INT>( _displs.size() - 1 );

    _displs[0] = 0;

    for ( size_t i = 0; i < _displs.size() - 1; ++i )
        {
            _displs[i + 1] = _displs[i] + _nrows / num_threads;
        }

    _displs.back() = _nrows;
}

// ----------------------------------------------------------------------------

std::string Threadutils::fit(
    AUTOSQL_INT _num_threads,
    decisiontrees::DecisionTreeEnsemble& _model,
    const std::shared_ptr<const logging::Logger>& _logger,
    std::vector<containers::DataFrame>& _peripheral_tables,
    containers::DataFrame& _population_table,
    descriptors::Hyperparameters& _hyperparameters )
{
    // ------------------------------------------------------

    std::vector<decisiontrees::DecisionTreeEnsemble> models( _num_threads - 1 );

    std::vector<std::thread> threads = {};

    autosql::multithreading::Communicator comm( _num_threads );

    _model.set_comm( &comm );

    // ------------------------------------------------------
    // Build thread_nums

    const auto thread_nums = DataFrameScatterer::build_thread_nums(
        _population_table.join_keys(), _num_threads );

    // ------------------------------------------------------
    // Create copies of the model and spawn threads

    for ( AUTOSQL_INT i = 0; i < static_cast<AUTOSQL_INT>( models.size() ); ++i )
        {
            models[i] = _model;

            models[i].set_comm( &comm );

            auto subview = DataFrameScatterer::scatter_data_frame(
                _population_table, thread_nums, i + 1 );

            threads.push_back( std::thread(
                fit_model,
                models[i],
                _logger,
                _peripheral_tables,
                subview,
                _hyperparameters ) );
        }

    // ------------------------------------------------------
    // Train model in main thread

    std::string msg;

    try
        {
            auto subview = DataFrameScatterer::scatter_data_frame(
                _population_table, thread_nums, 0 );

            msg = _model.fit(
                _logger, _peripheral_tables, subview, _hyperparameters );
        }
    catch ( std::exception& e )
        {
            for ( auto& thr : threads )
                {
                    thr.join();
                }

            throw std::invalid_argument( e.what() );
        }

    // ------------------------------------------------------
    // Join all other threads

    for ( auto& thr : threads )
        {
            thr.join();
        }

    // ------------------------------------------------------

    return msg;
}

// ----------------------------------------------------------------------------

AUTOSQL_INT Threadutils::get_num_threads( const AUTOSQL_INT _num_threads )
{
    auto num_threads = _num_threads;

    if ( num_threads <= 0 )
        {
            num_threads = std::max(
                2,
                static_cast<AUTOSQL_INT>( std::thread::hardware_concurrency() ) -
                    2 );
        }

    return num_threads;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Threadutils::score(
    const AUTOSQL_INT _num_threads,
    const containers::Matrix<AUTOSQL_FLOAT>& _yhat,
    const containers::Matrix<AUTOSQL_FLOAT>& _y,
    decisiontrees::DecisionTreeEnsemble* _model )
{
    // ------------------------------------------------------

    assert( _yhat.nrows() == _y.nrows() );
    assert( _yhat.ncols() == _y.ncols() );

    // ------------------------------------------------------

    std::vector<decisiontrees::DecisionTreeEnsemble> models( _num_threads - 1 );

    std::vector<std::thread> threads = {};

    autosql::multithreading::Communicator comm( _num_threads );

    _model->set_comm( &comm );

    // ------------------------------------------------------
    // Calculate displs

    std::vector<AUTOSQL_INT> displs( _num_threads + 1 );

    calculate_displs( _yhat.nrows(), displs );

    // ------------------------------------------------------
    // Create copies of the metric and spawn threads

    for ( AUTOSQL_SIZE i = 0; i < models.size(); ++i )
        {
            models[i] = *_model;

            models[i].set_comm( &comm );

            threads.push_back( std::thread(
                score_model,
                _yhat.subview( displs[i + 1], displs[i + 2] ),
                _y.subview( displs[i + 1], displs[i + 2] ),
                &models[i] ) );
        }

    // ------------------------------------------------------
    // Get metric in main thread

    Poco::JSON::Object result;

    try
        {
            result = _model->score(
                _yhat.subview( displs[0], displs[1] ),
                _y.subview( displs[0], displs[1] ) );
        }
    catch ( std::exception& e )
        {
            for ( auto& thr : threads )
                {
                    thr.join();
                }

            throw std::invalid_argument( e.what() );
        }

    // ------------------------------------------------------
    // Join all other threads

    for ( auto& thr : threads )
        {
            thr.join();
        }

    // ------------------------------------------------------

    return result;

    // ------------------------------------------------------
}

// ----------------------------------------------------------------------------

containers::Matrix<AUTOSQL_FLOAT> Threadutils::transform(
    AUTOSQL_INT _num_threads,
    decisiontrees::DecisionTreeEnsemble& _model,
    const std::shared_ptr<const logging::Logger>& _logger,
    std::vector<containers::DataFrame>& _peripheral_tables,
    containers::DataFrame& _population_table,
    bool _score )
{
    std::vector<decisiontrees::DecisionTreeEnsemble> models( _num_threads - 1 );

    std::vector<std::future<containers::Matrix<AUTOSQL_FLOAT>>> futures;

    std::vector<std::shared_ptr<const std::vector<AUTOSQL_INT>>> indices;

    autosql::multithreading::Communicator comm( _num_threads );

    _model.set_comm( &comm );

    // ------------------------------------------------------
    // Build thread_nums

    const auto thread_nums = DataFrameScatterer::build_thread_nums(
        _population_table.join_keys(), _num_threads );

    // ------------------------------------------------------
    // Create copies of the model and spawn threads

    for ( AUTOSQL_INT i = 0; i < static_cast<AUTOSQL_INT>( models.size() ); ++i )
        {
            models[i] = _model;

            models[i].set_comm( &comm );

            auto subview = DataFrameScatterer::scatter_data_frame(
                _population_table, thread_nums, i + 1 );

            indices.push_back( subview.get_indices() );

            futures.push_back( std::async(
                std::launch::async,
                transform_model,
                models[i],
                _logger,
                _peripheral_tables,
                subview,
                _score ) );
        }

    // ------------------------------------------------------
    // Transform in main thread

    containers::Matrix<AUTOSQL_FLOAT> yhat;

    try
        {
            auto subview = DataFrameScatterer::scatter_data_frame(
                _population_table, thread_nums, 0 );

            auto temp = _model.transform(
                _logger,
                _peripheral_tables,
                subview,
                true,  // _transpose
                _score );

            yhat = containers::Matrix<AUTOSQL_FLOAT>(
                _population_table.nrows(), temp.ncols() );

            copy( subview.get_indices(), temp, yhat );
        }
    catch ( std::exception& e )
        {
            for ( auto& f : futures )
                {
                    f.get();
                }

            throw std::invalid_argument( e.what() );
        }

    // ------------------------------------------------------
    // Join all other threads

    assert( futures.size() == indices.size() );

    for ( size_t i = 0; i < futures.size(); ++i )
        {
            auto temp = futures[i].get();

            copy( indices[i], temp, yhat );
        }

    // ------------------------------------------------------

    return yhat;
}

// ----------------------------------------------------------------------------
}  // namespace multithreading
}  // namespace engine
}  // namespace autosql

#endif  // AUTOSQL_MULTITHREADING
