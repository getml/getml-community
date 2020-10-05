#include "relcit/ensemble/ensemble.hpp"

namespace relcit
{
namespace ensemble
{
// ----------------------------------------------------------------------------

void Threadutils::copy(
    const std::vector<size_t> _rows,
    const std::vector<Float>& _local_feature,
    std::vector<Float>* _global_feature )
{
    assert_true( _rows.size() == _local_feature.size() );

    for ( size_t i = 0; i < _rows.size(); ++i )
        {
            assert_true( _rows[i] < _global_feature->size() );

            ( *_global_feature )[_rows[i]] = _local_feature[i];
        }
}

// ----------------------------------------------------------------------------

void Threadutils::fit_as_feature_learner(
    const size_t _this_thread_num,
    const std::vector<size_t>& _thread_nums,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    multithreading::Communicator* _comm,
    ensemble::DecisionTreeEnsemble* _ensemble )
{
    const auto population_subview =
        relcit::utils::DataFrameScatterer::scatter_data_frame(
            _population, _thread_nums, _this_thread_num );

    const auto [loss_function, table_holder] =
        _ensemble->init_as_feature_learner( population_subview, _peripheral );

    _ensemble->fit_subensembles( table_holder, _logger, loss_function );

    auto predictions =
        _ensemble->make_subpredictions( *table_holder, _logger, _comm );

    auto subfeatures =
        SubtreeHelper::make_subfeatures( *table_holder, predictions );

    const auto num_features = _ensemble->hyperparameters().num_features_;

    utils::Logger::log( "RelCITModel: Training features...", _logger, _comm );

    for ( int i = 0; i < num_features; ++i )
        {
            _ensemble->fit_new_feature(
                loss_function, table_holder, subfeatures );

            const auto progress = ( ( i + 1 ) * 100 ) / num_features;

            utils::Logger::log(
                "Trained FEATURE_" + std::to_string( i + 1 ) +
                    ". Progress: " + std::to_string( progress ) + "\%.",
                _logger,
                _comm );
        }
}

// ----------------------------------------------------------------------------

void Threadutils::fit_as_predictor(
    const size_t _this_thread_num,
    const std::vector<size_t>& _thread_nums,
    const containers::DataFrame& _population,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    multithreading::Communicator* _comm,
    ensemble::DecisionTreeEnsemble* _ensemble )
{
    const auto population_subview =
        relcit::utils::DataFrameScatterer::scatter_data_frame(
            _population, _thread_nums, _this_thread_num );

    const auto loss_function =
        _ensemble->init_as_predictor( population_subview );

    const auto num_features = _ensemble->hyperparameters().num_features_;

    auto matches = utils::Matchmaker::make_matches( population_subview );

    for ( size_t i = 0; i < num_features; ++i )
        {
            _ensemble->fit_new_tree(
                loss_function,
                population_subview,
                matches.begin(),
                matches.end() );

            utils::Logger::log(
                "Trained tree " + std::to_string( i + 1 ) + ".",
                _logger,
                _comm );
        }
}

// ----------------------------------------------------------------------------

void Threadutils::fit_ensemble(
    const size_t _this_thread_num,
    const std::vector<size_t> _thread_nums,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    multithreading::Communicator* _comm,
    ensemble::DecisionTreeEnsemble* _ensemble )
{
    try
        {
            if ( _peripheral.size() > 0 )
                {
                    fit_as_feature_learner(
                        _this_thread_num,
                        _thread_nums,
                        _population,
                        _peripheral,
                        _logger,
                        _comm,
                        _ensemble );
                }
            else
                {
                    fit_as_predictor(
                        _this_thread_num,
                        _thread_nums,
                        _population,
                        _logger,
                        _comm,
                        _ensemble );
                }
        }
    catch ( std::exception& e )
        {
            if ( _logger )
                {
                    throw std::runtime_error( e.what() );
                }
            // TODO: Remove
            else
                {
                    std::cout << e.what() << std::endl;
                }
        }
}

// ----------------------------------------------------------------------------

Int Threadutils::get_num_threads( const Int _num_threads )
{
    auto num_threads = _num_threads;

    if ( num_threads <= 0 )
        {
            num_threads = std::max(
                2,
                static_cast<Int>( std::thread::hardware_concurrency() ) / 2 );
        }

    return num_threads;
}

// ----------------------------------------------------------------------------

void Threadutils::transform_as_feature_learner(
    const size_t _this_thread_num,
    const std::vector<size_t> _thread_nums,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::vector<size_t>& _index,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const ensemble::DecisionTreeEnsemble& _ensemble,
    multithreading::Communicator* _comm,
    containers::Features* _features )
{
    auto population_subview =
        utils::DataFrameScatterer::DataFrameScatterer::scatter_data_frame(
            _population, _thread_nums, _this_thread_num );

    const auto table_holder = TableHolder(
        _ensemble.placeholder(),
        population_subview,
        _peripheral,
        _ensemble.peripheral() );

    auto predictions =
        _ensemble.make_subpredictions( table_holder, _logger, _comm );

    auto subfeatures =
        SubtreeHelper::make_subfeatures( table_holder, predictions );

    assert_true( _features->size() == _index.size() );

    utils::Logger::log( "RelCITModel: Building features...", _logger, _comm );

    for ( size_t i = 0; i < _index.size(); ++i )
        {
            const auto ix = _index.at( i );

            const auto new_feature =
                _ensemble.transform( table_holder, subfeatures, ix );

            copy(
                population_subview.rows(),
                new_feature,
                _features->at( i ).get() );

            const auto progress = ( ( i + 1 ) * 100 ) / _index.size();

            utils::Logger::log(
                "Built FEATURE_" + std::to_string( ix + 1 ) +
                    ". Progress: " + std::to_string( progress ) + "\%.",
                _logger,
                _comm );
        }
}

// ----------------------------------------------------------------------------

void Threadutils::transform_as_predictor(
    const size_t _this_thread_num,
    const std::vector<size_t> _thread_nums,
    const containers::DataFrame& _population,
    const ensemble::DecisionTreeEnsemble& _ensemble,
    multithreading::Communicator* _comm,
    containers::Features* _features )
{
    const auto population_subview =
        relcit::utils::DataFrameScatterer::scatter_data_frame(
            _population, _thread_nums, _this_thread_num );

    assert_true( _features->size() == 1 );

    const auto predictions = _ensemble.predict( population_subview );

    copy( population_subview.rows(), predictions, ( *_features )[0].get() );
}

// ----------------------------------------------------------------------------

void Threadutils::transform_ensemble(
    const size_t _this_thread_num,
    const std::vector<size_t> _thread_nums,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::vector<size_t>& _index,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const ensemble::DecisionTreeEnsemble& _ensemble,
    multithreading::Communicator* _comm,
    containers::Features* _features )
{
    try
        {
            if ( _peripheral.size() > 0 )
                {
                    transform_as_feature_learner(
                        _this_thread_num,
                        _thread_nums,
                        _population,
                        _peripheral,
                        _index,
                        _logger,
                        _ensemble,
                        _comm,
                        _features );
                }
            else
                {
                    transform_as_predictor(
                        _this_thread_num,
                        _thread_nums,
                        _population,
                        _ensemble,
                        _comm,
                        _features );
                }
        }
    catch ( std::exception& e )
        {
            if ( _logger )
                {
                    throw std::runtime_error( e.what() );
                }
            // TODO: Remove
            else
                {
                    std::cout << e.what() << std::endl;
                }
        }
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relcit
