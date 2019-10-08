#include "relboost/ensemble/ensemble.hpp"

namespace relboost
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

void Threadutils::fit_ensemble(
    const size_t _this_thread_num,
    const std::vector<size_t> _thread_nums,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    ensemble::DecisionTreeEnsemble* _ensemble )
{
    try
        {
            const auto population_subview =
                relboost::utils::DataFrameScatterer::scatter_data_frame(
                    _population, _thread_nums, _this_thread_num );

            _ensemble->init( population_subview, _peripheral );

            const auto num_features =
                _ensemble->hyperparameters().num_features_;

            const auto silent = _ensemble->hyperparameters().silent_;

            for ( size_t i = 0; i < num_features; ++i )
                {
                    _ensemble->fit_new_feature();

                    if ( !silent && _logger )
                        {
                            _logger->log(
                                "Trained FEATURE_" + std::to_string( i + 1 ) +
                                "." );
                        }
                }
        }
    catch ( std::exception& e )
        {
            if ( _logger )
                {
                    throw std::runtime_error( e.what() );
                }

            std::cout << e.what() << std::endl;  // ToDO: remove this line
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
                static_cast<Int>( std::thread::hardware_concurrency() ) - 2 );
        }

    return num_threads;
}

// ----------------------------------------------------------------------------

void Threadutils::transform_ensemble(
    const size_t _this_thread_num,
    const std::vector<size_t> _thread_nums,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const ensemble::DecisionTreeEnsemble& _ensemble,
    containers::Features* _features )
{
    try
        {
            auto population_subview = utils::DataFrameScatterer::
                DataFrameScatterer::scatter_data_frame(
                    _population, _thread_nums, _this_thread_num );

            const auto table_holder = TableHolder(
                _ensemble.placeholder(),
                population_subview,
                _peripheral,
                _ensemble.peripheral_names() );

            const auto silent = _ensemble.hyperparameters().silent_;

            for ( size_t i = 0; i < _ensemble.num_features(); ++i )
                {
                    const auto new_feature =
                        _ensemble.transform( table_holder, i );

                    copy(
                        population_subview.rows(),
                        new_feature,
                        ( *_features )[i].get() );

                    if ( !silent && _logger )
                        {
                            _logger->log(
                                "Built FEATURE_" + std::to_string( i + 1 ) +
                                "." );
                        }
                }
        }
    catch ( std::exception& e )
        {
            if ( _logger )
                {
                    throw std::runtime_error( e.what() );
                }
        }
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relboost
