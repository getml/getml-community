#include "relboost/ensemble/ensemble.hpp"

namespace relboost
{
namespace ensemble
{
// ----------------------------------------------------------------------------

void Threadutils::copy(
    const std::vector<size_t> _rows,
    const std::vector<Float>& _local_feature,
    helpers::Feature<Float, false>* _global_feature )
{
    assert_true( _rows.size() == _local_feature.size() );

    for ( size_t i = 0; i < _rows.size(); ++i )
        {
            assert_true( _rows[i] < _global_feature->size() );

            ( *_global_feature )[_rows[i]] = _local_feature[i];
        }
}

// ----------------------------------------------------------------------------

void Threadutils::fit_as_feature_learner( const ThreadutilsFitParams& _params )
{
    assert_true( _params.comm_ );
    assert_true( _params.ensemble_ );

    const auto population_subview =
        relboost::utils::DataFrameScatterer::scatter_data_frame(
            _params.population_,
            _params.thread_nums_,
            _params.this_thread_num_ );

    const auto [loss_function, table_holder] = _params.ensemble_->init(
        population_subview,
        _params.peripheral_,
        _params.row_indices_,
        _params.word_indices_,
        _params.feature_container_ );

    _params.ensemble_->fit_subensembles(
        table_holder, _params.logger_, loss_function );

    auto predictions = _params.ensemble_->make_subpredictions(
        *table_holder, _params.logger_, _params.comm_ );

    auto subfeatures =
        SubtreeHelper::make_subfeatures( *table_holder, predictions );

    const auto num_features =
        _params.ensemble_->hyperparameters().num_features_;

    utils::Logger::log(
        "Relboost: Training features...", _params.logger_, _params.comm_ );

    while ( _params.ensemble_->num_features() < num_features )
        {
            _params.ensemble_->fit_new_features(
                loss_function, table_holder, subfeatures, num_features );

            const auto progress =
                ( _params.ensemble_->num_features() * 100 ) / num_features;

            utils::Logger::log(
                "Trained new features. Progress: " +
                    std::to_string( progress ) + "%.",
                _params.logger_,
                _params.comm_ );
        }
}

// ----------------------------------------------------------------------------

void Threadutils::fit_ensemble( const ThreadutilsFitParams _params )
{
    try
        {
            fit_as_feature_learner( _params );
        }
    catch ( std::exception& e )
        {
            if ( _params.logger_ )
                {
                    throw std::runtime_error( e.what() );
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
    const ThreadutilsTransformParams& _params )
{
    assert_true( _params.comm_ );
    assert_true( _params.features_ );

    auto population_subview =
        utils::DataFrameScatterer::DataFrameScatterer::scatter_data_frame(
            _params.population_,
            _params.thread_nums_,
            _params.this_thread_num_ );

    const auto table_holder = TableHolder(
        _params.ensemble_.placeholder(),
        population_subview,
        _params.peripheral_,
        _params.ensemble_.peripheral(),
        std::nullopt,
        _params.word_indices_,
        _params.feature_container_ );

    auto predictions = _params.ensemble_.make_subpredictions(
        table_holder, _params.logger_, _params.comm_ );

    auto subfeatures =
        SubtreeHelper::make_subfeatures( table_holder, predictions );

    assert_true( _params.features_->size() == _params.index_.size() );

    utils::Logger::log(
        "Relboost: Building features...", _params.logger_, _params.comm_ );

    for ( size_t i = 0; i < _params.index_.size(); ++i )
        {
            const auto ix = _params.index_.at( i );

            const auto new_feature =
                _params.ensemble_.transform( table_holder, subfeatures, ix );

            assert_true( new_feature );

            copy(
                population_subview.rows(),
                *new_feature,
                &_params.features_->at( i ) );

            const auto progress = ( ( i + 1 ) * 100 ) / _params.index_.size();

            utils::Logger::log(
                "Built FEATURE_" + std::to_string( ix + 1 ) +
                    ". Progress: " + std::to_string( progress ) + "%.",
                _params.logger_,
                _params.comm_ );
        }
}

// ----------------------------------------------------------------------------

void Threadutils::transform_ensemble( const ThreadutilsTransformParams _params )
{
    try
        {
            transform_as_feature_learner( _params );
        }
    catch ( std::exception& e )
        {
            if ( _params.logger_ )
                {
                    throw std::runtime_error( e.what() );
                }
        }
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relboost
