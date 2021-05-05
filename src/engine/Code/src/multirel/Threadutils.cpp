#include "multirel/ensemble/ensemble.hpp"

namespace multirel
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

void Threadutils::fit_ensemble( const ThreadutilsFitParams& _params )
{
    try
        {
            // ----------------------------------------------------------------

            const auto population_subview = utils::DataFrameScatterer::
                DataFrameScatterer::scatter_data_frame(
                    _params.population_,
                    _params.thread_nums_,
                    _params.this_thread_num_ );

            // ----------------------------------------------------------------

            const auto table_holder =
                std::make_shared<const decisiontrees::TableHolder>(
                    _params.ensemble_.placeholder(),
                    population_subview,
                    _params.peripheral_,
                    _params.ensemble_.peripheral(),
                    _params.row_indices_,
                    _params.word_indices_,
                    _params.feature_container_ );

            // ----------------------------------------------------------------

            assert_true( table_holder->main_tables().size() > 0 );

            const auto opt = _params.ensemble_.make_r_squared(
                table_holder->main_tables().at( 0 ), &_params.comm_ );

            // ----------------------------------------------------------------

            _params.ensemble_.fit(
                table_holder,
                _params.word_indices_,
                _params.logger_,
                _params.ensemble_.hyperparameters().num_features_,
                opt,
                &_params.comm_ );

            // ----------------------------------------------------------------
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

size_t Threadutils::get_num_threads( const size_t _num_threads )
{
    auto num_threads = _num_threads;

    if ( num_threads == 0 )
        {
            num_threads = std::max(
                static_cast<size_t>( 2 ),
                static_cast<size_t>( std::thread::hardware_concurrency() ) /
                    2 );
        }

    return num_threads;
}

// ----------------------------------------------------------------------------

void Threadutils::transform_ensemble(
    const ThreadutilsTransformParams& _params )
{
    try
        {
            // ----------------------------------------------------------------

            const auto population_subview = utils::DataFrameScatterer::
                DataFrameScatterer::scatter_data_frame(
                    _params.population_,
                    _params.thread_nums_,
                    _params.this_thread_num_ );

            const auto table_holder = decisiontrees::TableHolder(
                _params.ensemble_.placeholder(),
                population_subview,
                _params.peripheral_,
                _params.ensemble_.peripheral(),
                std::nullopt,
                _params.word_indices_,
                _params.feature_container_ );

            // ----------------------------------------------------------------

            const auto subpredictions = SubtreeHelper::make_predictions(
                table_holder,
                _params.ensemble_.subensembles_avg(),
                _params.ensemble_.subensembles_sum(),
                _params.logger_,
                &_params.comm_ );

            const auto subfeatures =
                SubtreeHelper::make_subfeatures( table_holder, subpredictions );

            // ----------------------------------------------------------------

            auto impl = containers::Optional<aggregations::AggregationImpl>(
                new aggregations::AggregationImpl(
                    population_subview.nrows() ) );

            // ----------------------------------------------------------------

            utils::Logger::log(
                "Multirel: Building features...",
                _params.logger_,
                &_params.comm_ );

            // ----------------------------------------------------------------
            // Build the actual features.

            assert_true( _params.index_.size() == _params.features_.size() );

            for ( size_t i = 0; i < _params.index_.size(); ++i )
                {
                    const auto ix = _params.index_.at( i );

                    assert_true( ix < _params.ensemble_.num_features() );

                    const auto new_feature = _params.ensemble_.transform(
                        table_holder, subfeatures, ix, &impl );

                    assert_true( new_feature );

                    copy(
                        population_subview.rows(),
                        *new_feature,
                        _params.features_.at( i ).get() );

                    const auto progress =
                        ( ( i + 1 ) * 100 ) / _params.index_.size();

                    utils::Logger::log(
                        "Built FEATURE_" + std::to_string( ix + 1 ) +
                            ". Progress: " + std::to_string( progress ) + "%.",
                        _params.logger_,
                        &_params.comm_ );
                }

            // ----------------------------------------------------------------
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
}  // namespace multirel
