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

void Threadutils::fit_ensemble(
    const size_t _this_thread_num,
    const std::vector<size_t> _thread_nums,
    const std::shared_ptr<const descriptors::Hyperparameters>& _hyperparameters,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const helpers::RowIndexContainer& _row_indices,
    const helpers::WordIndexContainer& _word_indices,
    const std::optional<const helpers::MappedContainer>& _mapped,
    const containers::Placeholder& _placeholder,
    const std::vector<std::string>& _peripheral_names,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    multithreading::Communicator* _comm,
    ensemble::DecisionTreeEnsemble* _ensemble )
{
    try
        {
            // ----------------------------------------------------------------

            const auto population_subview = utils::DataFrameScatterer::
                DataFrameScatterer::scatter_data_frame(
                    _population, _thread_nums, _this_thread_num );

            // ----------------------------------------------------------------

            const auto table_holder =
                std::make_shared<const decisiontrees::TableHolder>(
                    _placeholder,
                    population_subview,
                    _peripheral,
                    _peripheral_names,
                    _row_indices,
                    _word_indices,
                    _mapped );

            // ----------------------------------------------------------------

            assert_true( table_holder->main_tables_.size() > 0 );

            assert_true( _hyperparameters );

            const auto opt =
                std::unique_ptr<optimizationcriteria::OptimizationCriterion>(
                    new optimizationcriteria::RSquaredCriterion(
                        _hyperparameters,
                        _hyperparameters->loss_function_,
                        table_holder->main_tables_[0],
                        _comm ) );

            opt->calc_sampling_rate();

            opt->calc_residuals();

            // ----------------------------------------------------------------

            _ensemble->fit(
                table_holder,
                _word_indices,
                _logger,
                _hyperparameters->num_features_,
                opt.get(),
                _comm );

            // ----------------------------------------------------------------
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
    const size_t _this_thread_num,
    const std::vector<size_t> _thread_nums,
    const std::shared_ptr<const descriptors::Hyperparameters>& _hyperparameters,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::optional<helpers::WordIndexContainer>& _word_indices,
    const std::optional<const helpers::MappedContainer>& _mapped,
    const std::vector<size_t>& _index,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const ensemble::DecisionTreeEnsemble& _ensemble,
    multithreading::Communicator* _comm,
    containers::Features* _features )
{
    try
        {
            // ----------------------------------------------------------------

            const auto population_subview = utils::DataFrameScatterer::
                DataFrameScatterer::scatter_data_frame(
                    _population, _thread_nums, _this_thread_num );

            const auto table_holder = decisiontrees::TableHolder(
                _ensemble.placeholder(),
                population_subview,
                _peripheral,
                _ensemble.peripheral(),
                std::nullopt,
                _word_indices,
                _mapped );

            // ----------------------------------------------------------------

            const auto subpredictions = SubtreeHelper::make_predictions(
                table_holder,
                _ensemble.subensembles_avg(),
                _ensemble.subensembles_sum(),
                _logger,
                _comm );

            const auto subfeatures =
                SubtreeHelper::make_subfeatures( table_holder, subpredictions );

            // ----------------------------------------------------------------

            auto impl = containers::Optional<aggregations::AggregationImpl>(
                new aggregations::AggregationImpl(
                    population_subview.nrows() ) );

            // ----------------------------------------------------------------

            utils::Logger::log(
                "Multirel: Building features...", _logger, _comm );

            // ----------------------------------------------------------------
            // Build the actual features.

            assert_true( _index.size() == _features->size() );

            for ( size_t i = 0; i < _index.size(); ++i )
                {
                    const auto ix = _index.at( i );

                    assert_true( ix < _ensemble.num_features() );

                    const auto new_feature = _ensemble.transform(
                        table_holder, subfeatures, ix, &impl );

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

            // ----------------------------------------------------------------
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
}  // namespace multirel
