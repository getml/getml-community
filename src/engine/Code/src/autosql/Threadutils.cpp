#include "autosql/ensemble/ensemble.hpp"

namespace autosql
{
namespace ensemble
{
// ----------------------------------------------------------------------------

void Threadutils::copy(
    const std::vector<size_t> _rows,
    const size_t _col,
    const size_t _num_features,
    const std::vector<AUTOSQL_FLOAT>& _new_feature,
    std::vector<AUTOSQL_FLOAT>* _features )
{
    for ( size_t i = 0; i < _rows.size(); ++i )
        {
            assert( _rows[i] * _num_features + _col < _features->size() );

            ( *_features )[_rows[i] * _num_features + _col] = _new_feature[i];
        }
}

// ----------------------------------------------------------------------------

void Threadutils::fit_ensemble(
    const size_t _this_thread_num,
    const std::vector<size_t> _thread_nums,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const decisiontrees::Placeholder& _placeholder,
    const std::vector<std::string>& _peripheral_names,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    ensemble::DecisionTreeEnsemble* _ensemble )
{
    try
        {
            // ----------------------------------------------------------------
            // Build the subview on the population table

            const auto population_subview = utils::DataFrameScatterer::
                DataFrameScatterer::scatter_data_frame(
                    _population, _thread_nums, _this_thread_num );

            // ----------------------------------------------------------------
            // Create abstractions over the peripheral_tables and the population
            // table - for convenience.

            const auto table_holder = std::make_shared<const decisiontrees::TableHolder>(
                _placeholder,
                population_subview,
                _peripheral,
                _peripheral_names );

            // ----------------------------------------------------------------
            // Start fitting

            _ensemble->fit( table_holder, _logger );

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

AUTOSQL_INT Threadutils::get_num_threads( const AUTOSQL_INT _num_threads )
{
    auto num_threads = _num_threads;

    if ( num_threads <= 0 )
        {
            num_threads = std::max(
                2,
                static_cast<AUTOSQL_INT>(
                    std::thread::hardware_concurrency() ) -
                    2 );
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
    std::vector<AUTOSQL_FLOAT>* _features )
{
    try
        {
            // ----------------------------------------------------------------
            // Build the subview on the population table

            const auto population_subview = utils::DataFrameScatterer::
                DataFrameScatterer::scatter_data_frame(
                    _population, _thread_nums, _this_thread_num );

            // ----------------------------------------------------------------
            // Create abstractions over the peripheral_tables and the population
            // table - for convenience

            const auto table_holder = decisiontrees::TableHolder(
                _ensemble.placeholder(),
                population_subview,
                _peripheral,
                _ensemble.peripheral_names() );

            // ----------------------------------------------------------------
            // aggregations::AggregationImpl stores most of the data for the
            // aggregations. We do not want to reallocate the data all the time.

            auto impl = containers::Optional<aggregations::AggregationImpl>(
                new aggregations::AggregationImpl(
                    population_subview.nrows() ) );

            // ----------------------------------------------------------------
            // Builc the actual features.

            for ( size_t i = 0; i < _ensemble.trees().size(); ++i )
                {
                    const auto new_feature =
                        _ensemble.transform( table_holder, i, _logger, &impl );

                    copy(
                        population_subview.rows(),
                        i,
                        _ensemble.num_features(),
                        new_feature,
                        _features );

                    if ( _logger /*&& !silent */ )
                        {
                            _logger->log(
                                "Built FEATURE_" + std::to_string( i + 1 ) +
                                "." );
                        }
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
}  // namespace autosql
