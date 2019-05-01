#include "metrics/metrics.hpp"

namespace metrics
{
// ----------------------------------------------------------------------------

Poco::JSON::Object AUC::score(
    const METRICS_FLOAT* const _yhat,
    const size_t _yhat_nrows,
    const size_t _yhat_ncols,
    const METRICS_FLOAT* const _y,
    const size_t _y_nrows,
    const size_t _y_ncols )
{
    // -----------------------------------------------------

    impl_.set_data( _yhat, _yhat_nrows, _yhat_ncols, _y, _y_nrows, _y_ncols );

    // -----------------------------------------------------

    if ( nrows() < 1 )
        {
            std::invalid_argument(
                "There needs to be at least one row for the AUC score to "
                "work!" );
        }

    // -----------------------------------------------------

    std::vector<METRICS_FLOAT> auc( ncols() );

    auto true_positive_arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    auto false_positive_arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    // -----------------------------------------------------

    for ( size_t j = 0; j < ncols(); ++j )
        {
            // ---------------------------------------------
            // Find minimum and maximum

            METRICS_FLOAT yhat_min = yhat( 0, j );

            METRICS_FLOAT yhat_max = yhat( 0, j );

            for ( size_t i = 0; i < nrows(); ++i )
                {
                    if ( yhat( i, j ) < yhat_min )
                        yhat_min = yhat( i, j );

                    else if ( yhat( i, j ) > yhat_max )
                        yhat_max = yhat( i, j );
                }

            // ---------------------------------------------
            // Reduce, if applicable.

            if ( impl_.has_comm() )
                {
                    impl_.reduce(
                        multithreading::minimum<METRICS_FLOAT>(), &yhat_min );

                    impl_.reduce(
                        multithreading::maximum<METRICS_FLOAT>(), &yhat_max );
                }

            // ---------------------------------------------
            // If there is no variation in _yhat, then auc is 0.5;

            if ( yhat_min == yhat_max )
                {
                    auc[j] = 0.5;
                    continue;
                }

            // ---------------------------------------------
            // Calculate step_size

            const size_t num_critical_values = 10000;

            // We use num_critical_values - 1, so that the greatest
            // critical_value will actually be greater than y_max.
            // This is to avoid segfaults.
            const METRICS_FLOAT step_size =
                ( yhat_max - yhat_min ) /
                static_cast<METRICS_FLOAT>( num_critical_values - 1 );

            // ---------------------------------------------
            // Calculate true_positives and predicted_negative

            std::vector<METRICS_FLOAT> true_positives( num_critical_values );

            std::vector<METRICS_FLOAT> predicted_negative(
                num_critical_values );

            for ( size_t i = 0; i < nrows(); ++i )
                {
                    // Note that this operation will always round down.
                    const size_t crv = static_cast<size_t>(
                        ( yhat( i, j ) - yhat_min ) / step_size );

                    true_positives[crv] += y( i, j );

                    predicted_negative[crv] += 1.0;
                }

            if ( impl_.has_comm() )
                {
                    impl_.reduce( std::plus<METRICS_FLOAT>(), &true_positives );

                    impl_.reduce(
                        std::plus<METRICS_FLOAT>(), &predicted_negative );
                }

            std::partial_sum(
                predicted_negative.begin(),
                predicted_negative.end(),
                predicted_negative.begin() );

            const METRICS_FLOAT all_positives = std::accumulate(
                true_positives.begin(), true_positives.end(), 0.0 );

            std::partial_sum(
                true_positives.begin(),
                true_positives.end(),
                true_positives.begin() );

            std::for_each(
                true_positives.begin(),
                true_positives.end(),
                [all_positives]( METRICS_FLOAT& val ) {
                    val = all_positives - val;
                } );

            // ---------------------------------------------
            // Calculate false positives

            METRICS_FLOAT nrow_float = static_cast<METRICS_FLOAT>( nrows() );

            if ( impl_.has_comm() )
                {
                    impl_.reduce( std::plus<METRICS_FLOAT>(), &nrow_float );
                }

            std::vector<METRICS_FLOAT> false_positives( num_critical_values );

            std::transform(
                true_positives.begin(),
                true_positives.end(),
                predicted_negative.begin(),
                false_positives.begin(),
                [nrow_float](
                    const METRICS_FLOAT& tp, const METRICS_FLOAT& pn ) {
                    return nrow_float - tp - pn;
                } );

            // ---------------------------------------------
            // Calculate true positive rate and false positive rate

            std::vector<METRICS_FLOAT> true_positive_rate(
                num_critical_values );

            std::vector<METRICS_FLOAT> false_positive_rate(
                num_critical_values );

            const METRICS_FLOAT all_negatives = nrow_float - all_positives;

            std::transform(
                true_positives.begin(),
                true_positives.end(),
                true_positive_rate.begin(),
                [all_positives]( const METRICS_FLOAT& tp ) {
                    return tp / all_positives;
                } );

            std::transform(
                false_positives.begin(),
                false_positives.end(),
                false_positive_rate.begin(),
                [all_negatives]( const METRICS_FLOAT& fp ) {
                    return fp / all_negatives;
                } );

            // ---------------------------------------------
            // Calculate area under curve - note that the last critical
            // value is greater than y_max, so the last point must always be
            // (1, 1).

            auc[j] +=
                ( 1.0 - false_positive_rate[0] ) * true_positive_rate[0] * 0.5;

            for ( size_t i = 0; i < num_critical_values - 1; ++i )
                {
                    auc[j] +=
                        ( false_positive_rate[i] -
                          false_positive_rate[i + 1] ) *
                        ( true_positive_rate[i] + true_positive_rate[i + 1] ) *
                        0.5;
                }

            // -----------------------------------------------------
            // Downsample the TPR and the FPR and add them to the arrays.

            const auto skip = num_critical_values / 200;

            auto true_positive_downsampled =
                Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

            auto false_positive_downsampled =
                Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

            for ( size_t i = 0; i < num_critical_values; i += skip )
                {
                    true_positive_downsampled->add( true_positive_rate[i] );
                }

            for ( size_t i = 0; i < num_critical_values; i += skip )
                {
                    false_positive_downsampled->add( false_positive_rate[i] );
                }

            true_positive_arr->add( true_positive_downsampled );

            false_positive_arr->add( false_positive_downsampled );

            // ---------------------------------------------
        }

    // -----------------------------------------------------
    // Transform to object and return.

    Poco::JSON::Object obj;

    obj.set( "auc_", JSON::vector_to_array_ptr( auc ) );

    obj.set( "fpr_", false_positive_arr );

    obj.set( "tpr_", true_positive_arr );

    // -----------------------------------------------------

    return obj;
}

// ----------------------------------------------------------------------------
}  // namespace metrics
