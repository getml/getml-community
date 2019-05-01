#include "metrics/metrics.hpp"

namespace metrics
{
// ----------------------------------------------------------------------------

Poco::JSON::Object Accuracy::score(
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

    std::vector<METRICS_FLOAT> accuracy( ncols() );

    auto accuracy_curves = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    std::vector<METRICS_FLOAT> prediction_min( ncols() );
    std::vector<METRICS_FLOAT> prediction_step_size( ncols() );

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
            // Calculate step_size

            const size_t num_critical_values = 200;

            // We use num_critical_values - 1, so that the greatest
            // critical_value will actually be greater than y_max.
            // This is to avoid segfaults.
            const METRICS_FLOAT step_size =
                ( yhat_max - yhat_min ) /
                static_cast<METRICS_FLOAT>( num_critical_values - 1 );

            // -----------------------------------------------------
            // Calculate bins

            std::vector<METRICS_FLOAT> negatives( num_critical_values );

            std::vector<METRICS_FLOAT> false_positives( num_critical_values );

            for ( size_t i = 0; i < nrows(); ++i )
                {
                    if ( y( i, j ) != 0.0 && y( i, j ) != 1.0 )
                        {
                            throw std::invalid_argument(
                                "For the accuracy metric, the target "
                                "values can only be zero or one! Found "
                                "value: " +
                                std::to_string( y( i, j ) ) );
                        }

                    // Note that this operation will always round down.
                    const size_t crv = static_cast<size_t>(
                        ( yhat( i, j ) - yhat_min ) / step_size );

                    negatives[crv] += 1.0;

                    false_positives[crv] += y( i, j );
                }

            // ---------------------------------------------
            // Reduce, if applicable.

            if ( impl_.has_comm() )
                {
                    impl_.reduce( std::plus<METRICS_FLOAT>(), &negatives );

                    impl_.reduce(
                        std::plus<METRICS_FLOAT>(), &false_positives );
                }

            // ---------------------------------------------
            // Calculate partial sums (now negatives and false_positives are
            // true to their name).

            std::partial_sum(
                negatives.begin(), negatives.end(), negatives.begin() );

            std::partial_sum(
                false_positives.begin(),
                false_positives.end(),
                false_positives.begin() );

            const auto nrows = negatives.back();

            const auto all_positives = false_positives.back();

            // ---------------------------------------------
            // Calculate accuracies.

            std::vector<METRICS_FLOAT> accuracies( num_critical_values );

            for ( size_t i = 0; i < num_critical_values; ++i )
                {
                    const auto true_positives =
                        all_positives - false_positives[i];

                    const auto true_negatives =
                        negatives[i] - false_positives[i];

                    accuracies[i] = ( true_positives + true_negatives ) / nrows;
                }

            // ---------------------------------------------
            // Find maximum accuracy

            accuracy[j] =
                *std::max_element( accuracies.begin(), accuracies.end() );

            // -----------------------------------------------------

            prediction_min[j] = yhat_min;
            prediction_step_size[j] = step_size;

            accuracy_curves->add( JSON::vector_to_array_ptr( accuracies ) );

            // -----------------------------------------------------
        }

    // -----------------------------------------------------
    // Transform to object.

    Poco::JSON::Object obj;

    obj.set( "accuracy_", JSON::vector_to_array_ptr( accuracy ) );

    obj.set( "accuracy_curves_", accuracy_curves );

    obj.set( "prediction_min_", JSON::vector_to_array_ptr( prediction_min ) );

    obj.set(
        "prediction_step_size_",
        JSON::vector_to_array_ptr( prediction_step_size ) );

    // -----------------------------------------------------

    return obj;

    // -----------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace metrics
