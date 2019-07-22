#include "metrics/metrics.hpp"

namespace metrics
{
// ----------------------------------------------------------------------------

Poco::JSON::Object ROCCurve::score( const Features _yhat, const Features _y )
{
    // -----------------------------------------------------

    impl_.set_data( _yhat, _y );

    // -----------------------------------------------------

    if ( nrows() < 1 )
        {
            std::invalid_argument(
                "There needs to be at least one row for the AUC score to "
                "work!" );
        }

    // -----------------------------------------------------

    auto true_positive_arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    auto false_positive_arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    // -----------------------------------------------------

    for ( size_t j = 0; j < ncols(); ++j )
        {
            // ---------------------------------------------
            // Find minimum and maximum

            Float yhat_min = yhat( 0, j );

            Float yhat_max = yhat( 0, j );

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
                    impl_.reduce( multithreading::minimum<Float>(), &yhat_min );

                    impl_.reduce( multithreading::maximum<Float>(), &yhat_max );
                }

            // ---------------------------------------------
            // If there is no variation in _yhat, then auc is 0.5;

            if ( yhat_min == yhat_max )
                {
                    true_positive_arr->add( JSON::vector_to_array_ptr(
                        std::vector<Float>( {1.0, 0.0} ) ) );

                    false_positive_arr->add( JSON::vector_to_array_ptr(
                        std::vector<Float>( {0.0, 1.0} ) ) );

                    continue;
                }

            // ---------------------------------------------
            // Calculate step_size

            const size_t num_critical_values = 100;

            // We use num_critical_values - 1, so that the greatest
            // critical_value will actually be greater than y_max.
            // This is to avoid segfaults.
            const Float step_size =
                ( yhat_max - yhat_min ) /
                static_cast<Float>( num_critical_values - 1 );

            // ---------------------------------------------
            // Calculate true_positives and predicted_negative

            std::vector<Float> true_positives( num_critical_values );

            std::vector<Float> predicted_negative( num_critical_values );

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
                    impl_.reduce( std::plus<Float>(), &true_positives );

                    impl_.reduce( std::plus<Float>(), &predicted_negative );
                }

            std::partial_sum(
                predicted_negative.begin(),
                predicted_negative.end(),
                predicted_negative.begin() );

            const Float all_positives = std::accumulate(
                true_positives.begin(), true_positives.end(), 0.0 );

            std::partial_sum(
                true_positives.begin(),
                true_positives.end(),
                true_positives.begin() );

            std::for_each(
                true_positives.begin(),
                true_positives.end(),
                [all_positives]( Float& val ) { val = all_positives - val; } );

            // ---------------------------------------------
            // Calculate false positives

            Float nrow_float = static_cast<Float>( nrows() );

            if ( impl_.has_comm() )
                {
                    impl_.reduce( std::plus<Float>(), &nrow_float );
                }

            std::vector<Float> false_positives( num_critical_values );

            std::transform(
                true_positives.begin(),
                true_positives.end(),
                predicted_negative.begin(),
                false_positives.begin(),
                [nrow_float]( const Float& tp, const Float& pn ) {
                    return nrow_float - tp - pn;
                } );

            // ---------------------------------------------
            // Calculate true positive rate and false positive rate

            std::vector<Float> true_positive_rate( num_critical_values );

            std::vector<Float> false_positive_rate( num_critical_values );

            const Float all_negatives = nrow_float - all_positives;

            std::transform(
                true_positives.begin(),
                true_positives.end(),
                true_positive_rate.begin(),
                [all_positives]( const Float& tp ) {
                    return tp / all_positives;
                } );

            std::transform(
                false_positives.begin(),
                false_positives.end(),
                false_positive_rate.begin(),
                [all_negatives]( const Float& fp ) {
                    return fp / all_negatives;
                } );

            // -----------------------------------------------------
            // Add to arrays.

            true_positive_arr->add(
                JSON::vector_to_array_ptr( true_positive_rate ) );

            false_positive_arr->add(
                JSON::vector_to_array_ptr( false_positive_rate ) );

            // ---------------------------------------------
        }

    // -----------------------------------------------------
    // Transform to object and return.

    Poco::JSON::Object obj;

    obj.set( "fpr_", false_positive_arr );

    obj.set( "tpr_", true_positive_arr );

    // -----------------------------------------------------

    return obj;
}

// ----------------------------------------------------------------------------
}  // namespace metrics
