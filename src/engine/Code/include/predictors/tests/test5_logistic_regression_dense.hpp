
// -----------------------------------------------------------------------------

void test5_logistic_regression_dense()
{
    // -------------------------------------------------------------------------

    std::cout << "Test 5 | Logistic regression, dense\t\t";

    // -------------------------------------------------------------------------

    auto rng = std::mt19937( 100 );

    auto X_categorical = std::vector<predictors::CIntColumn>();

    auto X_numerical = std::vector<predictors::FloatColumn>();

    for ( size_t i = 0; i < 3; ++i )
        {
            X_numerical.push_back(
                make_column<predictors::Float>( 1000, &rng ) );
        }

    auto y = std::make_shared<std::vector<predictors::Float>>( 1000 );

    for ( size_t i = 0; i < 1000; ++i )
        {
            const auto val = 3.0 * ( *X_numerical[0] )[i] +
                             2.0 * ( *X_numerical[1] )[i] +
                             7.0 * ( *X_numerical[2] )[i] + 2.0;

            if ( val > 2000.0 )
                {
                    ( *y )[i] = 1.0;
                }
            else
                {
                    ( *y )[i] = 0.0;
                }
        }

    const auto impl = std::make_shared<predictors::PredictorImpl>(
        std::vector<size_t>( {3} ),
        std::vector<std::string>(),
        std::vector<std::string>() );

    auto hyperparams = Poco::JSON::Object();
    hyperparams.set( "reg_lambda_", 1e-10 );
    hyperparams.set( "learning_rate_", 0.9 );

    auto log_reg = predictors::LogisticRegression( hyperparams, impl, {} );

    log_reg.fit(
        std::shared_ptr<const logging::AbstractLogger>(),
        X_categorical,
        X_numerical,
        y );

    auto yhat = log_reg.predict( X_categorical, X_numerical );

    for ( size_t i = 0; i < yhat->size(); ++i )
        {
            /*  std::cout << "target: " << y->at( i )
                        << ", prediction: " << yhat->at( i ) << std::endl;*/

            // Note that this implies 100% predictive accuracy.
            assert_true( std::abs( y->at( i ) - yhat->at( i ) ) < 0.5 );
        }

    // ------------------------------------------------------------------------

    std::cout << "| OK" << std::endl;

    // ------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
