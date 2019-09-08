
// -----------------------------------------------------------------------------

void test6_logistic_regression_sparse()
{
    // -------------------------------------------------------------------------

    std::cout << std::endl
              << "Test 6 (Logistic regression, sparse): " << std::endl
              << std::endl;

    // -------------------------------------------------------------------------

    auto rng = std::mt19937( 100 );

    auto X_categorical = std::vector<predictors::CIntColumn>();

    X_categorical.push_back( make_column<predictors::Int>( 1000, &rng ) );

    auto X_numerical = std::vector<predictors::FloatColumn>();

    for ( size_t i = 0; i < 3; ++i )
        {
            X_numerical.push_back(
                make_column<predictors::Float>( 1000, &rng ) );
        }

    auto y = std::make_shared<std::vector<predictors::Float>>( 1000 );

    for ( size_t i = 0; i < 1000; ++i )
        {
            auto val = 3.0 * ( *X_numerical[0] )[i] +
                       2.0 * ( *X_numerical[1] )[i] +
                       7.0 * ( *X_numerical[2] )[i] + 2.0;

            if ( ( *X_categorical[0] )[i] < 250 )
                {
                    val += 1000.0;
                }

            if ( val > 3000.0 )
                {
                    ( *y )[i] = 1.0;
                }
            else
                {
                    ( *y )[i] = 0.0;
                }
        }

    const auto impl = std::make_shared<predictors::PredictorImpl>(
        std::vector<std::string>( {"categorical"} ),
        std::vector<std::string>(),
        std::vector<std::string>(),
        3 );

    impl->fit_encodings( X_categorical );

    X_categorical = impl->transform_encodings( X_categorical );

    const auto hyperparams =
        std::make_shared<predictors::LinearHyperparams>( 1e-10, 0.9 );

    auto log_reg = predictors::LogisticRegression( hyperparams, impl );

    log_reg.fit(
        std::shared_ptr<const logging::AbstractLogger>(),
        X_categorical,
        X_numerical,
        y );

    auto yhat = log_reg.predict( X_categorical, X_numerical );

    auto accuracy = 0.0;

    for ( size_t i = 0; i < yhat->size(); ++i )
        {
            /*  std::cout << "target: " << y->at( i )
                        << ", prediction: " << yhat->at( i ) << std::endl;*/

            if ( std::abs( y->at( i ) - yhat->at( i ) ) < 0.5 )
                {
                    accuracy += 0.001;
                };
        }

    std::cout << "Accuracy: " << accuracy << std::endl;
    assert_true( accuracy > 0.99 );

    std::cout << std::endl << std::endl;

    // ------------------------------------------------------------------------

    std::cout << "OK." << std::endl << std::endl;

    // ------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
