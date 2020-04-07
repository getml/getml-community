
// -----------------------------------------------------------------------------

void test3_linear_regression_sparse()
{
    // -------------------------------------------------------------------------

    std::cout << "Test 3 | Linear regression, sparse\t\t";

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
            ( *y )[i] = 3.0 * ( *X_numerical[0] )[i] +
                        2.0 * ( *X_numerical[1] )[i] +
                        7.0 * ( *X_numerical[2] )[i] + 2.0;

            if ( ( *X_categorical[0] )[i] < 250 )
                {
                    ( *y )[i] += 1000.0;
                }
        }

    const auto impl = std::make_shared<predictors::PredictorImpl>(
        std::vector<size_t>( {3} ),
        std::vector<std::string>( {"categorical"} ),
        std::vector<std::string>() );

    impl->fit_encodings( X_categorical );

    X_categorical = impl->transform_encodings( X_categorical );

    auto hyperparams = Poco::JSON::Object();
    hyperparams.set( "reg_lambda_", 1e-10 );
    hyperparams.set( "learning_rate_", 0.9 );

    auto lin_reg = predictors::LinearRegression( hyperparams, impl, {} );

    lin_reg.fit(
        std::shared_ptr<const logging::AbstractLogger>(),
        X_categorical,
        X_numerical,
        y );

    auto yhat = lin_reg.predict( X_categorical, X_numerical );

    for ( size_t i = 0; i < yhat->size(); ++i )
        {
            /*  std::cout << "target: " << y->at( i )
                        << ", prediction: " << yhat->at( i ) << std::endl;*/

            assert_true( std::abs( y->at( i ) - yhat->at( i ) < 10.0 ) );
        }

    // ------------------------------------------------------------------------

    std::cout << "| OK" << std::endl;

    // ------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
