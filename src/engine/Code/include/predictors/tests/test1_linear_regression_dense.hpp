
// -----------------------------------------------------------------------------

void test1_linear_regression_dense()
{
    // -------------------------------------------------------------------------

    std::cout << std::endl
              << "Test 1 (Linear regression, dense): " << std::endl
              << std::endl;

    // -------------------------------------------------------------------------

    auto rng = std::mt19937( 100 );

    auto X = std::vector<predictors::FloatColumn>();

    for ( size_t i = 0; i < 3; ++i )
        {
            X.push_back( make_column<predictors::Float>( 1000, &rng ) );
        }

    auto y = std::make_shared<std::vector<predictors::Float>>( 1000 );

    for ( size_t i = 0; i < 1000; ++i )
        {
            ( *y )[i] = 3.0 * ( *X[0] )[i] + 2.0 * ( *X[1] )[i] +
                        7.0 * ( *X[2] )[i] + 2.0;
        }

    auto lin_reg = predictors::LinearRegression();

    lin_reg.fit( std::shared_ptr<const logging::AbstractLogger>(), X, y );

    auto yhat = lin_reg.predict( X );

    for ( size_t i = 0; i < yhat->size(); ++i )
        {
            /*std::cout << "target: " << y->at( i )
                      << ", prediction: " << yhat->at( i ) << std::endl;*/

            assert( std::abs( y->at( i ) - yhat->at( i ) < 1e-4 ) );
        }

    std::cout << std::endl << std::endl;

    // ------------------------------------------------------------------------

    std::cout << "OK." << std::endl << std::endl;

    // ------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
