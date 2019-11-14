
// -----------------------------------------------------------------------------

void test4_saving_and_loading_linear_regression( std::filesystem::path _test_path )
{
    // -------------------------------------------------------------------------

    std::cout << std::endl
              << "Test 4 (Saving and loading linear regression): " << std::endl
              << std::endl;

    // Append all subfolders to reach the required file. This 
    // appending will have a persistent effect of _test_path which
    // is stored on the heap. After setting it once to the correct
    // folder only the filename has to be replaced.
    _test_path.append( "predictors" ).append( "test4" ).append( "LinearRegression" );

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
        std::vector<std::string>( {"categorical"} ),
        std::vector<std::string>(),
        std::vector<std::string>(),
        3 );

    impl->fit_encodings( X_categorical );

    X_categorical = impl->transform_encodings( X_categorical );

    const auto hyperparams =
        std::make_shared<predictors::LinearHyperparams>( 1e-12, 0.9 );

    auto lin_reg = predictors::LinearRegression( hyperparams, impl );

    lin_reg.fit(
        std::shared_ptr<const logging::AbstractLogger>(),
        X_categorical,
        X_numerical,
        y );

    // ------------------------------------------------------------------------

    lin_reg.save( _test_path.string() );

    auto lin_reg2 = predictors::LinearRegression( hyperparams, impl );

    lin_reg2.load( _test_path.string() );

    lin_reg2.save( _test_path.replace_filename( "LinearRegression2" ).string() );

    auto lin_reg3 = predictors::LinearRegression( hyperparams, impl );

    lin_reg3.load( _test_path.string() );

    // ------------------------------------------------------------------------

    auto yhat = lin_reg.predict( X_categorical, X_numerical );
    auto yhat2 = lin_reg2.predict( X_categorical, X_numerical );
    auto yhat3 = lin_reg3.predict( X_categorical, X_numerical );

    for ( size_t i = 0; i < yhat->size(); ++i )
        {
            assert_true( std::abs( yhat->at( i ) - yhat2->at( i ) ) < 1e-4 );
            assert_true( std::abs( yhat->at( i ) - yhat3->at( i ) ) < 1e-4 );
        }

    std::cout << std::endl << std::endl;

    // ------------------------------------------------------------------------

    std::cout << "OK." << std::endl << std::endl;

    // ------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
