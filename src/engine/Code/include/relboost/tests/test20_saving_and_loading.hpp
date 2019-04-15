
// ---------------------------------------------------------------------------

void test20_saving_and_loading()
{
    // ------------------------------------------------------------------------

    std::cout << std::endl
              << "Test 20 (saving and loading): " << std::endl
              << std::endl;

    // ------------------------------------------------------------------------
    // Build artificial data set.

    auto rng = std::mt19937( 100 );

    // ------------------------------------------------------------------------
    // Build peripheral table.

    const auto categorical_peripheral_mat =
        relboost::containers::Matrix<std::int32_t>( 250000 );

    const auto discrete_peripheral_mat =
        relboost::containers::Matrix<double>( 250000 );

    const auto join_keys_peripheral = make_column<std::int32_t>( 250000, rng );

    const auto join_keys_peripheral_mat =
        relboost::containers::Matrix<std::int32_t>(
            {"join_key"},
            join_keys_peripheral.data(),
            join_keys_peripheral.size() );

    auto numerical_peripheral = make_column<double>( 250000, rng );

    const auto numerical_peripheral_mat = relboost::containers::Matrix<double>(
        {"column_01"},
        numerical_peripheral.data(),
        numerical_peripheral.size() );

    const auto time_stamps_peripheral = make_column<double>( 250000, rng );

    const auto time_stamps_peripheral_mat =
        relboost::containers::Matrix<double>(
            {"time_stamp"},
            time_stamps_peripheral.data(),
            time_stamps_peripheral.size() );

    const auto peripheral_df = relboost::containers::DataFrame(
        categorical_peripheral_mat,
        discrete_peripheral_mat,
        {join_keys_peripheral_mat},
        "PERIPHERAL",
        numerical_peripheral_mat,
        relboost::containers::Matrix<double>( 250000 ),
        {time_stamps_peripheral_mat} );

    // ------------------------------------------------------------------------
    // Build population table.

    const auto categorical_population_mat =
        relboost::containers::Matrix<std::int32_t>( 500 );

    const auto discrete_population_mat =
        relboost::containers::Matrix<double>( 500 );

    auto join_keys_population = std::vector<std::int32_t>( 500 );

    for ( size_t i = 0; i < join_keys_population.size(); ++i )
        {
            join_keys_population[i] = i;
        }

    const auto join_keys_population_mat =
        relboost::containers::Matrix<std::int32_t>(
            {"join_key"},
            join_keys_population.data(),
            join_keys_population.size() );

    auto numerical_population = make_column<double>( 500, rng );

    const auto numerical_population_mat = relboost::containers::Matrix<double>(
        {"column_01"},
        numerical_population.data(),
        numerical_population.size() );

    const auto time_stamps_population = make_column<double>( 500, rng );

    const auto time_stamps_population_mat =
        relboost::containers::Matrix<double>(
            {"time_stamp"},
            time_stamps_population.data(),
            time_stamps_population.size() );

    auto targets_population = std::vector<double>( 500 );

    for ( size_t i = 0; i < peripheral_df.nrows(); ++i )
        {
            const auto jk = peripheral_df.join_keys_[0]( i, 0 );

            assert( jk < 500 );

            if ( peripheral_df.time_stamps( i ) <=
                     time_stamps_population_mat( jk, 0 ) &&
                 peripheral_df.numerical_( i, 0 ) < 250.0 )
                {
                    targets_population[jk]++;
                }
        }

    const auto target_population_mat = relboost::containers::Matrix<double>(
        {"target"}, targets_population.data(), targets_population.size() );

    const auto population_df = relboost::containers::DataFrame(
        categorical_population_mat,
        discrete_population_mat,
        {join_keys_population_mat},
        "POPULATION",
        numerical_population_mat,
        target_population_mat,
        {time_stamps_population_mat} );

    // ---------------------------------------------
    // Build data model.

    const auto population_json = load_json( "../../tests/test20/schema.json" );

    const auto population =
        std::make_shared<const relboost::ensemble::Placeholder>(
            *population_json );

    const auto peripheral = std::make_shared<std::vector<std::string>>(
        std::vector<std::string>{"PERIPHERAL"} );

    // ------------------------------------------------------------------------
    // Load hyperparameters.

    const auto hyperparameters_json =
        load_json( "../../tests/test20/hyperparameters.json" );

    std::cout << relboost::JSON::stringify( *hyperparameters_json ) << std::endl
              << std::endl;

    const auto hyperparameters =
        std::make_shared<const relboost::Hyperparameters>(
            *hyperparameters_json );

    // ------------------------------------------------------------------------
    // Build model

    const auto encoding = std::make_shared<const std::vector<std::string>>(
        std::vector<std::string>(
            {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"} ) );

    auto model = relboost::ensemble::DecisionTreeEnsemble(
        encoding, hyperparameters, peripheral, population );

    // ------------------------------------------------------------------------
    // Fit model.

    model.fit( population_df, {peripheral_df} );

    // ------------------------------------------------------------------------
    // Reload model.

    model.save( "../../tests/test20/Model.json" );

    const auto model_json = load_json( "../../tests/test20/Model.json" );

    auto model2 =
        relboost::ensemble::DecisionTreeEnsemble( encoding, *model_json );

    model2.save( "../../tests/test20/Model2.json" );

    const auto model2_json = load_json( "../../tests/test20/Model.json" );

    auto model3 =
        relboost::ensemble::DecisionTreeEnsemble( encoding, *model2_json );

    // ------------------------------------------------------------------------
    // Express as SQL code.

    std::ofstream sql( "../../tests/test20/Model.sql" );
    sql << model.to_sql();
    sql.close();

    // ------------------------------------------------------------------------
    // Generate predictions.

    const auto predictions = model.predict( population_df, {peripheral_df} );

    const auto predictions2 = model2.predict( population_df, {peripheral_df} );

    const auto predictions3 = model3.predict( population_df, {peripheral_df} );

    assert( predictions.size() == predictions2.size() );

    for ( size_t i = 0; i < predictions.size(); ++i )
        {
            // std::cout << "prediction: " << predictions[i]
            //          << ", prediction2: " << predictions2[i] << std::endl;

            assert( std::abs( predictions[i] - predictions2[i] ) < 1e-07 );

            assert( std::abs( predictions[i] - predictions3[i] ) < 1e-07 );
        }
    std::cout << std::endl << std::endl;

    // ------------------------------------------------------------------------

    std::cout << "OK." << std::endl << std::endl;

    // ------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
