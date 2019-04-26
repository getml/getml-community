
// ---------------------------------------------------------------------------

void test6_categorical()
{
    // ------------------------------------------------------------------------

    std::cout << std::endl
              << "Test 6 (AVG aggregation with categorical variables): "
              << std::endl
              << std::endl;

    // ------------------------------------------------------------------------
    // Build artificial data set.

    auto rng = std::mt19937( 100 );

    // ------------------------------------------------------------------------
    // Build peripheral table.

    const auto categorical_peripheral =
        make_categorical_column<std::int32_t>( 250000, rng );

    const auto categorical_peripheral_mat =
        relboost::containers::Matrix<std::int32_t>(
            categorical_peripheral.data(),
            "column_01",
            categorical_peripheral.size() );

    const auto join_keys_peripheral = make_column<std::int32_t>( 250000, rng );

    const auto join_keys_peripheral_mat =
        relboost::containers::Matrix<std::int32_t>(
            join_keys_peripheral.data(),
            "join_key",
            join_keys_peripheral.size() );

    const auto time_stamps_peripheral = make_column<double>( 250000, rng );

    const auto time_stamps_peripheral_mat =
        relboost::containers::Matrix<double>(
            time_stamps_peripheral.data(),
            "time_stamp",
            time_stamps_peripheral.size() );

    const auto peripheral_df = relboost::containers::DataFrame(
        {categorical_peripheral_mat},
        {},
        {join_keys_peripheral_mat},
        "PERIPHERAL",
        {},
        {},
        {time_stamps_peripheral_mat} );

    // ------------------------------------------------------------------------
    // Build population table.

    const auto categorical_population =
        make_categorical_column<std::int32_t>( 500, rng );

    const auto categorical_population_mat =
        relboost::containers::Matrix<std::int32_t>(
            categorical_population.data(), "column_01", 500 );

    auto join_keys_population = std::vector<std::int32_t>( 500 );

    for ( size_t i = 0; i < join_keys_population.size(); ++i )
        {
            join_keys_population[i] = i;
        }

    const auto join_keys_population_mat =
        relboost::containers::Matrix<std::int32_t>(
            join_keys_population.data(),
            "join_key",
            join_keys_population.size() );

    const auto time_stamps_population = make_column<double>( 500, rng );

    const auto time_stamps_population_mat =
        relboost::containers::Matrix<double>(
            time_stamps_population.data(),
            "time_stamp",
            time_stamps_population.size() );

    auto targets_population = std::vector<double>( 500 );

    const auto target_population_mat = relboost::containers::Matrix<double>(
        targets_population.data(), "target", targets_population.size() );

    const auto population_df = relboost::containers::DataFrame(
        {categorical_population_mat},
        {},
        {join_keys_population_mat},
        "POPULATION",
        {},
        {target_population_mat},
        {time_stamps_population_mat} );

    // ---------------------------------------------
    // Define targets.

    auto counts = std::vector<double>( 500 );

    for ( size_t i = 0; i < peripheral_df.nrows(); ++i )
        {
            const auto jk = peripheral_df.join_key( i );

            assert( jk < 500 );

            if ( peripheral_df.time_stamp( i ) <=
                 time_stamps_population_mat[jk] )
                {
                    counts[jk]++;
                }
        }

    for ( size_t i = 0; i < peripheral_df.nrows(); ++i )
        {
            const auto jk = peripheral_df.join_key( i );

            assert( jk < 500 );

            if ( peripheral_df.time_stamp( i ) <=
                 time_stamps_population_mat[jk] )
                {
                    if ( peripheral_df.categorical( i, 0 ) == 3 )
                        {
                            targets_population[jk] += 300.0 / counts[jk];
                        }
                    else
                        {
                            targets_population[jk] += 1000.0 / counts[jk];
                        }
                }
        }

    // ---------------------------------------------
    // Build data model.

    const auto population_json = load_json( "../../tests/test6/schema.json" );

    const auto population =
        std::make_shared<const relboost::ensemble::Placeholder>(
            *population_json );

    const auto peripheral = std::make_shared<std::vector<std::string>>(
        std::vector<std::string>{"PERIPHERAL"} );

    // ------------------------------------------------------------------------
    // Load hyperparameters.

    const auto hyperparameters_json =
        load_json( "../../tests/test6/hyperparameters.json" );

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

    model.save( "../../tests/test6/Model.json" );

    // ------------------------------------------------------------------------
    // Express as SQL code.

    std::ofstream sql( "../../tests/test6/Model.sql" );
    sql << model.to_sql();
    sql.close();

    // ------------------------------------------------------------------------
    // Generate predictions.

    const auto predictions = model.predict( population_df, {peripheral_df} );

    assert( predictions.size() == population_df.nrows() );

    for ( size_t i = 0; i < predictions.size(); ++i )
        {
            assert(
                std::abs( population_df.target( i, 0 ) - predictions[i] ) <
                10.0 );
        }
    std::cout << std::endl << std::endl;

    // ------------------------------------------------------------------------

    std::cout << "OK." << std::endl << std::endl;

    // ------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
