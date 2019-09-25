

// ---------------------------------------------------------------------------

void test21_snowflake_model()
{
    // ------------------------------------------------------------------------

    std::cout << std::endl
              << "Test 21 (snowflake model): " << std::endl
              << std::endl;

    // ------------------------------------------------------------------------
    // Build artificial data set.

    auto rng = std::mt19937( 100 );

    // ------------------------------------------------------------------------
    // Build peripheral table 2.

    const auto join_key2_peripheral2 = make_column<std::int32_t>( 5000, rng );

    const auto join_key2_peripheral2_col =
        multirel::containers::Column<std::int32_t>(
            join_key2_peripheral2.data(),
            "join_key2",
            join_key2_peripheral2.size() );

    auto numerical_peripheral2 = make_column<double>( 5000, rng );

    const auto numerical_peripheral2_col = multirel::containers::Column<double>(
        numerical_peripheral2.data(),
        "column_01",
        numerical_peripheral2.size() );

    const auto time_stamp2_peripheral2 = make_column<double>( 5000, rng );

    const auto time_stamp2_peripheral2_col =
        multirel::containers::Column<double>(
            time_stamp2_peripheral2.data(),
            "time_stamp2",
            time_stamp2_peripheral2.size() );

    const auto peripheral2_df = multirel::containers::DataFrame(
        {},
        {},
        {join_key2_peripheral2_col},
        "PERIPHERAL2",
        {numerical_peripheral2_col},
        {},
        {time_stamp2_peripheral2_col} );

    // ------------------------------------------------------------------------
    // Build peripheral table 1.

    const auto join_key1_peripheral1 = make_column<std::int32_t>( 5000, rng );

    const auto join_key1_peripheral1_col =
        multirel::containers::Column<std::int32_t>(
            join_key1_peripheral1.data(),
            "join_key1",
            join_key1_peripheral1.size() );

    const auto join_key2_peripheral1 = make_column<std::int32_t>( 5000, rng );

    const auto join_key2_peripheral1_col =
        multirel::containers::Column<std::int32_t>(
            join_key2_peripheral1.data(),
            "join_key2",
            join_key2_peripheral1.size() );

    auto numerical_peripheral1 = make_column<double>( 5000, rng );

    const auto numerical_peripheral1_col = multirel::containers::Column<double>(
        numerical_peripheral1.data(),
        "column_01",
        numerical_peripheral1.size() );

    const auto time_stamp1_peripheral1 = make_column<double>( 5000, rng );

    const auto time_stamp1_peripheral1_col =
        multirel::containers::Column<double>(
            time_stamp1_peripheral1.data(),
            "time_stamp1",
            time_stamp1_peripheral1.size() );

    const auto time_stamp2_peripheral1 = make_column<double>( 5000, rng );

    const auto time_stamp2_peripheral1_col =
        multirel::containers::Column<double>(
            time_stamp2_peripheral1.data(),
            "time_stamp2",
            time_stamp2_peripheral1.size() );

    const auto peripheral1_df = multirel::containers::DataFrame(
        {},
        {},
        {join_key1_peripheral1_col, join_key2_peripheral1_col},
        "PERIPHERAL1",
        {numerical_peripheral1_col},
        {},
        {time_stamp1_peripheral1_col, time_stamp2_peripheral1_col} );

    // ------------------------------------------------------------------------
    // Build population table.

    auto join_keys_population = std::vector<std::int32_t>( 500 );

    for ( size_t i = 0; i < join_keys_population.size(); ++i )
        {
            join_keys_population[i] = static_cast<std::int32_t>( i );
        }

    const auto join_keys_population_col =
        multirel::containers::Column<std::int32_t>(
            join_keys_population.data(),
            "join_key1",
            join_keys_population.size() );

    auto numerical_population = make_column<double>( 500, rng );

    const auto numerical_population_col = multirel::containers::Column<double>(
        numerical_population.data(), "column_01", numerical_population.size() );

    const auto time_stamps_population = make_column<double>( 500, rng );

    const auto time_stamps_population_col = multirel::containers::Column<double>(
        time_stamps_population.data(),
        "time_stamp1",
        time_stamps_population.size() );

    auto targets_population = std::vector<double>( 500 );

    const auto target_population_col = multirel::containers::Column<double>(
        targets_population.data(), "target", targets_population.size() );

    const auto population_df = multirel::containers::DataFrame(
        {},
        {},
        {join_keys_population_col},
        "POPULATION",
        {numerical_population_col},
        {target_population_col},
        {time_stamps_population_col} );

    // ---------------------------------------------
    // Define subtargets.

    auto subtargets = std::vector<double>( peripheral1_df.nrows() );

    for ( size_t i = 0; i < peripheral2_df.nrows(); ++i )
        {
            if ( numerical_peripheral2[i] < 250.0 )
                {
                    for ( size_t j = 0; j < peripheral1_df.nrows(); ++j )
                        {
                            if ( join_key2_peripheral2[i] ==
                                     join_key2_peripheral1[j] &&
                                 time_stamp2_peripheral2[i] <=
                                     time_stamp2_peripheral1[j] )
                                {
                                    subtargets[j]++;
                                }
                        }
                }
        }

    // ---------------------------------------------
    // Define targets.

    for ( size_t i = 0; i < peripheral1_df.nrows(); ++i )
        {
            const auto jk = join_key1_peripheral1[i];

            assert_true( jk < 500 );

            if ( time_stamp1_peripheral1[i] <= time_stamps_population[jk] )
                {
                    targets_population[jk] += subtargets[i];
                }
        }

    // ---------------------------------------------
    // Build data model.

    const auto population_json =
        load_json( "../../tests/multirel/test21/schema.json" );

    const auto population =
        std::make_shared<const multirel::decisiontrees::Placeholder>(
            *population_json );

    const auto peripheral = std::make_shared<std::vector<std::string>>(
        std::vector<std::string>{"PERIPHERAL1", "PERIPHERAL2"} );

    // ------------------------------------------------------------------------
    // Load hyperparameters.

    const auto hyperparameters_json =
        load_json( "../../tests/multirel/test21/hyperparameters.json" );

    std::cout << multirel::JSON::stringify( *hyperparameters_json ) << std::endl
              << std::endl;

    const auto hyperparameters =
        std::make_shared<multirel::descriptors::Hyperparameters>(
            *hyperparameters_json );

    // ------------------------------------------------------------------------
    // Build model

    const auto encoding = std::make_shared<const std::vector<std::string>>(
        std::vector<std::string>(
            {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"} ) );

    auto model = multirel::ensemble::DecisionTreeEnsemble(
        encoding, hyperparameters, peripheral, population );

    // ------------------------------------------------------------------------
    // Fit model.

    model.fit( population_df, {peripheral1_df, peripheral2_df} );

    model.save( "../../tests/multirel/test21/Model.json" );

    // ------------------------------------------------------------------------
    // Express as SQL code.

    std::ofstream sql( "../../tests/multirel/test21/Model.sql" );
    sql << model.to_sql();
    sql.close();

    // ------------------------------------------------------------------------
    // Generate predictions.

    const auto predictions =
        model.transform( population_df, {peripheral1_df, peripheral2_df} );

    for ( size_t j = 0; j < predictions.size(); ++j )
        {
            for ( size_t i = 0; i < predictions[j]->size(); ++i )
                {
                    /*std::cout << "target: "
                               << population_df.target( i , 0 )
                               << ", prediction: " << predictions[j][i] <<
                       std::endl;*/

                    assert_true(
                        std::abs(
                            population_df.target( i, 0 ) -
                            ( *predictions[j] )[i] ) < 10.0 );
                }
        }
    std::cout << std::endl << std::endl;

    // ------------------------------------------------------------------------

    std::cout << "OK." << std::endl << std::endl;

    // ------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

