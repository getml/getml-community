

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
        autosql::containers::Column<std::int32_t>(
            join_key2_peripheral2.data(),
            "join_key2",
            join_key2_peripheral2.size() );

    auto numerical_peripheral2 = make_column<double>( 5000, rng );

    const auto numerical_peripheral2_col = autosql::containers::Column<double>(
        numerical_peripheral2.data(),
        "column_01",
        numerical_peripheral2.size() );

    const auto time_stamp2_peripheral2 = make_column<double>( 5000, rng );

    const auto time_stamp2_peripheral2_col =
        autosql::containers::Column<double>(
            time_stamp2_peripheral2.data(),
            "time_stamp2",
            time_stamp2_peripheral2.size() );

    const auto peripheral2_df = autosql::containers::DataFrame(
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
        autosql::containers::Column<std::int32_t>(
            join_key1_peripheral1.data(),
            "join_key1",
            join_key1_peripheral1.size() );

    const auto join_key2_peripheral1 = make_column<std::int32_t>( 5000, rng );

    const auto join_key2_peripheral1_col =
        autosql::containers::Column<std::int32_t>(
            join_key2_peripheral1.data(),
            "join_key2",
            join_key2_peripheral1.size() );

    auto numerical_peripheral1 = make_column<double>( 5000, rng );

    const auto numerical_peripheral1_col = autosql::containers::Column<double>(
        numerical_peripheral1.data(),
        "column_01",
        numerical_peripheral1.size() );

    const auto time_stamp1_peripheral1 = make_column<double>( 5000, rng );

    const auto time_stamp1_peripheral1_col =
        autosql::containers::Column<double>(
            time_stamp1_peripheral1.data(),
            "time_stamp1",
            time_stamp1_peripheral1.size() );

    const auto time_stamp2_peripheral1 = make_column<double>( 5000, rng );

    const auto time_stamp2_peripheral1_col =
        autosql::containers::Column<double>(
            time_stamp2_peripheral1.data(),
            "time_stamp2",
            time_stamp2_peripheral1.size() );

    const auto peripheral1_df = autosql::containers::DataFrame(
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
            join_keys_population[i] = i;
        }

    const auto join_keys_population_col =
        autosql::containers::Column<std::int32_t>(
            join_keys_population.data(),
            "join_key1",
            join_keys_population.size() );

    auto numerical_population = make_column<double>( 500, rng );

    const auto numerical_population_col = autosql::containers::Column<double>(
        numerical_population.data(), "column_01", numerical_population.size() );

    const auto time_stamps_population = make_column<double>( 500, rng );

    const auto time_stamps_population_col = autosql::containers::Column<double>(
        time_stamps_population.data(),
        "time_stamp1",
        time_stamps_population.size() );

    auto targets_population = std::vector<double>( 500 );

    const auto target_population_col = autosql::containers::Column<double>(
        targets_population.data(), "target", targets_population.size() );

    const auto population_df = autosql::containers::DataFrame(
        {},
        {},
        {join_keys_population_col},
        "POPULATION",
        {numerical_population_col},
        {target_population_col},
        {time_stamps_population_col} );

    // ---------------------------------------------
    // Define targets.

    for ( size_t i = 0; i < peripheral1_df.nrows(); ++i )
        {
            const auto jk = join_key1_peripheral1[i];

            assert( jk < 500 );

            if ( time_stamp1_peripheral1[i] <= time_stamps_population[jk] &&
                 peripheral1_df.numerical( i, 0 ) < 250.0 )
                {
                    targets_population[jk]++;
                }
        }

    // ---------------------------------------------
    // Build data model.

    const auto population_json =
        load_json( "../../tests/autosql/test21/schema.json" );

    const auto population =
        std::make_shared<const autosql::decisiontrees::Placeholder>(
            *population_json );

    const auto peripheral = std::make_shared<std::vector<std::string>>(
        std::vector<std::string>{"PERIPHERAL1", "PERIPHERAL2"} );

    // ------------------------------------------------------------------------
    // Load hyperparameters.

    const auto hyperparameters_json =
        load_json( "../../tests/autosql/test21/hyperparameters.json" );

    std::cout << autosql::JSON::stringify( *hyperparameters_json ) << std::endl
              << std::endl;

    const auto hyperparameters =
        std::make_shared<autosql::descriptors::Hyperparameters>(
            *hyperparameters_json );

    // ------------------------------------------------------------------------
    // Build model

    const auto encoding = std::make_shared<const std::vector<std::string>>(
        std::vector<std::string>(
            {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"} ) );

    auto model = autosql::ensemble::DecisionTreeEnsemble(
        encoding, hyperparameters, peripheral, population );

    // ------------------------------------------------------------------------
    // Fit model.

    model.fit( population_df, {peripheral1_df, peripheral2_df} );

    model.save( "../../tests/autosql/test21/Model.json" );

    // ------------------------------------------------------------------------
    // Express as SQL code.

    std::ofstream sql( "../../tests/autosql/test21/Model.sql" );
    sql << model.to_sql();
    sql.close();

    // ------------------------------------------------------------------------
    // Generate predictions.

    const auto predictions =
        *model.transform( population_df, {peripheral1_df, peripheral2_df} );

    const auto num_features = hyperparameters->num_features_;

    for ( size_t i = 0; i < predictions.size(); ++i )
        {
            /*std::cout << "target: "
                       << population_df.target( i / num_features, 0 )
                       << ", prediction: " << predictions[i] << std::endl;*/

            assert(
                std::abs(
                    population_df.target( i / num_features, 0 ) -
                    predictions[i] ) < 5.0 );
        }
    std::cout << std::endl << std::endl;

    // ------------------------------------------------------------------------

    std::cout << "OK." << std::endl << std::endl;

    // ------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
