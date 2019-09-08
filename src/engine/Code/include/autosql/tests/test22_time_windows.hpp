
// ---------------------------------------------------------------------------

void test22_time_windows()
{
    // ------------------------------------------------------------------------

    std::cout << std::endl
              << "Test 22 (time windows): " << std::endl
              << std::endl;

    // ------------------------------------------------------------------------
    // Build artificial data set.

    auto rng = std::mt19937( 100 );

    // ------------------------------------------------------------------------
    // Build peripheral table.

    const auto join_keys_peripheral = make_column<std::int32_t>( 250000, rng );

    const auto join_keys_peripheral_col =
        autosql::containers::Column<std::int32_t>(
            join_keys_peripheral.data(),
            "join_key",
            join_keys_peripheral.size() );

    auto numerical_peripheral = make_column<double>( 250000, rng );

    const auto numerical_peripheral_col = autosql::containers::Column<double>(
        numerical_peripheral.data(), "column_01", numerical_peripheral.size() );

    const auto time_stamps_peripheral = make_column<double>( 250000, rng );

    const auto time_stamps_peripheral_col = autosql::containers::Column<double>(
        time_stamps_peripheral.data(),
        "time_stamp",
        time_stamps_peripheral.size() );

    const auto peripheral_df = autosql::containers::DataFrame(
        {},
        {},
        {join_keys_peripheral_col},
        "PERIPHERAL",
        {numerical_peripheral_col},
        {},
        {time_stamps_peripheral_col} );

    // ------------------------------------------------------------------------
    // Build population table.

    auto join_keys_population = std::vector<std::int32_t>( 500 );

    for ( size_t i = 0; i < join_keys_population.size(); ++i )
        {
            join_keys_population[i] = static_cast<std::int32_t>( i );
        }

    const auto join_keys_population_col =
        autosql::containers::Column<std::int32_t>(
            join_keys_population.data(),
            "join_key",
            join_keys_population.size() );

    auto numerical_population = make_column<double>( 500, rng );

    const auto numerical_population_col = autosql::containers::Column<double>(
        numerical_population.data(), "column_01", numerical_population.size() );

    const auto time_stamps_population = make_column<double>( 500, rng );

    const auto time_stamps_population_col = autosql::containers::Column<double>(
        time_stamps_population.data(),
        "time_stamp",
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

    for ( size_t i = 0; i < peripheral_df.nrows(); ++i )
        {
            const auto jk = peripheral_df.join_key( i );

            assert_true( jk < 500 );

            if ( ( time_stamps_population_col[jk] -
                       peripheral_df.time_stamp( i ) >
                   30.0 ) &&
                 ( time_stamps_population_col[jk] -
                       peripheral_df.time_stamp( i ) <=
                   60.0 ) )
                {
                    targets_population[jk]++;
                }
        }

    // ---------------------------------------------
    // Build data model.

    const auto population_json =
        load_json( "../../tests/autosql/test22/schema.json" );

    const auto population =
        std::make_shared<const autosql::decisiontrees::Placeholder>(
            *population_json );

    const auto peripheral = std::make_shared<std::vector<std::string>>(
        std::vector<std::string>{"PERIPHERAL"} );

    // ------------------------------------------------------------------------
    // Load hyperparameters.

    const auto hyperparameters_json =
        load_json( "../../tests/autosql/test22/hyperparameters.json" );

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

    model.fit( population_df, {peripheral_df} );

    model.save( "../../tests/autosql/test22/Model.json" );

    // ------------------------------------------------------------------------
    // Express as SQL code.

    std::ofstream sql( "../../tests/autosql/test22/Model.sql" );
    sql << model.to_sql();
    sql.close();

    // ------------------------------------------------------------------------
    // Generate and evaluate predictions.

    const auto predictions = model.transform( population_df, {peripheral_df} );

    for ( size_t j = 0; j < predictions.size(); ++j )
        {
            for ( size_t i = 0; i < predictions[j]->size(); ++i )
                {
                    /*std::cout << "target: " << population_df.target( i, 0 )
                              << ", prediction: " << ( *predictions[j] )[i]
                              << std::endl;*/

                    assert_true(
                        std::abs(
                            population_df.target( i, 0 ) -
                            ( *predictions[j] )[i] ) < 5.0 );
                }
        }
    std::cout << std::endl << std::endl;

    // ------------------------------------------------------------------------

    std::cout << "OK." << std::endl << std::endl;

    // ------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

