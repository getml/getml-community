
// ---------------------------------------------------------------------------

void test13_categorical_output()
{
    // ------------------------------------------------------------------------

    std::cout << std::endl
              << "Test 13 (categorical output): " << std::endl
              << std::endl;

    // ------------------------------------------------------------------------
    // Build artificial data set.

    auto rng = std::mt19937( 100 );

    // ------------------------------------------------------------------------
    // Build peripheral table.

    const auto categorical_peripheral =
        make_categorical_column<std::int32_t>( 250000, rng );

    const auto categorical_peripheral_col =
        autosql::containers::Column<std::int32_t>(
            categorical_peripheral.data(),
            "column_01",
            categorical_peripheral.size(),
            "unit_01" );

    const auto join_keys_peripheral = make_column<std::int32_t>( 250000, rng );

    const auto join_keys_peripheral_col =
        autosql::containers::Column<std::int32_t>(
            join_keys_peripheral.data(),
            "join_key",
            join_keys_peripheral.size() );

    const auto time_stamps_peripheral = make_column<double>( 250000, rng );

    const auto time_stamps_peripheral_col = autosql::containers::Column<double>(
        time_stamps_peripheral.data(),
        "time_stamp",
        time_stamps_peripheral.size() );

    const auto peripheral_df = autosql::containers::DataFrame(
        {categorical_peripheral_col},
        {},
        {join_keys_peripheral_col},
        "PERIPHERAL",
        {},
        {},
        {time_stamps_peripheral_col} );

    // ------------------------------------------------------------------------
    // Build population table.

    const auto categorical_population =
        make_categorical_column<std::int32_t>( 500, rng );

    const auto categorical_population_col =
        autosql::containers::Column<std::int32_t>(
            categorical_population.data(), "column_01", 500, "unit_01" );

    auto join_keys_population = std::vector<std::int32_t>( 500 );

    for ( size_t i = 0; i < join_keys_population.size(); ++i )
        {
            join_keys_population[i] = i;
        }

    const auto join_keys_population_col =
        autosql::containers::Column<std::int32_t>(
            join_keys_population.data(),
            "join_key",
            join_keys_population.size() );

    const auto time_stamps_population = make_column<double>( 500, rng );

    const auto time_stamps_population_col = autosql::containers::Column<double>(
        time_stamps_population.data(),
        "time_stamp",
        time_stamps_population.size() );

    auto targets_population = std::vector<double>( 500 );

    const auto target_population_col = autosql::containers::Column<double>(
        targets_population.data(), "target", targets_population.size() );

    const auto population_df = autosql::containers::DataFrame(
        {categorical_population_col},
        {},
        {join_keys_population_col},
        "POPULATION",
        {},
        {target_population_col},
        {time_stamps_population_col} );

    // ---------------------------------------------
    // Define targets.

    for ( size_t i = 0; i < peripheral_df.nrows(); ++i )
        {
            const auto jk = peripheral_df.join_key( i );

            assert( jk < 500 );

            if ( peripheral_df.time_stamp( i ) <=
                 time_stamps_population_col[jk] )
                {
                    if ( peripheral_df.categorical( i, 0 ) == 3 ||
                         peripheral_df.categorical( i, 0 ) == 7 )
                        {
                            ++targets_population[jk];
                        }
                }
        }

    // ---------------------------------------------
    // Build data model.

    const auto population_json =
        load_json( "../../tests/autosql/test13/schema.json" );

    const auto population =
        std::make_shared<const autosql::decisiontrees::Placeholder>(
            *population_json );

    const auto peripheral = std::make_shared<std::vector<std::string>>(
        std::vector<std::string>{"PERIPHERAL"} );

    // ------------------------------------------------------------------------
    // Load hyperparameters.

    const auto hyperparameters_json =
        load_json( "../../tests/autosql/test13/hyperparameters.json" );

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

    model.save( "../../tests/autosql/test13/Model.json" );

    // ------------------------------------------------------------------------
    // Express as SQL code.

    std::ofstream sql( "../../tests/autosql/test13/Model.sql" );
    sql << model.to_sql();
    sql.close();

    // ------------------------------------------------------------------------
    // Generate predictions.

    const auto predictions = model.transform( population_df, {peripheral_df} );

    for ( size_t j = 0; j < predictions.size(); ++j )
        {
            for ( size_t i = 0; i < predictions[j]->size(); ++i )
                {
                    /*std::cout << "target: "
                               << population_df.target( i , 0 )
                               << ", prediction: " << predictions[j][i] <<
                       std::endl;*/

                    assert(
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
