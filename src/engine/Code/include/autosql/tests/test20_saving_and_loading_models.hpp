
// ---------------------------------------------------------------------------

void test20_saving_and_loading_models()
{
    // ------------------------------------------------------------------------

    std::cout << std::endl
              << "Test 20 (saving and loading models): " << std::endl
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

            if ( peripheral_df.time_stamp( i ) <=
                     time_stamps_population_col[jk] &&
                 peripheral_df.numerical( i, 0 ) < 250.0 )
                {
                    targets_population[jk]++;
                }
        }

    // ---------------------------------------------
    // Build data model.

    const auto population_json =
        load_json( "../../tests/autosql/test20/schema.json" );

    const auto population =
        std::make_shared<const autosql::decisiontrees::Placeholder>(
            *population_json );

    const auto peripheral = std::make_shared<std::vector<std::string>>(
        std::vector<std::string>{"PERIPHERAL"} );

    // ------------------------------------------------------------------------
    // Load hyperparameters.

    const auto hyperparameters_json =
        load_json( "../../tests/autosql/test20/hyperparameters.json" );

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

    // ------------------------------------------------------------------------
    // Express as SQL code.

    std::ofstream sql( "../../tests/autosql/test20/Model.sql" );
    sql << model.to_sql();
    sql.close();

    model.save( "../../tests/autosql/test20/Model.json" );

    // ------------------------------------------------------------------------
    // Reload model.

    const auto model_json =
        load_json( "../../tests/autosql/test20/Model.json" );

    auto model2 =
        autosql::ensemble::DecisionTreeEnsemble( encoding, *model_json );

    model2.save( "../../tests/autosql/test20/Model2.json" );

    std::ofstream sql2( "../../tests/autosql/test20/Model2.sql" );
    sql2 << model2.to_sql();
    sql2.close();

    const auto model2_json =
        load_json( "../../tests/autosql/test20/Model2.json" );

    auto model3 =
        autosql::ensemble::DecisionTreeEnsemble( encoding, *model2_json );

    std::ofstream sql3( "../../tests/autosql/test20/Model3.sql" );
    sql3 << model3.to_sql();
    sql3.close();

    // ------------------------------------------------------------------------
    // Generate predictions.

    const auto predictions = model.transform( population_df, {peripheral_df} );

    const auto predictions2 =
        model2.transform( population_df, {peripheral_df} );

    const auto predictions3 =
        model3.transform( population_df, {peripheral_df} );

    assert_true( predictions.size() == predictions2.size() );
    assert_true( predictions.size() == predictions3.size() );

    for ( size_t j = 0; j < predictions.size(); ++j )
        {
            assert_true( predictions[j]->size() == predictions2[j]->size() );
            assert_true( predictions[j]->size() == predictions3[j]->size() );

            for ( size_t i = 0; i < predictions[j]->size(); ++i )
                {
                    assert_true(
                        std::abs(
                            ( *predictions[j] )[i] - ( *predictions2[j] )[i] ) <
                        1e-07 );

                    assert_true(
                        std::abs(
                            ( *predictions[j] )[i] - ( *predictions3[j] )[i] ) <
                        1e-07 );

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
