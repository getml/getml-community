

// ---------------------------------------------------------------------------

void test6_snowflake_model( std::filesystem::path _test_path )
{
    // ------------------------------------------------------------------------

    std::cout << "Test 6 | snowflake model, SUM of SUM\t\t\t";

    // ---------------------------------------------------------------

    auto test6_path = _test_path;

    test6_path.append( "relmt" ).append( "test6" );

    // ------------------------------------------------------------------------
    // Build artificial data set.

    auto rng = std::mt19937( 100 );

    // ------------------------------------------------------------------------
    // Build peripheral table 2.

    const auto join_key2_peripheral2 = make_column<std::int32_t>( 5000, rng );

    const auto join_key2_peripheral2_col =
        relmt::containers::Column<std::int32_t>(
            join_key2_peripheral2.data(),
            "join_key2",
            join_key2_peripheral2.size() );

    auto numerical_peripheral2 = make_column<double>( 5000, rng );

    const auto numerical_peripheral2_col = relmt::containers::Column<double>(
        numerical_peripheral2.data(),
        "column_01",
        numerical_peripheral2.size() );

    const auto time_stamp2_peripheral2 = make_column<double>( 5000, rng );

    const auto time_stamp2_peripheral2_col = relmt::containers::Column<double>(
        time_stamp2_peripheral2.data(),
        "time_stamp2",
        time_stamp2_peripheral2.size() );

    const auto peripheral2_df = relmt::containers::DataFrame(
        {},
        {},
        { join_key2_peripheral2_col },
        "PERIPHERAL2",
        { numerical_peripheral2_col },
        {},
        { time_stamp2_peripheral2_col } );

    // ------------------------------------------------------------------------
    // Build peripheral table 1.

    const auto join_key1_peripheral1 = make_column<std::int32_t>( 5000, rng );

    const auto join_key1_peripheral1_col =
        relmt::containers::Column<std::int32_t>(
            join_key1_peripheral1.data(),
            "join_key1",
            join_key1_peripheral1.size() );

    const auto join_key2_peripheral1 = make_column<std::int32_t>( 5000, rng );

    const auto join_key2_peripheral1_col =
        relmt::containers::Column<std::int32_t>(
            join_key2_peripheral1.data(),
            "join_key2",
            join_key2_peripheral1.size() );

    auto numerical_peripheral1 = make_column<double>( 5000, rng );

    const auto numerical_peripheral1_col = relmt::containers::Column<double>(
        numerical_peripheral1.data(),
        "column_01",
        numerical_peripheral1.size() );

    const auto time_stamp1_peripheral1 = make_column<double>( 5000, rng );

    const auto time_stamp1_peripheral1_col = relmt::containers::Column<double>(
        time_stamp1_peripheral1.data(),
        "time_stamp1",
        time_stamp1_peripheral1.size() );

    const auto time_stamp2_peripheral1 = make_column<double>( 5000, rng );

    const auto time_stamp2_peripheral1_col = relmt::containers::Column<double>(
        time_stamp2_peripheral1.data(),
        "time_stamp2",
        time_stamp2_peripheral1.size() );

    const auto peripheral1_df = relmt::containers::DataFrame(
        {},
        {},
        { join_key1_peripheral1_col, join_key2_peripheral1_col },
        "PERIPHERAL1",
        { numerical_peripheral1_col },
        {},
        { time_stamp1_peripheral1_col, time_stamp2_peripheral1_col } );

    // ------------------------------------------------------------------------
    // Build population table.

    auto join_keys_population = std::vector<std::int32_t>( 500 );

    for ( size_t i = 0; i < join_keys_population.size(); ++i )
        {
            join_keys_population[i] = static_cast<std::int32_t>( i );
        }

    const auto join_keys_population_col =
        relmt::containers::Column<std::int32_t>(
            join_keys_population.data(),
            "join_key1",
            join_keys_population.size() );

    auto numerical_population = make_column<double>( 500, rng );

    const auto numerical_population_col = relmt::containers::Column<double>(
        numerical_population.data(), "column_01", numerical_population.size() );

    const auto time_stamps_population = make_column<double>( 500, rng );

    const auto time_stamps_population_col = relmt::containers::Column<double>(
        time_stamps_population.data(),
        "time_stamp1",
        time_stamps_population.size() );

    auto targets_population = std::vector<double>( 500 );

    const auto target_population_col = relmt::containers::Column<double>(
        targets_population.data(), "target", targets_population.size() );

    const auto population_df = relmt::containers::DataFrame(
        {},
        {},
        { join_keys_population_col },
        "POPULATION",
        { numerical_population_col },
        { target_population_col },
        { time_stamps_population_col } );

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

    auto schema_path = test6_path;
    schema_path.append( "schema.json" );

    const auto population_json = load_json( schema_path.string() );

    const auto population =
        std::make_shared<const relmt::containers::Placeholder>(
            *population_json );

    const auto peripheral = std::make_shared<std::vector<std::string>>(
        std::vector<std::string>{ "PERIPHERAL1", "PERIPHERAL2" } );

    // ------------------------------------------------------------------------
    // Load hyperparameters.

    auto hyperparameters_path = test6_path;
    hyperparameters_path.append( "hyperparameters.json" );

    // std::cout << relmt::JSON::stringify( *hyperparameters_json ) <<
    // std::endl
    //           << std::endl;

    const auto hyperparameters_json =
        load_json( hyperparameters_path.string() );

    const auto hyperparameters =
        std::make_shared<relmt::Hyperparameters>( *hyperparameters_json );

    // ------------------------------------------------------------------------
    // Build model

    const auto encoding = std::make_shared<const std::vector<strings::String>>(
        std::vector<strings::String>(
            { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10" } ) );

    auto model = relmt::ensemble::DecisionTreeEnsemble(
        hyperparameters, peripheral, population );

    // ------------------------------------------------------------------------
    // Fit model.

    model.fit( population_df, { peripheral1_df, peripheral2_df } );

    // ------------------------------------------------------------------------

    auto model_path = test6_path;
    model_path.append( "model.json" );

    model.save( model_path.string() );

    // ------------------------------------------------------------------------
    // Express as SQL code.

    auto sql_path = test6_path;
    sql_path.append( "model.sql" );

    std::ofstream sql( sql_path.string() );
    const auto vec = model.to_sql( encoding );
    for ( const auto& str : vec ) sql << str;
    sql.close();

    // ------------------------------------------------------------------------
    // Generate predictions.

    const auto predictions =
        model.predict( population_df, { peripheral1_df, peripheral2_df } );

    for ( size_t i = 0; i < predictions.size(); ++i )
        {
            /*std::cout << i << " target: " << population_df.target( i, 0 )
                      << ", prediction: " << predictions[i] << std::endl;*/

            assert_true(
                std::abs( population_df.target( i, 0 ) - predictions[i] ) <
                10.0 );
        }

    // ------------------------------------------------------------------------

    std::cout << "| OK" << std::endl;

    // ------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

