
// ---------------------------------------------------------------------------

void test5_multiple_categorical( const std::filesystem::path _test_path )
{
    // ------------------------------------------------------------------------

    auto test5_path = _test_path;

    test5_path.append( "relcit" ).append( "test5" );

    // ------------------------------------------------------------------------

    std::cout << "Test 5 | COUNT aggregation, multiple categorical\t\t\t\t";

    // ------------------------------------------------------------------------
    // Build artificial data set.

    auto rng = std::mt19937( 100 );

    // ------------------------------------------------------------------------
    // Build peripheral table.

    const auto categorical_peripheral =
        make_categorical_column<std::int32_t>( 250000, rng );

    const auto categorical_peripheral_col =
        relcit::containers::Column<std::int32_t>(
            categorical_peripheral.data(),
            "column_01",
            categorical_peripheral.size() );

    const auto join_keys_peripheral = make_column<std::int32_t>( 250000, rng );

    const auto join_keys_peripheral_col =
        relcit::containers::Column<std::int32_t>(
            join_keys_peripheral.data(),
            "join_key",
            join_keys_peripheral.size() );

    auto numerical_peripheral = make_column<double>( 250000, rng );

    const auto numerical_peripheral_col = relcit::containers::Column<double>(
        numerical_peripheral.data(), "column_01", numerical_peripheral.size() );

    const auto time_stamps_peripheral = make_column<double>( 250000, rng );

    const auto time_stamps_peripheral_col = relcit::containers::Column<double>(
        time_stamps_peripheral.data(),
        "time_stamp",
        time_stamps_peripheral.size() );

    const auto peripheral_df = relcit::containers::DataFrame(
        { categorical_peripheral_col },
        {},
        { join_keys_peripheral_col },
        "PERIPHERAL",
        { numerical_peripheral_col },
        {},
        { time_stamps_peripheral_col } );

    // ------------------------------------------------------------------------
    // Build population table.

    auto join_keys_population = std::vector<std::int32_t>( 500 );

    for ( size_t i = 0; i < join_keys_population.size(); ++i )
        {
            join_keys_population[i] = static_cast<std::int32_t>( i );
        }

    const auto join_keys_population_col =
        relcit::containers::Column<std::int32_t>(
            join_keys_population.data(),
            "join_key",
            join_keys_population.size() );

    auto numerical_population = make_column<double>( 500, rng );

    const auto numerical_population_col = relcit::containers::Column<double>(
        numerical_population.data(), "column_01", numerical_population.size() );

    const auto time_stamps_population = make_column<double>( 500, rng );

    const auto time_stamps_population_col = relcit::containers::Column<double>(
        time_stamps_population.data(),
        "time_stamp",
        time_stamps_population.size() );

    auto targets_population = std::vector<double>( 500 );

    const auto target_population_col = relcit::containers::Column<double>(
        targets_population.data(), "target", targets_population.size() );

    const auto population_df = relcit::containers::DataFrame(
        {},
        {},
        { join_keys_population_col },
        "POPULATION",
        { numerical_population_col },
        { target_population_col },
        { time_stamps_population_col } );

    // ---------------------------------------------
    // Define targets.

    for ( size_t i = 0; i < peripheral_df.nrows(); ++i )
        {
            const auto jk = peripheral_df.join_key( i );

            assert_true( jk < 500 );

            const auto val = peripheral_df.categorical( i, 0 );

            if ( peripheral_df.time_stamp( i ) <=
                     time_stamps_population_col[jk] &&
                 ( val == 2 || val == 3 || val == 8 ) )
                {
                    ++targets_population[jk];
                }
        }

    // ---------------------------------------------
    // Build data model.

    // Append all subfolders to reach the required file. This
    // appending will have a persistent effect of _test_path which
    // is stored on the heap. After setting it once to the correct
    // folder only the filename has to be replaced.

    auto schema_path = test5_path;
    schema_path.append( "schema.json" );

    const auto population_json = load_json( schema_path.string() );

    const auto population =
        std::make_shared<const relcit::containers::Placeholder>(
            *population_json );

    const auto peripheral = std::make_shared<std::vector<std::string>>(
        std::vector<std::string>{ "PERIPHERAL" } );

    // ------------------------------------------------------------------------
    // Load hyperparameters.

    auto hyperparameters_path = test5_path;
    hyperparameters_path.append( "hyperparameters.json" );

    const auto hyperparameters_json =
        load_json( hyperparameters_path.string() );

    // std::cout << relcit::JSON::stringify( *hyperparameters_json ) <<
    // std::endl
    //           << std::endl;

    const auto hyperparameters =
        std::make_shared<const relcit::Hyperparameters>(
            *hyperparameters_json );

    // ------------------------------------------------------------------------
    // Build model

    const auto encoding = std::make_shared<const std::vector<strings::String>>(
        std::vector<strings::String>(
            { "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10" } ) );

    auto model = relcit::ensemble::DecisionTreeEnsemble(
        hyperparameters, peripheral, population );

    // ------------------------------------------------------------------------

    model.fit( population_df, { peripheral_df } );

    // ------------------------------------------------------------------------

    auto model_path = test5_path;
    model_path.append( "model.json" );

    model.save( model_path.string() );

    // ------------------------------------------------------------------------
    // Express as SQL code.

    auto sql_path = test5_path;
    sql_path.append( "model.sql" );

    std::ofstream sql( sql_path.string() );
    const auto vec = model.to_sql( encoding );
    for ( const auto& str : vec ) sql << str;
    sql.close();

    // ------------------------------------------------------------------------
    // Generate predictions.

    const auto predictions = model.predict( population_df, { peripheral_df } );

    assert_true( predictions.size() == population_df.nrows() );

    for ( size_t i = 0; i < predictions.size(); ++i )
        {
            /*std::cout << "target: " << population_df.target( i, 0 )
                      << ", prediction: " << predictions[i] << std::endl;*/

            assert_true(
                std::abs( population_df.target( i, 0 ) - predictions[i] ) <
                7.0 );
        }

    // ------------------------------------------------------------------------

    std::cout << "| OK" << std::endl;

    // ------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
