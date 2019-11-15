
// ---------------------------------------------------------------------------

void test15_discrete_output( std::filesystem::path _test_path )
{
    // ------------------------------------------------------------------------

    std::cout << std::endl
              << "Test 15 (discrete output): " << std::endl
              << std::endl;

    // ------------------------------------------------------------------------
    // Build artificial data set.

    auto rng = std::mt19937( 100 );

    // ------------------------------------------------------------------------
    // Build peripheral table.

    auto discrete_peripheral = make_column<double>( 250000, rng );

    const auto discrete_peripheral_col = multirel::containers::Column<double>(
        discrete_peripheral.data(),
        "column_01",
        discrete_peripheral.size(),
        "unit_01" );

    const auto join_keys_peripheral = make_column<std::int32_t>( 250000, rng );

    const auto join_keys_peripheral_col =
        multirel::containers::Column<std::int32_t>(
            join_keys_peripheral.data(),
            "join_key",
            join_keys_peripheral.size() );

    const auto time_stamps_peripheral = make_column<double>( 250000, rng );

    const auto time_stamps_peripheral_col =
        multirel::containers::Column<double>(
            time_stamps_peripheral.data(),
            "time_stamp",
            time_stamps_peripheral.size() );

    const auto peripheral_df = multirel::containers::DataFrame(
        {},
        {discrete_peripheral_col},
        {join_keys_peripheral_col},
        "PERIPHERAL",
        {},
        {},
        {time_stamps_peripheral_col} );

    // ------------------------------------------------------------------------
    // Build population table.

    auto discrete_population = make_column<double>( 500, rng );

    const auto discrete_population_col = multirel::containers::Column<double>(
        discrete_population.data(),
        "column_01",
        discrete_population.size(),
        "unit_01" );

    auto join_keys_population = std::vector<std::int32_t>( 500 );

    for ( size_t i = 0; i < join_keys_population.size(); ++i )
        {
            join_keys_population[i] = static_cast<std::int32_t>( i );
        }

    const auto join_keys_population_col =
        multirel::containers::Column<std::int32_t>(
            join_keys_population.data(),
            "join_key",
            join_keys_population.size() );

    const auto time_stamps_population = make_column<double>( 500, rng );

    const auto time_stamps_population_col =
        multirel::containers::Column<double>(
            time_stamps_population.data(),
            "time_stamp",
            time_stamps_population.size() );

    auto targets_population = std::vector<double>( 500 );

    const auto target_population_col = multirel::containers::Column<double>(
        targets_population.data(), "target", targets_population.size() );

    const auto population_df = multirel::containers::DataFrame(
        {},
        {discrete_population_col},
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

            assert_true( jk < 500 );

            if ( peripheral_df.time_stamp( i ) <=
                 time_stamps_population_col[jk] )
                {
                    if ( discrete_population[jk] < 50.0 )
                        {
                            ++targets_population[jk];
                        }
                }
        }

    // ---------------------------------------------
    // Build data model.

    // Append all subfolders to reach the required file. This 
    // appending will have a persistent effect of _test_path which
    // is stored on the heap. After setting it once to the correct
    // folder only the filename has to be replaced.
    _test_path.append( "multirel" ).append( "test15" ).append( "schema.json" );
    const auto population_json = load_json( _test_path.string() );

    const auto population =
        std::make_shared<const multirel::decisiontrees::Placeholder>(
            *population_json );

    const auto peripheral = std::make_shared<std::vector<std::string>>(
        std::vector<std::string>{"PERIPHERAL"} );

    // ------------------------------------------------------------------------
    // Load hyperparameters.

    const auto hyperparameters_json =
        load_json( _test_path.replace_filename( "hyperparameters.json" ).string() );

    std::cout << multirel::JSON::stringify( *hyperparameters_json ) << std::endl
              << std::endl;

    const auto hyperparameters =
        std::make_shared<multirel::descriptors::Hyperparameters>(
            *hyperparameters_json );

    // ------------------------------------------------------------------------
    // Build model

    const auto encoding = std::make_shared<const std::vector<strings::String>>(
        std::vector<strings::String>(
            {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"} ) );

    auto model = multirel::ensemble::DecisionTreeEnsemble(
        encoding, hyperparameters, peripheral, population );

    // ------------------------------------------------------------------------
    // Fit model.

    model.fit( population_df, {peripheral_df} );

    model.save( _test_path.replace_filename( "Model.json" ).string() );

    // ------------------------------------------------------------------------
    // Express as SQL code.

    std::ofstream sql( _test_path.replace_filename( "Model.sql" ).string() );
    sql << model.to_sql();
    sql.close();

    // ------------------------------------------------------------------------

    std::cout << "OK." << std::endl << std::endl;

    // ------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
