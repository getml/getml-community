
// ---------------------------------------------------------------------------

void test19_classification( std::filesystem::path _test_path )
{
    // ------------------------------------------------------------------------

    std::cout << std::endl
              << "Test 19 (classification): " << std::endl
              << std::endl;

    // ------------------------------------------------------------------------
    // Build artificial data set.

    auto rng = std::mt19937( 100 );

    // ------------------------------------------------------------------------
    // Build peripheral table.

    const auto join_keys_peripheral = make_column<std::int32_t>( 250000, rng );

    const auto join_keys_peripheral_col =
        relboost::containers::Column<std::int32_t>(
            join_keys_peripheral.data(),
            "join_key",
            join_keys_peripheral.size() );

    auto numerical_peripheral = make_column<double>( 250000, rng );

    const auto numerical_peripheral_col = relboost::containers::Column<double>(
        numerical_peripheral.data(), "column_01", numerical_peripheral.size() );

    const auto time_stamps_peripheral = make_column<double>( 250000, rng );

    const auto time_stamps_peripheral_col =
        relboost::containers::Column<double>(
            time_stamps_peripheral.data(),
            "time_stamp",
            time_stamps_peripheral.size() );

    const auto peripheral_df = relboost::containers::DataFrame(
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
        relboost::containers::Column<std::int32_t>(
            join_keys_population.data(),
            "join_key",
            join_keys_population.size() );

    auto numerical_population = make_column<double>( 500, rng );

    const auto numerical_population_col = relboost::containers::Column<double>(
        numerical_population.data(), "column_01", numerical_population.size() );

    const auto time_stamps_population = make_column<double>( 500, rng );

    const auto time_stamps_population_col =
        relboost::containers::Column<double>(
            time_stamps_population.data(),
            "time_stamp",
            time_stamps_population.size() );

    auto targets_population = std::vector<double>( 500 );

    const auto target_population_col = relboost::containers::Column<double>(
        targets_population.data(), "target", targets_population.size() );

    const auto population_df = relboost::containers::DataFrame(
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

    for ( auto& val : targets_population )
        {
            val = ( val > 100.0 ? 1.0 : 0.0 );
        }

    // ---------------------------------------------
    // Build data model.

    // Append all subfolders to reach the required file. This
    // appending will have a persistent effect of _test_path which
    // is stored on the heap. After setting it once to the correct
    // folder only the filename has to be replaced.
    _test_path.append( "relboost" ).append( "test19" ).append( "schema.json" );
    const auto population_json = load_json( _test_path.string() );

    const auto population =
        std::make_shared<const relboost::ensemble::Placeholder>(
            *population_json );

    const auto peripheral = std::make_shared<std::vector<std::string>>(
        std::vector<std::string>{"PERIPHERAL"} );

    // ------------------------------------------------------------------------
    // Load hyperparameters.

    const auto hyperparameters_json = load_json(
        _test_path.replace_filename( "hyperparameters.json" ).string() );

    std::cout << relboost::JSON::stringify( *hyperparameters_json ) << std::endl
              << std::endl;

    const auto hyperparameters =
        std::make_shared<const relboost::Hyperparameters>(
            *hyperparameters_json );

    // ------------------------------------------------------------------------
    // Build model

    const auto encoding = std::make_shared<const std::vector<strings::String>>(
        std::vector<strings::String>(
            {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"} ) );

    auto model = relboost::ensemble::DecisionTreeEnsemble(
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
    // Generate predictions.

    const auto predictions = model.predict( population_df, {peripheral_df} );

    assert_true( predictions.size() == population_df.nrows() );

    relboost::Float accuracy = 0.0;

    for ( size_t i = 0; i < predictions.size(); ++i )
        {
            // std::cout << "target: " << population_df.target_[i]
            //           << ", prediction: " << predictions[i] << std::endl;

            if ( ( predictions[i] > 0.5 &&
                   population_df.target( i, 0 ) == 1.0 ) ||
                 ( predictions[i] < 0.5 &&
                   population_df.target( i, 0 ) == 0.0 ) )
                {
                    ++accuracy;
                }
        }

    accuracy /= 500.0;

    std::cout << " accuracy: " << accuracy << std::endl;

    assert_true( accuracy > 0.95 );

    std::cout << std::endl << std::endl;

    // ------------------------------------------------------------------------

    std::cout << "OK." << std::endl << std::endl;

    // ------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
