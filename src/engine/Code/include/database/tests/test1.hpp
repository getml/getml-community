#ifndef DATABASE_TESTS_TEST1_HPP_
#define DATABASE_TESTS_TEST1_HPP_

void test1( std::filesystem::path _test_path )
{
    std::cout << "Test 1 | Parsing and inserting a CSV file\t\t";

    // Append all subfolders to reach the required file. This
    // appending will have a persistent effect of _test_path which
    // is stored on the heap. After setting it once to the correct
    // folder only the filename has to be replaced.
    _test_path.append( "database" ).append( "POPULATION.CSV" );

    auto sqlite_db = database::Sqlite3( ":memory:", {"%Y-%m-%d %H:%M:%S"} );

    auto population_sniffer = io::CSVSniffer(
        std::nullopt,
        "sqlite",
        {_test_path.string(), _test_path.string()},
        100,
        '\"',
        ',',
        0,
        "POPULATION" );

    const auto population_statement = population_sniffer.sniff();

    // std::cout << population_statement << std::endl;

    sqlite_db.execute( population_statement );

    auto reader =
        io::CSVReader( std::nullopt, _test_path.string(), 0, '\"', ',' );

    sqlite_db.read( "POPULATION", 0, &reader );

    auto it = sqlite_db.select(
        {"column_01", "join_key", "time_stamp", "targets"}, "POPULATION", "" );

    // First line:
    // 0.09902457667435494, 0, 0.7386545235592108, 113.0
    assert_true( std::abs( it->get_double() - 0.099024 ) < 1e-4 );
    assert_true( it->get_string() == "0" );
    assert_true( std::abs( it->get_time_stamp() - 0.738654 ) < 1e-4 );
    assert_true( std::abs( it->get_double() - 113.0 ) < 1e-4 );

    // ---------------------------------------------------------------

    std::cout << "| OK" << std::endl;
}

#endif  // DATABASE_TESTS_TEST1_HPP_
