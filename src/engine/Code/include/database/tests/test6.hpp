#ifndef DATABASE_TESTS_TEST6_HPP_
#define DATABASE_TESTS_TEST6_HPP_

void test6( std::filesystem::path _test_path )
{
    std::cout << "Test 6 | Dropping a table\t\t\t\t";

    // ---------------------------------------------------------------

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

    auto reader = io::CSVReader( std::nullopt, _test_path.string(), '\"', ',' );

    sqlite_db.read( "POPULATION", 0, &reader );

    sqlite_db.drop_table( "POPULATION" );

    // ---------------------------------------------------------------

    std::cout << "| OK" << std::endl;
}

#endif  // DATABASE_TESTS_TEST6_HPP_
