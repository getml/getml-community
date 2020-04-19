#ifndef DATABASE_TESTS_TEST7_HPP_
#define DATABASE_TESTS_TEST7_HPP_

void test7( std::filesystem::path _test_path )
{
    std::cout << "Test 7 | Getting the content\t\t\t\t";

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

    auto reader =
        io::CSVReader( std::nullopt, _test_path.string(), 0, '\"', ',' );

    sqlite_db.read( "POPULATION", 0, &reader );

    const auto colnames = sqlite_db.get_colnames( "POPULATION" );

    // for ( auto cname : colnames )
    //     {
    //         std::cout << cname << " ";
    //     }
    // std::cout << std::endl;

    const auto obj = sqlite_db.get_content( "POPULATION", 0, 99, 20 );

    // Poco::JSON::Stringifier::stringify( obj, std::cout );

    // ---------------------------------------------------------------

    std::cout << "| OK" << std::endl;
}

#endif  // DATABASE_TESTS_TEST7_HPP_
