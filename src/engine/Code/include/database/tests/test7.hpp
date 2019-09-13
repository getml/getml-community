#ifndef DATABASE_TESTS_TEST7_HPP_
#define DATABASE_TESTS_TEST7_HPP_

void test7()
{
    std::cout << "Test 7: Getting the content." << std::endl << std::endl;

    auto sqlite_db = database::Sqlite3( ":memory:", {"%Y-%m-%d %H:%M:%S"} );

    auto population_sniffer = csv::Sniffer(
        "sqlite",
        {"POPULATION.CSV", "POPULATION.CSV"},
        true,
        100,
        '\"',
        ',',
        0,
        "POPULATION" );

    const auto population_statement = population_sniffer.sniff();

    std::cout << population_statement << std::endl;

    sqlite_db.execute( population_statement );

    auto reader = csv::CSVReader( "POPULATION.CSV", '\"', ',' );

    sqlite_db.read( "POPULATION", true, 0, &reader );

    const auto colnames = sqlite_db.get_colnames( "POPULATION" );

    for ( auto cname : colnames )
        {
            std::cout << cname << " ";
        }
    std::cout << std::endl;

    const auto obj = sqlite_db.get_content( "POPULATION", 0, 99, 20 );

    Poco::JSON::Stringifier::stringify( obj, std::cout );

    std::cout << std::endl << std::endl;
    std::cout << "OK." << std::endl << std::endl;
}

#endif  // DATABASE_TESTS_TEST7_HPP_
