#ifndef DATABASE_TESTS_TEST6_HPP_
#define DATABASE_TESTS_TEST6_HPP_

void test6()
{
    std::cout << "Test 6: Dropping a table." << std::endl << std::endl;

    auto sqlite_db = database::Sqlite3( ":memory:", {"%Y-%m-%d %H:%M:%S"} );

    auto population_sniffer = csv::Sniffer(
        "sqlite",
        {"POPULATION.CSV", "POPULATION.CSV"},
        true,
        100,
        '\"',
        ',',
        "POPULATION",
        {"%Y-%b-%d %H:%M:%S"} );

    const auto population_statement = population_sniffer.sniff();

    std::cout << population_statement << std::endl;

    sqlite_db.execute( population_statement );

    auto reader = csv::Reader( "POPULATION.CSV", '\"', ',' );

    sqlite_db.read_csv( "POPULATION", true, &reader );

    sqlite_db.drop_table( "POPULATION" );

    std::cout << std::endl << std::endl;
    std::cout << "OK." << std::endl << std::endl;
}

#endif  // DATABASE_TESTS_TEST6_HPP_