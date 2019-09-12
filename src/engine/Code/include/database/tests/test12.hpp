#ifndef DATABASE_TESTS_TEST12_HPP_
#define DATABASE_TESTS_TEST12_HPP_

void test12()
{
    std::cout << "Test 12: Dropping a table in postgres." << std::endl
              << std::endl;

    auto postgres_db = database::Postgres( {"%Y-%m-%d %H:%M:%S"} );

    auto population_sniffer = csv::Sniffer(
        "postgres",
        {"POPULATION.CSV", "POPULATION.CSV"},
        true,
        100,
        '\"',
        ',',
        0,
        "POPULATION",
        {"%Y-%b-%d %H:%M:%S"} );

    const auto population_statement = population_sniffer.sniff();

    std::cout << population_statement << std::endl;

    postgres_db.execute( population_statement );

    auto reader = csv::CSVReader( "POPULATION.CSV", '\"', ',' );

    postgres_db.read( "POPULATION", true, 0, &reader );

    postgres_db.drop_table( "POPULATION" );

    std::cout << std::endl << std::endl;
    std::cout << "OK." << std::endl << std::endl;
}

#endif  // DATABASE_TESTS_TEST12_HPP_
