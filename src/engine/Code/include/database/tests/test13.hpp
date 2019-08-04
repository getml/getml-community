#ifndef DATABASE_TESTS_TEST13_HPP_
#define DATABASE_TESTS_TEST13_HPP_

void test13()
{
    std::cout << "Test 13: Getting the content from a postgres database."
              << std::endl
              << std::endl;

    auto postgres_db = database::Postgres( {"%Y-%m-%d %H:%M:%S"} );

    auto population_sniffer = csv::Sniffer(
        "postgres",
        {"POPULATION.CSV", "POPULATION.CSV"},
        true,
        100,
        '\"',
        ',',
        "POPULATION",
        {"%Y-%b-%d %H:%M:%S"} );

    const auto population_statement = population_sniffer.sniff();

    std::cout << population_statement << std::endl;

    postgres_db.execute( population_statement );

    auto reader = csv::Reader( "POPULATION.CSV", '\"', ',' );

    postgres_db.read_csv( "POPULATION", true, &reader );

    const auto colnames = postgres_db.get_colnames( "POPULATION" );

    for ( auto cname : colnames )
        {
            std::cout << cname << " ";
        }
    std::cout << std::endl;

    const auto obj = postgres_db.get_content( "POPULATION", 0, 99, 20 );

    Poco::JSON::Stringifier::stringify( obj, std::cout );

    std::cout << std::endl << std::endl;
    std::cout << "OK." << std::endl << std::endl;
}

#endif  // DATABASE_TESTS_TEST13_HPP_
