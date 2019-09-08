#ifndef DATABASE_TESTS_TEST11_HPP_
#define DATABASE_TESTS_TEST11_HPP_

void test11()
{
    std::cout << "Test 11: Parsing time stamps in postgrexs." << std::endl
              << std::endl;

    auto postgres_db =
        database::Postgres( {"%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"} );

    auto population_sniffer = csv::Sniffer(
        "postgres",
        {"POPULATION2.CSV", "POPULATION2.CSV"},
        true,
        100,
        '\"',
        ',',
        0,
        "POPULATION",
        {} );

    const auto population_statement = population_sniffer.sniff();

    std::cout << population_statement << std::endl;

    postgres_db.execute( population_statement );

    auto reader = csv::Reader( "POPULATION2.CSV", '\"', ',' );

    postgres_db.read_csv( "POPULATION", true, 0, &reader );

    auto it = postgres_db.select(
        {"column_01", "join_key", "time_stamp", "targets"}, "POPULATION", "" );

    // First line:
    // 0.09902457667435494, 0, 0.7386545235592108, 113.0
    assert( std::abs( it->get_double() - 0.099024 ) < 1e-4 );
    assert( it->get_string() == "0" );
    assert( std::abs( it->get_time_stamp() - 6647.85 ) < 1.0 );
    assert( it->get_int() == 113 );

    std::cout << std::endl << std::endl;
    std::cout << "OK." << std::endl << std::endl;
}

#endif  // DATABASE_TESTS_TEST11_HPP_
