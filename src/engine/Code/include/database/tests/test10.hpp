#ifndef DATABASE_TESTS_TEST10_HPP_
#define DATABASE_TESTS_TEST10_HPP_

void test10()
{
    std::cout << "Test 10: Parsing columns encoded as text in postgres."
              << std::endl
              << std::endl;

    auto postgres_db = database::Postgres( {"%Y-%m-%d %H:%M:%S"} );

    auto population_sniffer = csv::Sniffer(
        "postgres",
        {"POPULATION.CSV", "POPULATION.CSV"},
        false,
        100,
        '\"',
        ',',
        0,
        "POPULATION" );

    const auto population_statement = population_sniffer.sniff();

    std::cout << population_statement << std::endl;

    postgres_db.execute( population_statement );

    auto reader = csv::CSVReader( "POPULATION.CSV", '\"', ',' );

    postgres_db.read( "POPULATION", false, 0, &reader );

    auto it = postgres_db.select(
        {"COLUMN_1", "COLUMN_2", "COLUMN_3", "COLUMN_4"}, "POPULATION", "" );

    // Header line (read in and formatted):
    assert_true( std::isnan( it->get_double() ) );
    assert_true( std::isnan( it->get_double() ) );
    assert_true( std::isnan( it->get_double() ) );
    assert_true( std::isnan( it->get_double() ) );

    // First line (pay special attention to column 2 - it should not be NULL!):
    // 0.09902457667435494, 0, 0.7386545235592108, 113.0
    assert_true( std::abs( it->get_double() - 0.099024 ) < 1e-4 );
    assert_true( it->get_double() == 0.0 );
    assert_true( std::abs( it->get_time_stamp() - 0.738654 ) < 1e-4 );
    assert_true( it->get_int() == 113 );

    std::cout << std::endl << std::endl;
    std::cout << "OK." << std::endl << std::endl;
}

#endif  // DATABASE_TESTS_TEST10_HPP_
