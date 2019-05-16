#ifndef DATABASE_TESTS_TEST3_HPP_
#define DATABASE_TESTS_TEST3_HPP_

void test3()
{
    std::cout << "Test 3: Parsing columns encoded as text." << std::endl
              << std::endl;

    auto sqlite_db = database::Sqlite3( ":memory:", {"%Y-%m-%d %H:%M:%S"} );

    auto population_sniffer = csv::Sniffer(
        "sqlite",
        {"POPULATION.CSV", "POPULATION.CSV"},
        false,
        100,
        '\"',
        ',',
        "POPULATION",
        {"%Y-%b-%d %H:%M:%S"} );

    const auto population_statement = population_sniffer.sniff();

    std::cout << population_statement << std::endl;

    sqlite_db.exec( population_statement );

    auto reader = csv::Reader( "POPULATION.CSV", '\"', ',' );

    sqlite_db.read_csv( "POPULATION", false, &reader );

    auto it = sqlite_db.select(
        {"COLUMN_1", "COLUMN_2", "COLUMN_3", "COLUMN_4"}, "POPULATION" );

    // Header line (read in and formatted):
    assert( std::isnan( it->get_double() ) );
    assert( std::isnan( it->get_double() ) );
    assert( std::isnan( it->get_double() ) );
    assert( std::isnan( it->get_double() ) );

    // First line (pay special attention to column 2 - it should not be NULL!):
    // 0.09902457667435494, 0, 0.7386545235592108, 113.0
    assert( std::abs( it->get_double() - 0.099024 ) < 1e-4 );
    assert( it->get_double() == 0.0 );
    assert( std::abs( it->get_time_stamp() - 0.738654 ) < 1e-4 );
    assert( it->get_int() == 113 );

    std::cout << std::endl << std::endl;
    std::cout << "OK." << std::endl << std::endl;
}

#endif  // DATABASE_TESTS_TEST3_HPP_