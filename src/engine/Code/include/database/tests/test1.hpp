#ifndef DATABASE_TESTS_TEST1_HPP_
#define DATABASE_TESTS_TEST1_HPP_

void test1()
{
    std::cout << "Test 1: Parsing and inserting a CSV file." << std::endl
              << std::endl;

    auto sqlite_db = database::Sqlite3( ":memory:", {"%Y-%m-%d %H:%M:%S"} );

    auto population_sniffer = csv::Sniffer(
        "sqlite",
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

    sqlite_db.execute( population_statement );

    auto reader = csv::CSVReader( "POPULATION.CSV", '\"', ',' );

    sqlite_db.read( "POPULATION", true, 0, &reader );

    auto it = sqlite_db.select(
        {"column_01", "join_key", "time_stamp", "targets"}, "POPULATION", "" );

    // First line:
    // 0.09902457667435494, 0, 0.7386545235592108, 113.0
    assert_true( std::abs( it->get_double() - 0.099024 ) < 1e-4 );
    assert_true( it->get_string() == "0" );
    assert_true( std::abs( it->get_time_stamp() - 0.738654 ) < 1e-4 );
    assert_true( std::abs( it->get_double() - 113.0 ) < 1e-4 );

    std::cout << std::endl << std::endl;
    std::cout << "OK." << std::endl << std::endl;
}

#endif  // DATABASE_TESTS_TEST1_HPP_
