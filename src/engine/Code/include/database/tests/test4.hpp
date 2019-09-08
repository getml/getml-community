#ifndef DATABASE_TESTS_TEST4_HPP_
#define DATABASE_TESTS_TEST4_HPP_

void test4()
{
    std::cout << "Test 4: Parsing time stamps." << std::endl << std::endl;

    auto sqlite_db = database::Sqlite3(
        ":memory:", {"%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"} );

    auto population_sniffer = csv::Sniffer(
        "sqlite",
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

    sqlite_db.execute( population_statement );

    auto reader = csv::Reader( "POPULATION2.CSV", '\"', ',' );

    sqlite_db.read_csv( "POPULATION", true, 0, &reader );

    auto it = sqlite_db.select(
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

#endif  // DATABASE_TESTS_TEST4_HPP_
