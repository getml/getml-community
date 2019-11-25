#ifndef DATABASE_TESTS_TEST9_HPP_
#define DATABASE_TESTS_TEST9_HPP_

void test9( std::filesystem::path _test_path )
{
    std::cout << "Test 9: NULL values in postgres." << std::endl << std::endl;

    // Append all subfolders to reach the required file. This 
    // appending will have a persistent effect of _test_path which
    // is stored on the heap. After setting it once to the correct
    // folder only the filename has to be replaced.
    _test_path.append( "database" ).append( "POPULATION.CSV" );
    
    // ---------------------------------------------------------------
    
    // Configure PostgreSQL to connect using the user and database
    // created just for this unit test.
    Poco::JSON::Object connectionObject;
    connectionObject.set( "dbname_", "testbertsTestBase" );
    connectionObject.set( "host_", "localhost" );
    connectionObject.set( "hostaddr_", "127.0.0.1" );
    connectionObject.set( "password_", "" );
    connectionObject.set( "port_", 5432 );
    connectionObject.set( "user_", "testbert" );
    
    // Customized time format used within the database.
    const std::vector<std::string> timeFormats = {"%Y-%m-%d %H:%M:%S"};
    
    // ---------------------------------------------------------------

    auto postgres_db = database::Postgres( connectionObject, 
    					   timeFormats);

    auto population_sniffer = csv::Sniffer(
        "postgres",
        {_test_path.string(), _test_path.string()},
        true,
        100,
        '\"',
        ',',
        0,
        "POPULATION" );

    const auto population_statement = population_sniffer.sniff();

    std::cout << population_statement << std::endl;

    postgres_db.execute( population_statement );

    auto reader = csv::CSVReader( _test_path.string(), '\"', ',' );

    // We read in the header, which should be parsed as NULL values.
    postgres_db.read( "POPULATION", false, 0, &reader );

    auto it = postgres_db.select(
        {"column_01", "join_key", "time_stamp", "targets"}, "POPULATION", "" );

    // Header line (read in and formatted):
    assert_true( std::isnan( it->get_double() ) );
    assert_true( it->get_string() == "NULL" );
    assert_true( std::isnan( it->get_time_stamp() ) );
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

#endif  // DATABASE_TESTS_TEST9_HPP_
