#ifndef DATABASE_TESTS_TEST12_HPP_
#define DATABASE_TESTS_TEST12_HPP_

void test12( std::filesystem::path _test_path )
{
    std::cout << "Test 12 | Dropping a table in postgres\t\t\t";

    // Append all subfolders to reach the required file. This
    // appending will have a persistent effect of _test_path which
    // is stored on the heap. After setting it once to the correct
    // folder only the filename has to be replaced.
    _test_path.append( "database" ).append( "POPULATION.CSV" );

    // ---------------------------------------------------------------

    // Configure PostgreSQL to connect using the user and database
    // created just for this unit test.
    Poco::JSON::Object connectionObject;
    connectionObject.set( "dbname_", "testbertstestbase" );
    connectionObject.set( "host_", "localhost" );
    connectionObject.set( "hostaddr_", "127.0.0.1" );
    connectionObject.set( "password_", "testbert" );
    connectionObject.set( "port_", 5432 );
    connectionObject.set( "user_", "testbert" );

    // Customized time format used within the database.
    const std::vector<std::string> timeFormats = {"%Y-%m-%d %H:%M:%S"};

    // ---------------------------------------------------------------

    auto postgres_db = database::Postgres( connectionObject, timeFormats );

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

    // std::cout << population_statement << std::endl;

    postgres_db.execute( population_statement );

    auto reader = csv::CSVReader( _test_path.string(), '\"', ',' );

    postgres_db.read( "POPULATION", true, 0, &reader );

    postgres_db.drop_table( "POPULATION" );

    // ---------------------------------------------------------------

    std::cout << "| OK" << std::endl;
}

#endif  // DATABASE_TESTS_TEST12_HPP_
