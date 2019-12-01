#ifndef DATABASE_TESTS_TEST20_HPP_
#define DATABASE_TESTS_TEST20_HPP_

void test20( std::filesystem::path _test_path )
{
    std::cout << "Test 20 | Getting the content from a MySQL database\t";

    // ---------------------------------------------------------------

    // Append all subfolders to reach the required file. This
    // appending will have a persistent effect of _test_path which
    // is stored on the heap. After setting it once to the correct
    // folder only the filename has to be replaced.
    _test_path.append( "database" ).append( "POPULATION.CSV" );

    // ---------------------------------------------------------------

    // Configure MySQL to connect using the user and database
    // created just for this unit test.
    Poco::JSON::Object connectionObject;
    connectionObject.set( "db_", "testbertstestbase" );
    connectionObject.set( "host_", "localhost" );
    connectionObject.set( "passwd_", "testbert" );
    connectionObject.set( "port_", 3306 );
    connectionObject.set( "unix_socket_", "/var/run/mysqld/mysqld.sock" );
    connectionObject.set( "user_", "testbert" );

    // Customized time format used within the database.
    const std::vector<std::string> timeFormats = {"%Y-%m-%d %H:%M:%S"};

    // ---------------------------------------------------------------

    auto mysql_db = database::MySQL( connectionObject, timeFormats );

    auto population_sniffer = csv::Sniffer(
        "mysql",
        {_test_path.string(), _test_path.string()},
        true,
        100,
        '\"',
        ',',
        0,
        "POPULATION" );

    const auto population_statement = population_sniffer.sniff();

    // std::cout << population_statement << std::endl;

    mysql_db.execute( population_statement );

    auto reader = csv::CSVReader( _test_path.string(), '\"', ',' );

    mysql_db.read( "POPULATION", true, 0, &reader );

    const auto colnames = mysql_db.get_colnames( "POPULATION" );

    for ( auto cname : colnames )
        {
            std::cout << cname << " ";
        }

    const auto obj = mysql_db.get_content( "POPULATION", 0, 99, 20 );

    Poco::JSON::Stringifier::stringify( obj, std::cout );

    // ---------------------------------------------------------------

    std::cout << "| OK" << std::endl;
}

#endif  // DATABASE_TESTS_TEST20_HPP_
