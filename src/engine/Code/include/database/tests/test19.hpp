#ifndef DATABASE_TESTS_TEST19_HPP_
#define DATABASE_TESTS_TEST19_HPP_

void test19( std::filesystem::path _test_path )
{
    std::cout << "Test 19 | Dropping a table in MySQL\t\t\t";

    // Append all subfolders to reach the required file. This
    // appending will have a persistent effect of _test_path which
    // is stored on the heap. After setting it once to the correct
    // folder only the filename has to be replaced.
    _test_path.append( "database" ).append( "POPULATION.CSV" );

    // ---------------------------------------------------------------

#ifdef __APPLE__
    const auto unix_socket = "/tmp/mysql.sock";
#else
    const auto unix_socket = "/var/run/mysqld/mysqld.sock";
#endif

    /*
    CREATE USER 'testbert'@'localhost' IDENTIFIED BY 'testbert';
    GRANT ALL PRIVILEGES ON * . * TO 'testbert'@'localhost';
    CREATE DATABASE IF NOT EXISTS testbertstestbase;
    */

    // Configure MySQL to connect using the user and database
    // created just for this unit test.
    Poco::JSON::Object connectionObject;
    connectionObject.set( "dbname_", "testbertstestbase" );
    connectionObject.set( "host_", "localhost" );
    connectionObject.set( "passwd_", "testbert" );
    connectionObject.set( "port_", 3306 );
    connectionObject.set( "unix_socket_", unix_socket );
    connectionObject.set( "user_", "testbert" );

    // Customized time format used within the database.
    const std::vector<std::string> timeFormats = { "%Y-%m-%d %H:%M:%S" };

    // ---------------------------------------------------------------

    auto mysql_db =
        database::MySQL( connectionObject, "testbert", timeFormats );

    auto population_sniffer = io::CSVSniffer(
        std::nullopt,
        Poco::JSON::Object(),
        "mysql",
        { _test_path.string(), _test_path.string() },
        100,
        '\"',
        ',',
        0,
        "POPULATION" );

    const auto population_statement = population_sniffer.sniff();

    // std::cout << population_statement << std::endl;

    mysql_db.execute( population_statement );

    auto reader =
        io::CSVReader( std::nullopt, _test_path.string(), 0, '\"', ',' );

    mysql_db.read( "POPULATION", 0, &reader );

    mysql_db.drop_table( "POPULATION" );

    // ---------------------------------------------------------------

    std::cout << "| OK" << std::endl;
}

#endif  // DATABASE_TESTS_TEST19_HPP_
