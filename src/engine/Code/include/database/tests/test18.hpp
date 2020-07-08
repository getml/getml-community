#ifndef DATABASE_TESTS_TEST18_HPP_
#define DATABASE_TESTS_TEST18_HPP_

void test18( std::filesystem::path _test_path )
{
    std::cout << "Test 18 | Parsing time stamps in MySQL\t\t\t";

    // Append all subfolders to reach the required file. This
    // appending will have a persistent effect of _test_path which
    // is stored on the heap. After setting it once to the correct
    // folder only the filename has to be replaced.
    _test_path.append( "database" ).append( "POPULATION2.CSV" );

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

    // Configure PostgreSQL to connect using the user and database
    // created just for this unit test.
    Poco::JSON::Object connectionObject;
    connectionObject.set( "dbname_", "testbertstestbase" );
    connectionObject.set( "host_", "localhost" );
    connectionObject.set( "passwd_", "testbert" );
    connectionObject.set( "port_", 3306 );
    connectionObject.set( "unix_socket_", unix_socket );
    connectionObject.set( "user_", "testbert" );

    // Customized time format used within the database.
    const std::vector<std::string> timeFormats = {
        "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S" };

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

    auto it = mysql_db.select(
        { "column_01", "join_key", "time_stamp", "targets" },
        "POPULATION",
        "" );

    // First line:
    // 0.09902457667435494, 0, 0.7386545235592108, 113.0
    assert_true( std::abs( it->get_double() - 0.099024 ) < 1e-4 );
    assert_true( it->get_string() == "0" );
    assert_true( std::abs( it->get_time_stamp() - 6647.85 ) < 1.0 );
    assert_true( it->get_int() == 113 );

    // ---------------------------------------------------------------

    std::cout << "| OK" << std::endl;
}

#endif  // DATABASE_TESTS_TEST18_HPP_
