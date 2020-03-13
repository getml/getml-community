#ifndef DATABASE_TESTS_TEST15_HPP_
#define DATABASE_TESTS_TEST15_HPP_

void test15( std::filesystem::path _test_path )
{
    std::cout << "Test 15 | Parsing and inserting a CSV file into MySQL\t";

    // ---------------------------------------------------------------

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
    const std::vector<std::string> timeFormats = {"%Y-%m-%d %H:%M:%S"};

    // ---------------------------------------------------------------

    auto mysql_db =
        database::MySQL( connectionObject, "testbert", timeFormats );

    auto population_sniffer = io::CSVSniffer(
        "mysql",
        {_test_path.string(), _test_path.string()},
        true,
        100,
        '\"',
        ',',
        0,
        "POPULATION" );

    const auto population_statement = population_sniffer.sniff();

    mysql_db.execute( population_statement );

    auto reader = io::CSVReader( _test_path.string(), '\"', ',' );

    mysql_db.read( "POPULATION", true, 0, &reader );

    auto it = mysql_db.select(
        {"column_01", "join_key", "time_stamp", "targets"}, "POPULATION", "" );

    // First line:
    // 0.09902457667435494, 0, 0.7386545235592108, 113.0
    assert_true( std::abs( it->get_double() - 0.099024 ) < 1e-4 );
    assert_true( it->get_string() == "0" );
    assert_true( std::abs( it->get_time_stamp() - 0.738654 ) < 1e-4 );
    assert_true( it->get_int() == 113 );

    // ---------------------------------------------------------------

    std::cout << "| OK" << std::endl;
}

#endif  // DATABASE_TESTS_TEST15_HPP_
