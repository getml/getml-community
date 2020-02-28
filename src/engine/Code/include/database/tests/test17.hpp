#ifndef DATABASE_TESTS_TEST17_HPP_
#define DATABASE_TESTS_TEST17_HPP_

void test17( std::filesystem::path _test_path )
{
    std::cout << "Test 17 | Parsing columns encoded as text in MySQL.\t";

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

    auto population_sniffer = csv::Sniffer(
        "mysql",
        {_test_path.string(), _test_path.string()},
        false,
        100,
        '\"',
        ',',
        0,
        "POPULATION" );

    const auto population_statement = population_sniffer.sniff();

    // std::cout << population_statement << std::endl;

    mysql_db.execute( population_statement );

    auto reader = csv::CSVReader( _test_path.string(), '\"', ',' );

    mysql_db.read( "POPULATION", false, 0, &reader );

    auto it = mysql_db.select(
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

    // ---------------------------------------------------------------

    std::cout << "| OK" << std::endl;
}

#endif  // DATABASE_TESTS_TEST17_HPP_
