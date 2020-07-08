#ifndef DATABASE_TESTS_TEST22_HPP_
#define DATABASE_TESTS_TEST22_HPP_

void test22( std::filesystem::path _test_path )
{
    std::cout << "Test 22 | Copying from Postgres to SQLite\t\t";

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
    connectionObject.set( "port_", 5432 );
    connectionObject.set( "user_", "testbert" );

    // Customized time format used within the database.
    const std::vector<std::string> time_formats = { "%Y-%m-%d %H:%M:%S" };

    // ---------------------------------------------------------------

    const std::string source_table_name = "POPULATION";

    const std::string target_table_name = "POPULATION";

    const auto source_conn = std::make_shared<database::Postgres>(
        connectionObject, "testbert", time_formats );

    const auto target_conn =
        std::make_shared<database::Sqlite3>( ":memory:", time_formats );

    // ---------------------------------------------------------------

    const auto stmt = database::DatabaseSniffer::sniff(
        source_conn,
        target_conn->dialect(),
        target_conn->describe(),
        source_table_name,
        target_table_name );

    target_conn->execute( stmt );

    // ---------------------------------------------------------------

    const auto colnames = source_conn->get_colnames( source_table_name );

    const auto iterator =
        source_conn->select( colnames, source_table_name, "" );

    auto reader = database::DatabaseReader( iterator );

    target_conn->read( target_table_name, 0, &reader );

    // ---------------------------------------------------------------

    auto it = target_conn->select(
        { "column_01", "join_key", "time_stamp", "targets" },
        "POPULATION",
        "" );

    // First line:
    // 0.09902457667435494, 0, 0.7386545235592108, 113.0
    assert_true( std::abs( it->get_double() - 0.099024 ) < 1e-4 );
    assert_true( it->get_string() == "0" );
    assert_true( std::abs( it->get_time_stamp() - 0.738654 ) < 1e-4 );
    assert_true( std::abs( it->get_double() - 113.0 ) < 1e-4 );

    // ---------------------------------------------------------------

    std::cout << "| OK" << std::endl;
}

#endif  // DATABASE_TESTS_TEST22_HPP_
