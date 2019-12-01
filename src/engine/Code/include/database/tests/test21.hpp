#ifndef DATABASE_TESTS_TEST21_HPP_
#define DATABASE_TESTS_TEST21_HPP_

void test21()
{
    std::cout << "Test 21 | Getting the tables from a MySQL database\t";

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

    const auto table_names = mysql_db.list_tables();

    for ( const auto& table : table_names )
        {
            std::cout << table << " ";
        }

    // ---------------------------------------------------------------

    std::cout << "| OK" << std::endl;
}

#endif  // DATABASE_TESTS_TEST21_HPP_
