#ifndef DATABASE_TESTS_TEST14_HPP_
#define DATABASE_TESTS_TEST14_HPP_

void test14()
{
    std::cout << "Test 14 | Getting the tables from a postgres database\t";

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

    const auto table_names = postgres_db.list_tables();

    // for ( const auto& table : table_names )
    //     {
    //         std::cout << table << " ";
    //     }

    // ---------------------------------------------------------------
	
    std::cout << "| OK" << std::endl;
}

#endif  // DATABASE_TESTS_TEST14_HPP_
