#ifndef DATABASE_TESTS_TEST14_HPP_
#define DATABASE_TESTS_TEST14_HPP_

void test14()
{
    std::cout << "Test 14: Getting the tables from a postgres database."
              << std::endl
              << std::endl;

    auto postgres_db = database::Postgres( {"%Y-%m-%d %H:%M:%S"} );

    const auto table_names = postgres_db.list_tables();

    for ( const auto& table : table_names )
        {
            std::cout << table << " ";
        }

    std::cout << std::endl << std::endl;
    std::cout << "OK." << std::endl << std::endl;
}

#endif  // DATABASE_TESTS_TEST14_HPP_
