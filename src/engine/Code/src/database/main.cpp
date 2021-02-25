#include "database/tests/tests.hpp"

int main( int argc, char* argv[] )
{
    // ---------------------------------------------------------------

    // Checking the input arguments. A path to the tests folder
    // containing folders distros, multirel, predictors, and relboost
    // has to be provided.
    std::filesystem::path test_path;

    if ( argc == 2 )
        {
            test_path = argv[1];
        }
    else
        {
            std::cout
                << std::endl
                << "-----------------------------------------------------------"
                << std::endl
                << "ERROR: Please provide a path to the test folder!"
                << std::endl
                << std::endl;
            return 1;
        }

    // ---------------------------------------------------------------

    std::cout << std::endl
              << "-----------------------------------------------------------"
              << std::endl
              << "Tests for the module 'DATABASE'" << std::endl
              << "-----------------------------------------------------------"
              << std::endl;

    // Tests for sqlite
    test1( test_path );
    test2( test_path );
    test3( test_path );
    test4( test_path );
    test5( test_path );
    test6( test_path );
    test7( test_path );

    test8( test_path );
    test9( test_path );
    test10( test_path );
    test11( test_path );
    test12( test_path );
    test13( test_path );
    test14();

    // Tests for MySQL
    test15( test_path );
    test16( test_path );
    test17( test_path );
    test18( test_path );
    test19( test_path );
    test20( test_path );
    test21();

    test22( test_path );

}
