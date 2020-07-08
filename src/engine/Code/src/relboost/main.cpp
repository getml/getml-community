#include "relboost/tests/tests.hpp"

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
              << "Tests for the module 'RELBOOST'" << std::endl
              << "-----------------------------------------------------------"
              << std::endl;

    // ---------------------------------------------------------------

    test1_sum( test_path );

    test2_avg( test_path );
    test3_avg( test_path );

    test4_time_stamps_diff( test_path );

    test5_categorical( test_path );

    test6_categorical( test_path );
    test7_categorical( test_path );

    test8_multiple_categorical( test_path );

    test9_discrete( test_path );

    test10_same_units_categorical( test_path );
    test11_same_units_numerical( test_path );
    test12_same_units_discrete( test_path );

    // test13_numerical_output( test_path );
    test14_categorical_output( test_path );
    // test15_discrete_output( test_path );

    test16_nan_values_numerical( test_path );
    test17_nan_values_discrete( test_path );

    test18_upper_time_stamps( test_path );

    test19_classification( test_path );

    test20_saving_and_loading( test_path );

    test21_time_windows( test_path );

    test22_snowflake_model( test_path );
    test23_snowflake_model2( test_path );
    test24_snowflake_model3( test_path );
    test25_snowflake_model4( test_path );
    test26_snowflake_model5( test_path );
    test27_snowflake_model6( test_path );

    return 0;
}
