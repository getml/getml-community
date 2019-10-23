#include "relboost/tests/tests.hpp"

int main( int argc, char* argv[] )
{
    std::cout << std::endl
              << "-------------------------------------------------------------"
                 "------------------"
              << std::endl;
    std::cout << "TESTS FOR MODULE 'RELBOOST':" << std::endl << std::endl;

    test1_sum();

    test2_avg();
    test3_avg();

    test4_time_stamps_diff();

    test5_categorical();

    test6_categorical();
    test7_categorical();

    test8_multiple_categorical();

    test9_discrete();

    test10_same_units_categorical();
    test11_same_units_numerical();
    test12_same_units_discrete();

    // test13_numerical_output();
    test14_categorical_output();
    test15_discrete_output();

    test16_nan_values_numerical();
    test17_nan_values_discrete();

    test18_upper_time_stamps();

    test19_classification();

    test20_saving_and_loading();

    test21_time_windows();

    test22_snowflake_model();
    test23_snowflake_model2();
    test24_snowflake_model3();

    return 0;
}
