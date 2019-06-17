
#include "autosql/tests/tests.hpp"

int main( int argc, char* argv[] )
{
    std::cout << std::endl
              << "-------------------------------------------------------------"
                 "------------------"
              << std::endl;
    std::cout << "TESTS FOR MODULE 'AUTOSQL':" << std::endl << std::endl;

    test1_count();
    test2_avg();
    test3_sum();
    test4_max();
    test5_min();

    test6_time_stamps_diff();
    test7_categorical();
    test8_multiple_categorical();
    test9_discrete();

    test10_same_units_categorical();
    test11_same_units_numerical();
    test12_same_units_discrete();

    test13_categorical_output();
    test14_numerical_output();
    test15_discrete_output();

    test16_nan_values_numerical();
    test17_nan_values_discrete();

    test18_upper_time_stamps();

    test20_saving_and_loading_models();

    return 0;
}