
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

    return 0;
}