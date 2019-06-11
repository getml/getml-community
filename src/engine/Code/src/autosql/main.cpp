
#include "autosql/tests/tests.hpp"

int main( int argc, char* argv[] )
{
    std::cout << std::endl
              << "-------------------------------------------------------------"
                 "------------------"
              << std::endl;
    std::cout << "TESTS FOR MODULE 'AUTOSQL':" << std::endl << std::endl;

    test1_sum();
    test2_avg();

    return 0;
}