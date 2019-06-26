#include "predictors/tests/tests.hpp"

int main( int argc, char* argv[] )
{
    std::cout << std::endl
              << "-------------------------------------------------------------"
                 "------------------"
              << std::endl;
    std::cout << "TESTS FOR MODULE 'PREDICTORS':" << std::endl << std::endl;

    test1_linear_regression_dense();

    return 0;
}
