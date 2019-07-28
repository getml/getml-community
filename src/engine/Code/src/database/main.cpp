#include "database/tests/tests.hpp"

int main( int argc, char* argv[] )
{
    std::cout << std::endl
              << "-------------------------------------------------------------"
                 "------------------"
              << std::endl;
    std::cout << "TESTS FOR MODULE 'DATABASE':" << std::endl << std::endl;

    // Tests for sqlite
    test1();
    test2();
    test3();
    test4();
    test5();
    test6();
    test7();

    // Tests for postgres
    test8();
    test9();
    test10();
}
