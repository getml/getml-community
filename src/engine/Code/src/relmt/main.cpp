#include "relmt/tests/tests.hpp"

int main( int argc, char* argv[] )
{
    // ---------------------------------------------------------------

    // Checking the input arguments. A path to the tests folder
    // containing folders distros, multirel, predictors, and relmt
    // has to be provided.
    std::filesystem::path test_path =
        "../../../dependencies/relboost-engine/tests/";

    // ---------------------------------------------------------------

    std::cout << std::endl
              << "-----------------------------------------------------------"
              << std::endl
              << "Tests for the module 'RELCIT'" << std::endl
              << "-----------------------------------------------------------"
              << std::endl;

    // ---------------------------------------------------------------

    test1_sum( test_path );
    test2_count( test_path );
    test3_avg( test_path );
    test4_categorical( test_path );
    test5_multiple_categorical( test_path );
    test6_snowflake_model( test_path );

    // ---------------------------------------------------------------

    return 0;
}
