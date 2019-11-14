
#include "multirel/tests/tests.hpp"

int main( int argc, char* argv[] )
{

	// ---------------------------------------------------------------
	
	// Checking the input arguments. A path to the tests folder
	// containing folders distros, multirel, predictors, and relboost
	// has to be provided.
	std::filesystem::path test_path;
	
	if ( argc == 2 ) {
		test_path = argv[1];
	} else {
		std::cout << std::endl
				  << "-----------------------------------------------"
			"--------------------------------"
				  << std::endl;
		std::cout << "ERROR: Please provide a path to the test folder!" 
				  << std::endl << std::endl;
		return 1;
	}
	
	// ---------------------------------------------------------------
	
	std::cout << std::endl
              << "-------------------------------------------------------------"
                 "------------------"
              << std::endl;
    std::cout << "TESTS FOR MODULE 'MULTIREL' IN " << test_path.string()
			  << ":" << std::endl << std::endl;
	
	// ---------------------------------------------------------------
	
    test1_count( test_path );
    test2_avg( test_path );
    test3_sum( test_path );
    test4_max( test_path );
    test5_min( test_path );

    test6_time_stamps_diff( test_path );
    test7_categorical( test_path );
    test8_multiple_categorical( test_path );
    test9_discrete( test_path );

    test10_same_units_categorical( test_path );
    test11_same_units_numerical( test_path );
    test12_same_units_discrete( test_path );

    test13_categorical_output( test_path );
    test14_numerical_output( test_path );
    test15_discrete_output( test_path );

    test16_nan_values_numerical( test_path );
    test17_nan_values_discrete( test_path );

    test18_upper_time_stamps( test_path );

    test20_saving_and_loading_models( test_path );

    test21_snowflake_model( test_path );

    test22_time_windows( test_path );

    return 0;
}
