
#include "multirel/tests/tests.hpp"

int main( int argc, char* argv[] )
{

	// ---------------------------------------------------------------
	
	// Checking the input arguments. A path to the tests folder
	// containing folders distros, multirel, predictors, and relboost
	// has to be provided.
	if ( argc == 2 ) {
		std::filesystem::path test_path = argv[1];
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

    test21_snowflake_model();

    test22_time_windows();

    return 0;
}
