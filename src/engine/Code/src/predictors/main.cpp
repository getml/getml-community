#include "predictors/tests/tests.hpp"

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
    std::cout << "TESTS FOR MODULE 'PREDICTORS' IN " << test_path.string()
			  << ":" << std::endl << std::endl;

    test1_linear_regression_dense();

    test2_csr_matrix();

    test3_linear_regression_sparse();

    test4_saving_and_loading_linear_regression( test_path );

    test5_logistic_regression_dense();

    test6_logistic_regression_sparse();

    return 0;
}
