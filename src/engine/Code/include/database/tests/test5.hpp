#ifndef DATABASE_TESTS_TEST5_HPP_
#define DATABASE_TESTS_TEST5_HPP_

void test5( std::filesystem::path _test_path )
{
    std::cout << "Test 5: Make sure that opening a file that doesn't exist "
                 "results in failure."
              << std::endl
              << std::endl;

    // Append all subfolders to reach the required file. This 
    // appending will have a persistent effect of _test_path which
    // is stored on the heap. After setting it once to the correct
    // folder only the filename has to be replaced.
    _test_path.append( "database" ).append( "POPULATION2.CSV" );

    try
        {
            auto population_sniffer = csv::Sniffer(
                "sqlite",
                {"DOESNOTEXIST.CSV"},
                true,
                100,
                '\"',
                ',',
                0,
                "POPULATION" );

            const auto population_statement = population_sniffer.sniff();

            assert_true( false );
        }
    catch ( std::exception &e )
        {
            std::cout << e.what() << std::endl;
        }

    try
        {
            auto reader = csv::CSVReader( "DOESNOTEXIST.CSV", '\"', ',' );

            assert_true( false );
        }
    catch ( std::exception &e )
        {
            std::cout << e.what() << std::endl;
        }

    auto reader = csv::CSVReader( _test_path.string(), '\"', ',' );

    std::cout << std::endl << std::endl;
    std::cout << "OK." << std::endl << std::endl;
}

#endif  // DATABASE_TESTS_TEST5_HPP_
