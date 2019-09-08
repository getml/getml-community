#ifndef DATABASE_TESTS_TEST5_HPP_
#define DATABASE_TESTS_TEST5_HPP_

void test5()
{
    std::cout << "Test 5: Make sure that opening a file that doesn't exist "
                 "results in failure."
              << std::endl
              << std::endl;

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
                "POPULATION",
                {} );

            const auto population_statement = population_sniffer.sniff();

            assert_true( false );
        }
    catch ( std::exception &e )
        {
            std::cout << e.what() << std::endl;
        }

    try
        {
            auto reader = csv::Reader( "DOESNOTEXIST.CSV", '\"', ',' );

            assert_true( false );
        }
    catch ( std::exception &e )
        {
            std::cout << e.what() << std::endl;
        }

    auto reader = csv::Reader( "POPULATION2.CSV", '\"', ',' );

    std::cout << std::endl << std::endl;
    std::cout << "OK." << std::endl << std::endl;
}

#endif  // DATABASE_TESTS_TEST5_HPP_
