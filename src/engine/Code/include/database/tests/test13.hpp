#ifndef DATABASE_TESTS_TEST13_HPP_
#define DATABASE_TESTS_TEST13_HPP_

void test13( std::filesystem::path _test_path )
{
    std::cout << "Test 13: Getting the content from a postgres database."
              << std::endl
              << std::endl;

    // Append all subfolders to reach the required file. This 
    // appending will have a persistent effect of _test_path which
    // is stored on the heap. After setting it once to the correct
    // folder only the filename has to be replaced.
    _test_path.append( "database" ).append( "POPULATION.CSV" );

    auto postgres_db = database::Postgres( {"%Y-%m-%d %H:%M:%S"} );

    auto population_sniffer = csv::Sniffer(
        "postgres",
        {_test_path.string(), _test_path.string()},
        true,
        100,
        '\"',
        ',',
        0,
        "POPULATION" );

    const auto population_statement = population_sniffer.sniff();

    std::cout << population_statement << std::endl;

    postgres_db.execute( population_statement );

    auto reader = csv::CSVReader( _test_path.string(), '\"', ',' );

    postgres_db.read( "POPULATION", true, 0, &reader );

    const auto colnames = postgres_db.get_colnames( "POPULATION" );

    for ( auto cname : colnames )
        {
            std::cout << cname << " ";
        }
    std::cout << std::endl;

    const auto obj = postgres_db.get_content( "POPULATION", 0, 99, 20 );

    Poco::JSON::Stringifier::stringify( obj, std::cout );

    std::cout << std::endl << std::endl;
    std::cout << "OK." << std::endl << std::endl;
}

#endif  // DATABASE_TESTS_TEST13_HPP_
