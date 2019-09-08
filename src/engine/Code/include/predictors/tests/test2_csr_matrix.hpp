
// -----------------------------------------------------------------------------

void test2_csr_matrix()
{
    // -------------------------------------------------------------------------

    std::cout << std::endl << "Test 2 (CSR Matrix): " << std::endl << std::endl;

    // -------------------------------------------------------------------------

    auto rng = std::mt19937( 100 );

    auto X_numerical1 = make_column<predictors::Float>( 20, &rng );

    auto X_numerical2 = make_column<predictors::Float>( 20, &rng );

    auto X_categorical1 = make_column<predictors::Int>( 20, &rng );

    auto X_categorical2 = make_column<predictors::Int>( 20, &rng );

    auto csr_matrix =
        predictors::CSRMatrix<predictors::Float, predictors::Int, size_t>();

    csr_matrix.add( X_numerical1 );

    csr_matrix.add( X_numerical2 );

    csr_matrix.add( X_categorical1, 500 );

    csr_matrix.add( X_categorical2, 500 );

    std::cout << "indptr: ";
    for ( size_t i = 0; i < 21; ++i )
        {
            std::cout << csr_matrix.indptr()[i] << " ";
            assert_true( csr_matrix.indptr()[i] == i * 4 );
        }
    std::cout << std::endl << std::endl;

    std::cout << "indices: ";
    for ( size_t i = 0; i < 80; ++i )
        {
            std::cout << csr_matrix.indices()[i] << " ";
            assert_true( i % 4 != 0 || csr_matrix.indices()[i] == 0 );
            assert_true( i % 4 != 1 || csr_matrix.indices()[i] == 1 );
            assert_true(
                i % 4 != 2 || ( csr_matrix.indices()[i] > 1 &&
                                csr_matrix.indices()[i] < 502 ) );
            assert_true( i % 4 != 3 || ( csr_matrix.indices()[i] > 501 ) );
        }
    std::cout << std::endl << std::endl;

    std::cout << "data: ";
    for ( size_t i = 0; i < 80; ++i )
        {
            std::cout << csr_matrix.data()[i] << " ";
            assert_true( i % 4 != 2 || csr_matrix.data()[i] == 1.0 );
            assert_true( i % 4 != 3 || csr_matrix.data()[i] == 1.0 );
        }
    std::cout << std::endl << std::endl;

    // ------------------------------------------------------------------------

    std::cout << "OK." << std::endl << std::endl;

    // ------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
