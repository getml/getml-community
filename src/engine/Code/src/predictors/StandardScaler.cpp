#include "predictors/predictors.hpp"

namespace predictors
{
// -----------------------------------------------------------------------------

void StandardScaler::fit( const std::vector<CFloatColumn>& _X_numerical )
{
    std_.resize( _X_numerical.size() );

    for ( size_t j = 0; j < _X_numerical.size(); ++j )
        {
            const auto n = static_cast<Float>( _X_numerical[j]->size() );

            const auto mean =
                std::accumulate(
                    _X_numerical[j]->begin(), _X_numerical[j]->end(), 0.0 ) /
                n;

            auto mult = [mean, n]( Float val1, Float val2 ) {
                return ( val1 - mean ) * ( val2 - mean ) / n;
            };

            std_[j] = std::inner_product(
                _X_numerical[j]->begin(),
                _X_numerical[j]->end(),
                _X_numerical[j]->begin(),
                0.0,
                std::plus<Float>(),
                mult );

            std_[j] = std::sqrt( std_[j] );
        }
}

// -----------------------------------------------------------------------------

void StandardScaler::fit(
    const CSRMatrix<Float, unsigned int, size_t>& _X_sparse )
{
    // -------------------------------------------------------------------------

    const auto n = static_cast<Float>( _X_sparse.nrows() );

    // -------------------------------------------------------------------------
    // Calculate means

    auto means = std::vector<Float>( _X_sparse.ncols() );

    for ( size_t i = 0; i < _X_sparse.nrows(); ++i )
        {
            for ( size_t j = _X_sparse.indptr()[i];
                  j < _X_sparse.indptr()[i + 1];
                  ++j )
                {
                    assert( _X_sparse.indices()[j] < _X_sparse.ncols() );

                    means[_X_sparse.indices()[j]] += _X_sparse.data()[j];
                }
        }

    for ( auto& m : means )
        {
            m /= n;
        }

    // -------------------------------------------------------------------------
    // Calculate std_

    std_.resize( _X_sparse.ncols() );

    for ( size_t i = 0; i < _X_sparse.nrows(); ++i )
        {
            for ( size_t j = _X_sparse.indptr()[i];
                  j < _X_sparse.indptr()[i + 1];
                  ++j )
                {
                    assert( _X_sparse.indices()[j] < _X_sparse.ncols() );

                    auto diff =
                        ( _X_sparse.data()[j] - means[_X_sparse.indices()[j]] );

                    std_[_X_sparse.indices()[j]] += diff * diff;
                }
        }

    for ( auto& s : std_ )
        {
            s /= n;
            s = std::sqrt( s );
        }

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

std::vector<CFloatColumn> StandardScaler::transform(
    const std::vector<CFloatColumn>& _X_numerical ) const
{
    assert( _X_numerical.size() > 0 );
    assert( _X_numerical.size() == std_.size() );

    std::vector<CFloatColumn> output;

    for ( size_t j = 0; j < _X_numerical.size(); ++j )
        {
            output.push_back( std::make_shared<std::vector<Float>>(
                _X_numerical[j]->size() ) );

            if ( std_[j] != 0.0 )
                {
                    for ( size_t i = 0; i < _X_numerical[j]->size(); ++i )
                        {
                            ( *output.back() )[i] =
                                ( *_X_numerical[j] )[i] / std_[j];
                        }
                }
        }

    return output;
}

// -----------------------------------------------------------------------------

const CSRMatrix<Float, unsigned int, size_t> StandardScaler::transform(
    const CSRMatrix<Float, unsigned int, size_t>& _X_sparse ) const
{
    auto output = _X_sparse;

    for ( size_t i = 0; i < output.nrows(); ++i )
        {
            for ( size_t j = output.indptr()[i]; j < output.indptr()[i + 1];
                  ++j )
                {
                    assert( output.indices()[j] < output.ncols() );

                    const auto std = std_[_X_sparse.indices()[j]];

                    if ( std != 0.0 )
                        {
                            output.data()[j] /= std;
                        }
                    else
                        {
                            output.data()[j] = 0.0;
                        }
                }
        }

    return output;
}

// -------------------------------------------------------------------------
}  // namespace predictors
