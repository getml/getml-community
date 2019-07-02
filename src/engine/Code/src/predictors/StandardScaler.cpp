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

// -------------------------------------------------------------------------
}  // namespace predictors
