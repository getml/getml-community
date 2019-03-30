#include "utils/utils.hpp"

namespace relboost
{
namespace utils
{
// ----------------------------------------------------------------------------

std::shared_ptr<const std::vector<RELBOOST_FLOAT>> Sampler::make_sample_weights(
    const size_t _num_rows )
{
    auto sample_weights =
        std::make_shared<std::vector<RELBOOST_FLOAT>>( _num_rows );

    if ( hyperparameters_->subsample_ <= 0.0 )
        {
            std::fill( sample_weights->begin(), sample_weights->end(), 1.0 );

            return sample_weights;
        }

    std::uniform_int_distribution<> dist( 0, _num_rows - 1 );

    const auto num_samples = static_cast<size_t>(
        static_cast<RELBOOST_FLOAT>( _num_rows ) *
        hyperparameters_->subsample_ );

    for ( size_t i = 0; i < num_samples; ++i )
        {
            ( *sample_weights )[dist( random_number_generator_ )] += 1.0;
        }

    return sample_weights;
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost