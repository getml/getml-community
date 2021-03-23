#include "multirel/utils/utils.hpp"

namespace multirel
{
namespace utils
{
// ----------------------------------------------------------------------------

void Sampler::calc_sampling_rate(
    const size_t _num_rows,
    const Float _sampling_factor,
    multithreading::Communicator* _comm )
{
    auto global_num_rows = _num_rows;

    Reducer::reduce( std::plus<size_t>(), &global_num_rows, _comm );

    sampling_rate_ = std::min(
        1.0,
        _sampling_factor * 20000.0 / static_cast<Float>( global_num_rows ) );
}

// ----------------------------------------------------------------------------

std::shared_ptr<std::vector<Float>> Sampler::make_sample_weights(
    const size_t _num_rows )
{
    if ( sampling_rate_ <= 0.0 )
        {
            return std::make_shared<std::vector<Float>>( _num_rows, 1.0 );
        }

    auto sample_weights = std::make_shared<std::vector<Float>>( _num_rows );

    if ( sampling_rate_ <= 0.0 )
        {
            std::fill( sample_weights->begin(), sample_weights->end(), 1.0 );

            return sample_weights;
        }

    std::uniform_int_distribution<size_t> dist( 0, _num_rows - 1 );

    const auto num_samples =
        static_cast<size_t>( static_cast<Float>( _num_rows ) * sampling_rate_ );

    for ( size_t i = 0; i < num_samples; ++i )
        {
            ( *sample_weights )[dist( random_number_generator_ )] += 1.0;
        }

    return sample_weights;
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace multirel
