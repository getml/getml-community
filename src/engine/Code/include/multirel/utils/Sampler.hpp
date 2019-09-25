#ifndef MULTIREL_UTILS_SAMPLER_HPP_
#define MULTIREL_UTILS_SAMPLER_HPP_

// ----------------------------------------------------------------------------

namespace multirel
{
namespace utils
{
// ------------------------------------------------------------------------

class Sampler
{
    // --------------------------------------------------------------------

   public:
    Sampler( const unsigned int _seed )
        : random_number_generator_( std::mt19937( _seed ) )
    {
    }

    ~Sampler() = default;

    // --------------------------------------------------------------------

   public:
    /// Calculates the sampling rate (as opposed to the sampling factor, which
    /// can be set by the user).
    void calc_sampling_rate(
        const size_t _num_rows,
        const Float _sampling_factor,
        multithreading::Communicator* _comm );

    /// Generates a new set of sample weights.
    std::shared_ptr<std::vector<Float>> make_sample_weights(
        const size_t _num_rows );

    // --------------------------------------------------------------------

   private:
    /// The random number generator used for sampling.
    std::mt19937 random_number_generator_;

    /// The share of samples taken from the population table.
    Float sampling_rate_;

    // --------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace multirel

// ----------------------------------------------------------------------------

#endif  // MULTIREL_UTILS_SAMPLER_HPP_
