#ifndef AUTOSQL_UTILS_SAMPLER_HPP_
#define AUTOSQL_UTILS_SAMPLER_HPP_

// ----------------------------------------------------------------------------

namespace autosql
{
namespace utils
{
// ------------------------------------------------------------------------

class Sampler
{
    // --------------------------------------------------------------------

   public:
    Sampler( const size_t _seed )
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
        const AUTOSQL_FLOAT _sampling_factor,
        multithreading::Communicator* _comm );

    /// Generates a new set of sample weights.
    std::shared_ptr<std::vector<AUTOSQL_FLOAT>> make_sample_weights(
        const size_t _num_rows );

    // --------------------------------------------------------------------

    /// Trivial getter.
    AUTOSQL_FLOAT sampling_rate() const { return sampling_rate_; }

    // --------------------------------------------------------------------

   private:
    /// The random number generator used for sampling.
    std::mt19937 random_number_generator_;

    /// The share of samples taken from the population table.
    AUTOSQL_FLOAT sampling_rate_;

    // --------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace autosql

// ----------------------------------------------------------------------------

#endif  // AUTOSQL_UTILS_SAMPLER_HPP_