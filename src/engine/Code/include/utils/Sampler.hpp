#ifndef RELBOOST_UTILS_SAMPLER_HPP_
#define RELBOOST_UTILS_SAMPLER_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace utils
{
// ------------------------------------------------------------------------

class Sampler
{
    // --------------------------------------------------------------------

   public:
    Sampler( std::shared_ptr<const Hyperparameters> _hyperparameters )
        : hyperparameters_( _hyperparameters ),
          random_number_generator_( std::mt19937( _hyperparameters->seed_ ) )
    {
    }

    ~Sampler() = default;

    // --------------------------------------------------------------------

   public:
    /// Generates a new set of sample weights.
    std::shared_ptr<const std::vector<RELBOOST_FLOAT>> make_sample_weights(
        const size_t _num_rows );

    // --------------------------------------------------------------------

   private:
    /// Hyperparameters used to train the relboost model.
    std::shared_ptr<const Hyperparameters> hyperparameters_;

    /// The random number generator used for sampling.
    std::mt19937 random_number_generator_;

    // --------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_UTILS_SAMPLER_HPP_