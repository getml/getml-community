#ifndef AUTOSQL_UTILS_RANDOMNUMBERGENERATOR_HPP_
#define AUTOSQL_UTILS_RANDOMNUMBERGENERATOR_HPP_

namespace autosql
{
namespace utils
{
// ----------------------------------------------------------------------------

class RandomNumberGenerator
{
   public:
    RandomNumberGenerator(
        std::mt19937* _random_number_generator,
        multithreading::Communicator* _comm )
        : comm_( _comm ), random_number_generator_( _random_number_generator )
    {
    }

    ~RandomNumberGenerator() = default;

    // ------------------------------

   public:
    /// Returns a random integer in the range (_min, _max).
    AUTOSQL_FLOAT random_float(
        const AUTOSQL_FLOAT _min, const AUTOSQL_FLOAT _max )
    {
        std::uniform_real_distribution<> uniform_distribution( _min, _max );

        auto random = uniform_distribution( *random_number_generator_ );

        assert( comm_ != nullptr );

        multithreading::broadcast( *comm_, random, 0 );

        comm_->barrier();

        return random;
    }

    /// Returns a random integer in the range (_min, _max).
    AUTOSQL_INT random_int( const AUTOSQL_INT _min, const AUTOSQL_INT _max )
    {
        std::uniform_int_distribution<> uniform_distribution( _min, _max );

        auto random = uniform_distribution( *random_number_generator_ );

        assert( comm_ != nullptr );

        multithreading::broadcast( *comm_, random, 0 );

        comm_->barrier();

        return random;
    }

    // ------------------------------

   private:
    /// Communicator
    multithreading::Communicator* const comm_;

    /// Random number generator
    std::mt19937* const random_number_generator_;

    // ------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace autosql

#endif  // AUTOSQL_UTILS_RANDOMNUMBERGENERATOR_HPP_
