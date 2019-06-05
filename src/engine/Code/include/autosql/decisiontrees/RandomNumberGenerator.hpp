#ifndef AUTOSQL_DECISIONTREES_RANDOMNUMBERGENERATOR_HPP_
#define AUTOSQL_DECISIONTREES_RANDOMNUMBERGENERATOR_HPP_

namespace autosql
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

class RandomNumberGenerator
{
   public:
    RandomNumberGenerator(
        std::mt19937* const _random_number_generator,
        AUTOSQL_COMMUNICATOR* const _comm )
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

#ifdef AUTOSQL_PARALLEL

        assert( comm_ != nullptr );

        AUTOSQL_PARALLEL_LIB::broadcast( *comm_, random, 0 );

        comm_->barrier();

#endif  // AUTOSQL_PARALLEL

        return random;
    }

    /// Returns a random integer in the range (_min, _max).
    AUTOSQL_INT random_int( const AUTOSQL_INT _min, const AUTOSQL_INT _max )
    {
        std::uniform_int_distribution<> uniform_distribution( _min, _max );

        auto random = uniform_distribution( *random_number_generator_ );

#ifdef AUTOSQL_PARALLEL

        assert( comm_ != nullptr );

        AUTOSQL_PARALLEL_LIB::broadcast( *comm_, random, 0 );

        comm_->barrier();

#endif  // AUTOSQL_PARALLEL

        return random;
    }

    // ------------------------------

   private:
    /// Communicator
    AUTOSQL_COMMUNICATOR* const comm_;

    /// Random number generator
    std::mt19937* const random_number_generator_;

    // ------------------------------
};

// ----------------------------------------------------------------------------
}
}
#endif  // AUTOSQL_DECISIONTREES_RANDOMNUMBERGENERATOR_HPP_
