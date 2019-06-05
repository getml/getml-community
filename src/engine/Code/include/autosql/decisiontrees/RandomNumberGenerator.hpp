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
        SQLNET_COMMUNICATOR* const _comm )
        : comm_( _comm ), random_number_generator_( _random_number_generator )
    {
    }

    ~RandomNumberGenerator() = default;

    // ------------------------------

   public:
    /// Returns a random integer in the range (_min, _max).
    SQLNET_FLOAT random_float(
        const SQLNET_FLOAT _min, const SQLNET_FLOAT _max )
    {
        std::uniform_real_distribution<> uniform_distribution( _min, _max );

        auto random = uniform_distribution( *random_number_generator_ );

#ifdef SQLNET_PARALLEL

        assert( comm_ != nullptr );

        SQLNET_PARALLEL_LIB::broadcast( *comm_, random, 0 );

        comm_->barrier();

#endif  // SQLNET_PARALLEL

        return random;
    }

    /// Returns a random integer in the range (_min, _max).
    SQLNET_INT random_int( const SQLNET_INT _min, const SQLNET_INT _max )
    {
        std::uniform_int_distribution<> uniform_distribution( _min, _max );

        auto random = uniform_distribution( *random_number_generator_ );

#ifdef SQLNET_PARALLEL

        assert( comm_ != nullptr );

        SQLNET_PARALLEL_LIB::broadcast( *comm_, random, 0 );

        comm_->barrier();

#endif  // SQLNET_PARALLEL

        return random;
    }

    // ------------------------------

   private:
    /// Communicator
    SQLNET_COMMUNICATOR* const comm_;

    /// Random number generator
    std::mt19937* const random_number_generator_;

    // ------------------------------
};

// ----------------------------------------------------------------------------
}
}
#endif  // AUTOSQL_DECISIONTREES_RANDOMNUMBERGENERATOR_HPP_
