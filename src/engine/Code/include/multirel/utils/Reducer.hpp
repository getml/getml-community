#ifndef MULTIREL_UTILS_REDUCER_HPP_
#define MULTIREL_UTILS_REDUCER_HPP_

namespace multirel
{
namespace utils
{
// ----------------------------------------------------------------------------

class Reducer
{
    // ------------------------------------------------------------------------

   public:
    /// Reduces a value in a multithreading context.
    template <typename T, typename OperatorType>
    static void reduce(
        const OperatorType& _operator,
        T* _val,
        multithreading::Communicator* _comm )
    {
        auto global = static_cast<T>( 0 );

        multithreading::all_reduce(
            *_comm,    // comm
            _val,      // in_values
            1,         // count
            &global,   // out_values
            _operator  // op
        );

        _comm->barrier();

        *_val = global;
    }

    // ------------------------------------------------------------------------

    /// Reduces a vector in a multithreading context.
    template <typename T, typename OperatorType>
    static void reduce(
        const OperatorType& _operator,
        std::vector<T>* _vec,
        multithreading::Communicator* _comm )
    {
        std::vector<T> global( _vec->size() );

        multithreading::all_reduce(
            *_comm,         // comm
            _vec->data(),   // in_values
            _vec->size(),   // count,
            global.data(),  // out_values
            _operator       // op
        );

        _comm->barrier();

        *_vec = std::move( global );
    }

    // ------------------------------------------------------------------------

    /// Reduces an array in a multithreading context.
    template <int count, typename T, typename OperatorType>
    static void reduce(
        const OperatorType& _operator,
        std::array<T, count>* _arr,
        multithreading::Communicator* _comm )
    {
        std::array<T, count> global;

        multithreading::all_reduce(
            *_comm,         // comm
            _arr->data(),   // in_values
            count,          // count,
            global.data(),  // out_values
            _operator       // op
        );

        _comm->barrier();

        *_arr = std::move( global );
    }

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace multirel

// ----------------------------------------------------------------------------

#endif  // MULTIREL_UTILS_REDUCER_HPP_
