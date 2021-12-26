#ifndef MULTITHREADING_ALL_REDUCE_HPP_
#define MULTITHREADING_ALL_REDUCE_HPP_

// ----------------------------------------------------------------------------

#include <algorithm>
#include <cstddef>

// ----------------------------------------------------------------------------

#include "multithreading/Communicator.hpp"

// ----------------------------------------------------------------------------

namespace multithreading {
// ----------------------------------------------------------------------------

template <class T, class Operator>
void all_reduce(Communicator& _comm, const T* _in_values, const size_t _count,
                T* _out_values, const Operator _op) {
  // ---------------------------------------------------------
  // The number of threads might be one - this happens surprisingly
  // often. In this case, no synchonization is needed.

  if (_comm.num_threads() == 1) {
    std::copy(_in_values, _in_values + _count, _out_values);
    return;
  }

  // ---------------------------------------------------------

  _comm.lock();

  if ((_comm.num_threads_left())-- == _comm.num_threads()) {
    _comm.resize<T>(_count);

    std::copy(_in_values, _in_values + _count, _comm.global_data<T>());
  } else {
    std::transform(_comm.global_data_const<T>(),
                   _comm.global_data_const<T>() + _count, _in_values,
                   _comm.global_data<T>(), _op);

    // If there are no threads left, reset num_threads_left
    if (_comm.num_threads_left() == 0) {
      _comm.num_threads_left() = _comm.num_threads();
    }
  }

  _comm.unlock();

  // ---------------------------------------------------------
  // Once all threads have reached this point, copy the global data
  // to _out_values (because the access to the global data is read_only,
  // no locking is needed).

  _comm.barrier();

  std::copy(_comm.global_data_const<T>(), _comm.global_data_const<T>() + _count,
            _out_values);
}

// ----------------------------------------------------------------------------

template <class T, class Operator>
void all_reduce(Communicator& _comm, const T* _in_values, const size_t _count,
                T* _out_values, const Operator _op);

// ----------------------------------------------------------------------------

template <class T, class Operator>
void all_reduce(Communicator& _comm, const T& _in_values, T& _out_values,
                const Operator _op) {
  all_reduce(_comm, &_in_values, 1, &_out_values, _op);
}

template <class T, class Operator>
void all_reduce(Communicator& _comm, const T& _in_values, T& _out_values,
                const Operator _op);

// ----------------------------------------------------------------------------
}  // namespace multithreading

#endif  // MULTITHREADING_ALL_REDUCE_HPP_
