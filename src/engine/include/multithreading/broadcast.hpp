// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef MULTITHREADING_BROADCAST_HPP_
#define MULTITHREADING_BROADCAST_HPP_

// ----------------------------------------------------------------------------

#include <algorithm>
#include <cstddef>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"

// ----------------------------------------------------------------------------

#include "multithreading/Communicator.hpp"

// ----------------------------------------------------------------------------
namespace multithreading {
// ----------------------------------------------------------------------------

template <class T>
void broadcast(Communicator& _comm, T* _values, const size_t _count,
               const size_t _root) {
  // ---------------------------------------------------------
  // Root is contained to ensure compatability with MPI, but
  // in reality root process must always be 0.

  assert_true(_root == 0 && "broadcast");

  // ---------------------------------------------------------
  // The number of threads might be one - this happens surprisingly
  // often. In this case, there is nothing to be done.

  if (_comm.num_threads() == 1) {
    return;
  }

  // ---------------------------------------------------------
  // Copy from main thread to global data (since only on thread can be
  // the main thread, no lock is needed).

  if (_comm.main_thread_id() == std::this_thread::get_id()) {
    _comm.resize<T>(_count);

    std::copy(_values, _values + _count, _comm.global_data<T>());

    _comm.barrier();
  } else {
    _comm.barrier();

    std::copy(_comm.global_data_const<T>(),
              _comm.global_data_const<T>() + _count, _values);
  }
}

template <class T>
void broadcast(Communicator& _comm, T* _values, const size_t _count,
               const size_t _root);

// ----------------------------------------------------------------------------

template <class T>
void broadcast(Communicator& _comm, T& _value, const size_t _root) {
  broadcast(_comm, &_value, 1, _root);
}

template <class T>
void broadcast(Communicator& _comm, T& _value, const size_t _root);

// ----------------------------------------------------------------------------
}  // namespace multithreading

#endif  // MULTITHREADING_BROADCAST_HPP_
