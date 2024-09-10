// Copyright 2024 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef MULTITHREADING_REDUCER_HPP_
#define MULTITHREADING_REDUCER_HPP_

// ----------------------------------------------------------------------------

#include <vector>

// ----------------------------------------------------------------------------

#include "multithreading/Communicator.hpp"

// ----------------------------------------------------------------------------

namespace multithreading {
// ----------------------------------------------------------------------------

class Reducer {
  // ------------------------------------------------------------------------

 public:
  /// Reduces a value in a multithreading context.
  template <typename T, typename OperatorType>
  static void reduce(const OperatorType& _operator, T* _val,
                     Communicator* _comm) {
    auto global = static_cast<T>(0);

    all_reduce(*_comm,    // comm
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
  static void reduce(const OperatorType& _operator, std::vector<T>* _vec,
                     Communicator* _comm) {
    std::vector<T> global(_vec->size());

    all_reduce(*_comm,         // comm
               _vec->data(),   // in_values
               _vec->size(),   // count,
               global.data(),  // out_values
               _operator       // op
    );

    _comm->barrier();

    *_vec = std::move(global);
  }

  // ------------------------------------------------------------------------

  /// Reduces an array in a multithreading context.
  template <int count, typename T, typename OperatorType>
  static void reduce(const OperatorType& _operator, std::array<T, count>* _arr,
                     Communicator* _comm) {
    std::array<T, count> global;

    all_reduce(*_comm,         // comm
               _arr->data(),   // in_values
               count,          // count,
               global.data(),  // out_values
               _operator       // op
    );

    _comm->barrier();

    *_arr = std::move(global);
  }

  // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace multithreading

// ----------------------------------------------------------------------------

#endif  // MULTITHREADING_REDUCER_HPP_
