#ifndef MULTITHREADING_PARALLEL_FOR_EACH_HPP_
#define MULTITHREADING_PARALLEL_FOR_EACH_HPP_

// ----------------------------------------------------------------------------

#include <execution>
#include <memory>
#include <thread>

// ----------------------------------------------------------------------------

namespace multithreading {
// ----------------------------------------------------------------------------

template <class IteratorType, class OpType>
void parallel_for_each(const IteratorType _begin, const IteratorType _end,
                       const OpType _op) {
  const auto has_failed = std::make_shared<std::atomic_flag>();

  has_failed->clear();

  const auto error_message = std::make_shared<std::string>();

  const auto wrapped_operator = [has_failed, error_message,
                                 _op](const auto& _elem) {
    try {
      _op(_elem);
    } catch (std::exception& e) {
      const bool has_failed_before = has_failed->test_and_set();

      if (!has_failed_before) {
        *error_message = e.what();
      }
    }
  };

  std::for_each(std::execution::par, _begin, _end, wrapped_operator);

  if (*error_message != "") {
    throw std::runtime_error(*error_message);
  }
}

// ----------------------------------------------------------------------------
}  // namespace multithreading

#endif  // MULTITHREADING_PARALLEL_FOR_EACH_HPP_
