#ifndef MULTITHREADING_MINIMUM_HPP_
#define MULTITHREADING_MINIMUM_HPP_

namespace multithreading {
// ----------------------------------------------------------------------------

template <class T>
struct minimum {
 public:
  minimum() {}

  ~minimum() = default;

  // -----------------------------------------

  T operator()(const T& _elem1, const T& _elem2) {
    return ((_elem1 < _elem2) ? (_elem1) : (_elem2));
  }
};

// ----------------------------------------------------------------------------
}  // namespace multithreading

#endif  // MULTITHREADING_MINIMUM_HPP_
