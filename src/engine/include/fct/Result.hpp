// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FCT_RESULT_HPP_
#define FCT_RESULT_HPP_

#include <tuple>
#include <type_traits>
#include <variant>

namespace fct {

/// To be returned
class Error {
 public:
  Error(const std::string& _what) : what_(_what) {}

  ~Error() = default;

  /// Returns the error message, equivalent to .what() in std::exception.
  const std::string& what() const { return what_; }

 private:
  /// Documents what went wrong
  std::string what_;
};

/// Can be used when we are simply interested in whether an operation was
/// successful.
struct Nothing {};

/// Helper class to be used to within the Result class to support the operator*.
/// This is necessary, because we only want this class to be unwrapped, not
/// normal std::tuple.
template <class... Args>
struct HelperTuple {
  std::tuple<Args...> tuple_;
};

template <class>
struct is_helper_tuple : std::false_type {};

template <class... Args>
struct is_helper_tuple<HelperTuple<Args...>> : std::true_type {};

/// The Result class is used for monadic error handling.
template <class T>
class Result {
  static_assert(!std::is_same<T, Error>(), "The result type cannot be Error.");

 public:
  Result(const T& _val) : t_or_err_(_val) {}

  Result(T&& _val) : t_or_err_(_val) {}

  Result(const Error& _err) : t_or_err_(_err) {}

  Result(Error&& _err) : t_or_err_(_err) {}

  ~Result() = default;

  /// Monadic operation - F must be a function of type T -> Result<U>.
  template <class F>
  auto and_then(const F& _f) const {
    const auto apply = [&_f](const auto& _t) {
      if constexpr (is_helper_tuple<T>()) {
        return std::apply(_f, _t.tuple_);
      } else {
        return _f(_t);
      }
    };

    /// Result_U is expected to be of type Result<U>.
    using Result_U = typename std::invoke_result<decltype(apply), T>::type;

    const auto handle_variant =
        [apply]<class TOrError>(const TOrError& _t_or_err) -> Result_U {
      if constexpr (!std::is_same<TOrError, Error>()) {
        return apply(_t_or_err);
      } else {
        return _t_or_err;
      }
    };

    return std::visit(handle_variant, t_or_err_);
  }

  /// Results types can be iterated over, which even make it possible to use
  /// them within a std::range.
  // TODO: Fix nullptr
  /*  const T* begin() const {
      const auto get_ptr = [this]<class TOrError>(
          const TOrError& _t_or_err) -> const auto* {
        if constexpr (!std::is_same<TOrError, Error>()) {
          if constexpr (is_helper_tuple<T>()) {
            return &_t_or_err.tuple_;
          } else {
            return &_t_or_err;
          }
        } else {
          return nullptr;
        }
      };
      return std::visit(get_ptr, t_or_err_);
    }*/

  /// Results types can be iterated over, which even make it possible to use
  /// them within a std::range.
  // TODO: Fix nullptr
  /*const T* end() const {
    const auto get_ptr = [this]<class TOrError>(
        const TOrError& _t_or_err) -> const auto* {
      if constexpr (!std::is_same<TOrError, Error>()) {
        if constexpr (is_helper_tuple<T>()) {
          return &_t_or_err.tuple_ + 1;
        } else {
          return &_t_or_err + 1;
        }
      } else {
        return nullptr;
      }
    };
    return std::visit(get_ptr, t_or_err_);
  }*/

  /// Functor operation - F must be a function of type T -> U.
  template <class F>
  auto transform(const F& _f) const {
    // Makes use of the theoretical insight that every monad is also a functor.
    const auto f = [&_f](const auto& _t) { return Result(_f(_t)); };
    return and_then(f);
  }

  /// Returns the value or a default.
  T value_or(const T& _default) const {
    const auto handle_variant =
        [&]<class TOrError>(const TOrError& _t_or_err) -> T {
      if constexpr (!std::is_same<TOrError, Error>()) {
        return _t_or_err;
      } else {
        return _default;
      }
    };
    return std::visit(handle_variant, t_or_err_);
  }

 private:
  /// The underlying variant, can either be T or Error.
  std::variant<T, Error> t_or_err_;
};

/// operator* allows to combine result types as a product type.

template <class T, class U>
inline auto operator*(Result<T>&& _rt, Result<U>&& _ru) {
  const auto f1 = [&_ru](T&& _t) {
    const auto f2 = [&_t](U&& _u) {
      return HelperTuple<T, U>(std::forward_as_tuple(_t, _u));
    };
    return _ru.transform(f2);
  };
  return _rt.and_then(f1);
}

template <class... T, class... U>
inline auto operator*(Result<HelperTuple<T...>>&& _rt,
                      Result<HelperTuple<U...>>&& _ru) {
  const auto f1 = [&_ru](T&&... _t) {
    const auto f2 = [&](U&&... _u) {
      return HelperTuple<T..., U...>(std::forward_as_tuple(_t..., _u...));
    };
    return _ru.transform(f2);
  };
  return _rt.and_then(f1);
}

template <class... T, class U>
inline auto operator*(Result<HelperTuple<T...>>&& _rt, Result<U>&& _ru) {
  const auto f = [](U&& _u) {
    return HelperTuple<U>(std::forward_as_tuple(_u));
  };
  return _rt * _ru.transform(f);
}

template <class T, class... U>
inline auto operator*(Result<T>&& _rt, Result<HelperTuple<U...>>&& _ru) {
  const auto f = [](T&& _t) {
    return HelperTuple<T>(std::forward_as_tuple(_t));
  };
  return _rt.transform(f) * _ru;
}

template <class T, class U>
inline auto operator*(Result<T>&& _rt, U&& _u) {
  return _rt * Result<U>(_u);
}

template <class T, class U>
inline auto operator*(T&& _t, Result<U>&& _ru) {
  return Result<T>(_t) * _ru;
}

}  // namespace fct

#endif

