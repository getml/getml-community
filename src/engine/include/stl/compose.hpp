#ifndef STL_COMPOSE_HPP_
#define STL_COMPOSE_HPP_

namespace stl {

/// Composes two functions.
template <class FunctionType1, class FunctionType2>
inline auto compose(const FunctionType1& _func1, const FunctionType2& _func2) {
  return [_func1, _func2](auto... args) { return _func2(_func1(args...)); };
}

/// Composes more than two functions.
template <class FunctionType1, class... OtherFunctionTypes>
inline auto compose(const FunctionType1& _func1,
                    const OtherFunctionTypes&... _other_funcs) {
  const auto func2 = compose(_other_funcs...);
  return [_func1, func2](auto... args) { return func2(_func1(args...)); };
}

}  // namespace stl

#endif  // STL_COMPOSE_HPP_
