// This has been taken from cista++ (https://github.com/felixguendling/cista)
// and slighly modified to fit the style of this library.

// Copyright (c) 2018-2021 Felix Gündling

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#ifndef RFL_INTERNAL_ARITY_HPP_
#define RFL_INTERNAL_ARITY_HPP_

#include <cstddef>
#include <type_traits>

// Credits: Implementation by Anatoliy V. Tomilov (@tomilov),
//          based on gist by Rafal T. Janik (@ChemiaAion)
//
// Resources:
// https://playfulprogramming.blogspot.com/2016/12/serializing-structs-with-c17-structured.html
// https://codereview.stackexchange.com/questions/142804/get-n-th-data-member-of-a-struct
// https://stackoverflow.com/questions/39768517/structured-bindings-width
// https://stackoverflow.com/questions/35463646/arity-of-aggregate-in-logarithmic-time
// https://stackoverflow.com/questions/38393302/returning-variadic-aggregates-struct-and-syntax-for-c17-variadic-template-c

namespace rfl {
namespace internal {

struct instance {
  template <typename Type>
  operator Type() const;
};

template <typename Aggregate, typename IndexSequence = std::index_sequence<>,
          typename = void>
struct arity_impl : IndexSequence {};

template <typename Aggregate, std::size_t... Indices>
struct arity_impl<Aggregate, std::index_sequence<Indices...>,
                  std::void_t<decltype(Aggregate{
                      (static_cast<void>(Indices), std::declval<instance>())...,
                      std::declval<instance>()})>>
    : arity_impl<Aggregate,
                 std::index_sequence<Indices..., sizeof...(Indices)>> {};

template <typename T>
constexpr std::size_t arity() noexcept {
  return arity_impl<std::decay_t<T>>().size();
}

}  // namespace internal
}  // namespace rfl
