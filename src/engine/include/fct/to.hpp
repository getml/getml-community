#ifndef FCT_TO_HPP
#define FCT_TO_HPP

#include <algorithm>
#include <iterator>
#include <map>
#include <memory>
#include <ranges>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace fct {

namespace shared_ptr {
template <typename T>
using vector = std::shared_ptr<std::vector<T>>;

template <typename Key, typename Value>
using map = std::shared_ptr<std::map<Key, Value>>;
}  // namespace shared_ptr

namespace ranges {

namespace detail {
template <template <typename...> typename Container>
struct ToNested {
  template <typename Range>
  auto operator()(Range&& range) const
      -> Container<std::ranges::range_value_t<Range>> {
    auto container = Container<std::ranges::range_value_t<Range>>{};
    std::ranges::move(range, std::back_inserter(container));
    return container;
  }
};

template <>
struct ToNested<std::map> {
  template <typename Range>
  auto operator()(Range&& range) const
      -> std::map<std::remove_cv_t<
                      typename std::ranges::range_value_t<Range>::first_type>,
                  typename std::ranges::range_value_t<Range>::second_type> {
    auto map =
        std::map<std::remove_cv_t<
                     typename std::ranges::range_value_t<Range>::first_type>,
                 typename std::ranges::range_value_t<Range>::second_type>{};
    std::ranges::move(range, std::inserter(map, map.end()));
    return map;
  }
};

template <>
struct ToNested<shared_ptr::map> {
  template <typename Range>
  auto operator()(Range&& range) const
      -> std::shared_ptr<
          std::map<std::remove_cv_t<
                       typename std::ranges::range_value_t<Range>::first_type>,
                   typename std::ranges::range_value_t<Range>::second_type>> {
    auto map = std::make_shared<
        std::map<std::remove_cv_t<
                     typename std::ranges::range_value_t<Range>::first_type>,
                 typename std::ranges::range_value_t<Range>::second_type>>();
    std::ranges::move(range, std::inserter(*map, map->end()));
    return map;
  }
};

template <>
struct ToNested<std::set> {
  template <typename Range>
  auto operator()(Range&& range) const
      -> std::set<std::ranges::range_value_t<Range>> {
    auto set = std::set<std::ranges::range_value_t<Range>>{};
    std::ranges::move(range, std::inserter(set, set.end()));
    return set;
  }
};

template <>
struct ToNested<std::vector> {
  template <typename Range>
  auto operator()(Range&& range) const
      -> std::vector<std::ranges::range_value_t<Range>> {
    auto vector = std::vector<std::ranges::range_value_t<Range>>{};
    if constexpr (std::ranges::sized_range<Range>) {
      vector.reserve(std::size(range));
    }
    std::ranges::move(range, std::back_inserter(vector));
    return vector;
  }
};

template <>
struct ToNested<shared_ptr::vector> {
  template <typename Range>
  auto operator()(Range&& range) const
      -> std::shared_ptr<std::vector<std::ranges::range_value_t<Range>>> {
    auto vector =
        std::make_shared<std::vector<std::ranges::range_value_t<Range>>>();
    if constexpr (std::ranges::sized_range<Range>) {
      vector->reserve(std::size(range));
    }
    std::ranges::move(range, std::back_inserter(*vector));
    return vector;
  }
};

template <typename Container>
struct ToBasic {
  template <typename Range>
  auto operator()(Range&& range) const -> Container {
    auto container = Container{};
    std::ranges::move(range, std::back_inserter(container));
    return container;
  }
};

template <>
struct ToBasic<std::string> {
  template <typename Range>
  auto operator()(Range&& range) const -> std::string {
    auto stream = std::stringstream{};
    std::ranges::for_each(range,
                          [&stream](const auto& elem) { stream << elem; });
    return stream.str();
  }
};

}  // namespace detail

template <template <typename...> typename Container>
constexpr auto to() -> detail::ToNested<Container> {
  return detail::ToNested<Container>();
}

template <typename Container>
constexpr auto to() -> detail::ToBasic<Container> {
  return detail::ToBasic<Container>();
}

}  // namespace ranges
}  // namespace fct

template <typename Range, template <typename...> typename Container>
auto operator|(Range&& range,
               fct::ranges::detail::ToNested<Container>&& toNested) {
  return toNested(std::forward<Range>(range));
}

template <typename Range, typename Container>
auto operator|(Range&& range,
               fct::ranges::detail::ToBasic<Container>&& toBasic) -> Container {
  return toBasic(std::forward<Range>(range));
}

#endif  // FCT_TO_HPP
