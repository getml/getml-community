#include <gtest/gtest.h>

#include <cmath>
#include <range/v3/view/concat.hpp>
#include <ranges>
#include <string>

#include "fct/to.hpp"
#include "gwt.h"

using namespace std::literals::string_literals;

TEST(TestTo, TestToVector) {
  GWT::given(std::vector{1, 2, 3, 4, 5})
      .when([](auto const&& vector) {
        return vector | fct::ranges::to<std::vector>();
      })
      .then([](auto const&& result) {
        auto const expected = std::vector{1, 2, 3, 4, 5};
        EXPECT_EQ(expected, result);
      });
}

TEST(TestTo, TestConcatToVector) {
  GWT::given([first_items = std::vector{"Hello"s, "World"s, "!"s},
              second_items = std::vector{"Good"s, "Bye"s, "."s}]() {
    return ranges::views::concat(first_items, second_items);
  })
      .when([](auto const&& view) {
        return view | fct::ranges::to<std::vector>();
      })
      .then([](auto const&& result) {
        auto const expected =
            std::vector{"Hello"s, "World"s, "!"s, "Good"s, "Bye"s, "."s};
        EXPECT_EQ(expected, result);
      });
}

TEST(TestTo, TestIotaFilterTransformToSharedPtrVector) {
  GWT::given([]() {
    return std::views::iota(0u, 10u) |
           std::views::filter(
               [](auto const& value) { return value % 2 == 0; }) |
           std::views::transform([](auto const& value) -> std::uint32_t {
             return value * value;
           });
  })
      .when([](auto&& view) {
        return view | fct::ranges::to<fct::shared_ptr::vector>();
      })
      .then([](auto const&& result) {
        auto const expected = std::vector{0u, 4u, 16u, 36u, 64u};
        EXPECT_EQ(expected, *result);
      });
}

TEST(TestTo, TestToMap) {
  GWT::given([vector = std::vector<std::pair<int, std::string>>{
                  {1, "one"}, {2, "two"}, {3, "three"}}]() {
    return vector | std::views::filter([](auto const&) { return true; });
  }).when([](auto&& vector) {
      return vector | fct::ranges::to<std::map>();
    }).then([](auto const&& result) {
    auto const expected =
        std::map<int, std::string>{{1, "one"}, {2, "two"}, {3, "three"}};
    EXPECT_EQ(expected, result);
  });
}

TEST(TestTo, TestToSharedPtrMap) {
  GWT::given([vector =
                  std::vector<std::pair<int, std::string>>{
                      {1, "one"}, {2, "two"}, {3, "three"}}]() {
    return vector | std::views::filter([](auto const&) { return true; });
  })
      .when([](auto&& vector) {
        return vector | fct::ranges::to<fct::shared_ptr::map>();
      })
      .then([](auto const&& result) {
        auto const expected =
            std::map<int, std::string>{{1, "one"}, {2, "two"}, {3, "three"}};
        EXPECT_EQ(expected, *result);
      });
}

TEST(TestTo, TestToVectorJoinEqualsConcat) {
  auto const first = std::views::iota(1, 4);
  auto const second = std::views::iota(4, 7);
  auto const third = std::views::iota(7, 10);

  GWT::given(ranges::views::concat(first, second, third))
      .when([](auto&& view) { return view | fct::ranges::to<std::vector>(); })
      .then([&first, &second, &third](auto const&& result) {
        auto const expected = std::vector{first, second, third} |
                              std::views::join | fct::ranges::to<std::vector>();
        EXPECT_EQ(expected, result);
      });
}

TEST(TestTo, TestToSet) {
  GWT::given([data = std::views::iota(1, 11)]() {
    return std::views::repeat(data, 5) | std::views::join |
           std::views::filter(
               [](auto const& value) { return value % 2 == 0; }) |
           std::views::transform(
               [](auto const& value) { return value * value; }) |
           std::views::take(20);
  }).when([](auto&& view) {
      return view | fct::ranges::to<std::set>();
    }).then([](auto const&& result) {
    auto const expected = std::set{4, 16, 36, 64, 100};
    EXPECT_EQ(expected, result);
  });
}

TEST(TestTo, TestToVectorFromSet) {
  GWT::given([data = std::set{1, 2, 3, 4, 5}]() {
    return data;
  }).when([](auto&& set) {
      return set | fct::ranges::to<std::vector>();
    }).then([](auto const&& result) {
    auto const expected = std::vector{1, 2, 3, 4, 5};
    EXPECT_EQ(expected, result);
  });
}

TEST(TestTo, TestToString) {
  GWT::given([]() {
    return std::views::iota(0, 10) |
           std::views::transform(
               [](auto const& value) { return std::to_string(value); }) |
           std::views::join;
  }).when([](auto&& view) {
      return view | fct::ranges::to<std::string>();
    }).then([](auto const&& result) {
    auto const expected = "0123456789"s;
    EXPECT_EQ(expected, result);
  });
}

TEST(TestTo, TestToVectorWithTakeWhile) {
  GWT::given(std::views::iota(1, 20) | std::views::transform([](auto i) {
               return (i % 5) ? std::optional{i} : std::nullopt;
             }) |
             std::views::take_while(
                 [](auto const& optional) { return optional.has_value(); }) |
             std::views::transform(
                 [](auto const& optional) { return optional.value(); }))
      .when([](auto const&& view) {
        return view | fct::ranges::to<std::vector>();
      })
      .then([](auto const&& result) {
        auto const expected = std::vector{1, 2, 3, 4};
        EXPECT_EQ(expected, result);
      });
}
