#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <optional>

#include "containers/Column.hpp"
#include "containers/ColumnView.hpp"
#include "containers/Float.hpp"
#include "containers/Int.hpp"
#include "fct/to.hpp"
#include "gwt.h"

using namespace std::literals::string_literals;

TEST(TestColumnView, TestToVectorExpectedLength) {
  GWT::given([]() {
    auto const ptr_to_vector =
        std::vector{1.0, 2.0, 3.0, 4.0} | fct::ranges::to_shared_ptr_vector();
    auto const column = containers::Column<containers::Float>{ptr_to_vector};
    return containers::ColumnView<containers::Float>::from_column(column);
  })
      .when([](auto&& column_view) {
        return column_view.to_vector(0uz, std::get<0>(column_view.nrows()),
                                     true);
      })
      .then([](auto&& ptr_to_vector) {
        auto const expected = std::vector{1.0, 2.0, 3.0, 4.0};
        EXPECT_EQ(expected, *ptr_to_vector);
      });
}

TEST(TestColumnView, TestToVectorExpectedLengthUnknown) {
  GWT::given([]() {
    auto const ptr_to_vector =
        std::vector{1.0, 2.0, 3.0, 4.0} | fct::ranges::to_shared_ptr_vector();
    auto const column = containers::Column<containers::Float>{ptr_to_vector};
    return containers::ColumnView<containers::Float>::from_column(column);
  })
      .when([](auto&& column_view) {
        return column_view.to_vector(0uz, std::nullopt, false);
      })
      .then([](auto&& ptr_to_vector) {
        auto const expected = std::vector{1.0, 2.0, 3.0, 4.0};
        EXPECT_EQ(expected, *ptr_to_vector);
      });
}

TEST(TestColumnView, TestFromUnOpToVector) {
  GWT::given([]() {
    auto const ptr_to_vector =
        std::vector{0, 0, 0, 0} | fct::ranges::to_shared_ptr_vector();
    auto const column = containers::Column<containers::Int>{ptr_to_vector};
    auto const operand =
        containers::ColumnView<containers::Int>::from_column(column);
    auto const identiy_operator = [](auto const& value) { return value; };
    return containers::ColumnView<containers::Int>::from_un_op(
        operand, identiy_operator);
  })
      .when([](auto const&& column_view) {
        return column_view.to_vector(0uz, std::get<0>(column_view.nrows()),
                                     true);
      })
      .then([](auto const&& result) {
        auto const expected = std::vector{0, 0, 0, 0};
        EXPECT_EQ(expected, *result);
      });
}

TEST(TestColumnView, TestFromValueToVectorThrows) {
  GWT::given(containers::ColumnView<bool>::from_value(false))
      .when([]<typename ColumnView>(ColumnView&& column_view) {
        return [column_view = std::forward<ColumnView>(column_view)]() {
          return column_view.to_vector(0uz, std::nullopt, false);
        };
      })
      .then([](auto const&& to_vector) {
        EXPECT_THAT(
            to_vector,
            testing::ThrowsMessage<std::runtime_error>(
                "The length of the column view is infinite. You can look at it, but it cannot be transformed into an actual column unless the length can be inferred from somewhere else."s));
      });
}

TEST(TestColumnView, TestFromMutableLambdaToVector) {
  GWT::given(
      containers::ColumnView<std::size_t>{
          [i = 0uz](auto const&) mutable { return i++; }, std::size_t{10}})
      .when([](auto&& column_view) {
        return column_view.to_vector(0uz, std::optional{10uz}, true);
      })
      .then([](auto&& result) {
        auto const expected =
            std::vector{0uz, 1uz, 2uz, 3uz, 4uz, 5uz, 6uz, 7uz, 8uz, 9uz};
        EXPECT_EQ(expected, *result);
      });
}
