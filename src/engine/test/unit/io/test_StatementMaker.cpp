#include <gtest/gtest.h>

#include <tuple>

#include "gwt.h"
#include "io/StatementMaker.hpp"

using namespace std::literals::string_literals;

TEST(TestStatementMaker, TestHandleSchema) {
  GWT::given(std::make_tuple("this.is.a.table.name"s, ">"s, "<"s))
      .when([](auto const&& args) {
        auto const& [table_name, quotechar1, quotechar2] = args;
        return io::StatementMaker::handle_schema(table_name, quotechar1,
                                                 quotechar2);
      })
      .then([](auto const&& schema) {
        auto const expected = "this<.>is<.>a<.>table<.>name"s;
        EXPECT_EQ(expected, schema);
      });
}
