#include <gtest/gtest.h>

#include <ranges>
#include <tuple>

#include "fct/Range.hpp"
#include "gtest/gtest.h"
#include "gwt.h"
#include "strings/String.hpp"
#include "textmining/Vocabulary.hpp"

using namespace std::literals::string_literals;

namespace strings {
// Help print correct information when a test fails.
auto PrintTo(String const& string, std::ostream* os) -> void {
  *os << string.str();
}
}  // namespace strings

class TestVocabulary
    : public ::testing::TestWithParam<
          std::tuple<std::size_t, std::size_t, std::vector<strings::String>,
                     std::vector<strings::String>>> {};

auto toStringsString(std::string const& string) -> strings::String {
  return strings::String(string);
};

inline auto make_parameter(std::size_t const min_df, std::size_t const max_df,
                           std::vector<std::string> const& given,
                           std::vector<std::string> const& expected) {
  return std::make_tuple(min_df, max_df,
                         given | std::views::transform(&toStringsString) |
                             std::ranges::to<std::vector>(),
                         expected | std::views::transform(toStringsString) |
                             std::ranges::to<std::vector>());
}

TEST_P(TestVocabulary, TestGenerate) {
  auto const& parameter = GetParam();
  GWT::given([&parameter]() {
    auto const& [min_df, max_size, data, _ignored] = parameter;
    return std::make_tuple(
        min_df, max_size,
        fct::Range{std::begin(data), std::end(std::get<2>(parameter))});
  })
      .when([](auto const&& args) {
        auto const& [min_df, max_size, range] = args;
        return textmining::Vocabulary::generate(min_df, max_size, range);
      })
      .then([expected = std::get<3>(parameter)](auto const&& result) {
        EXPECT_EQ(expected, *result);
      });
}

INSTANTIATE_TEST_SUITE_P(
    Parametrized, TestVocabulary,
    testing::Values(make_parameter(0uz, 10uz, {"1"s, "2"s, "3"s, "4"s},
                                   {"1"s, "2"s, "3"s, "4"s}),
                    make_parameter(0uz, 10uz, {"4"s, "3"s, "2"s, "1"s},
                                   {"1"s, "2"s, "3"s, "4"s}),
                    make_parameter(0uz, 10uz, {"4"s, "4"s, "4"s, "4"s}, {"4"s}),
                    make_parameter(0uz, 10uz,
                                   {"1"s, "2"s, "3"s, "4"s, "2"s, "3"s, "4"s,
                                    "3"s, "4"s, "4"s},
                                   {"1"s, "2"s, "3"s, "4"s}),
                    make_parameter(2uz, 2uz,
                                   {"1"s, "2"s, "3"s, "4"s, "2"s, "3"s, "4"s,
                                    "3"s, "4"s, "4"s},
                                   {"3"s, "4"s}),
                    make_parameter(3uz, 2uz,
                                   {"1"s, "2"s, "3"s, "4"s, "1"s, "2"s, "3"s,
                                    "1"s, "2"s, "1"s},
                                   {"1"s, "2"s})));
