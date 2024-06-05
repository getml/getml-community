// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_PREPROCESSOR_HPP_
#define COMMANDS_PREPROCESSOR_HPP_

#include <cstddef>
#include <optional>
#include <string>

#include "commands/NotSupportedInCommunity.hpp"
#include <rfl/Field.hpp>
#include <rfl/Literal.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/TaggedUnion.hpp>

namespace commands {

struct Preprocessor {
  /// The command needed to produce a CategoryTrimmer.
  struct CategoryTrimmerOp {
    using Tag = rfl::Literal<"CategoryTrimmer">;
    rfl::Field<"max_num_categories_", size_t> max_num_categories;
    rfl::Field<"min_freq_", size_t> min_freq;
  };

  /// The command needed to produce an EmailDomain preprocessor.
  struct EMailDomainOp {
    using Tag = rfl::Literal<"EMailDomain">;
    rfl::Field<"dummy_", std::optional<int>> dummy;
  };

  /// The command needed to produce an Imputation preprocessor.
  struct ImputationOp {
    using Tag = rfl::Literal<"Imputation">;
    rfl::Field<"add_dummies_", bool> add_dummies;
  };

  /// The command needed to produce the mapping preprocessor, which is not
  /// supported.
  using MappingOp = NotSupportedInCommunity<"Mapping">;

  /// The command needed to produce a Seasonal preprocessor.
  struct SeasonalOp {
    using Tag = rfl::Literal<"Seasonal">;
    rfl::Field<"disable_hour_", std::optional<bool>> disable_hour;
    rfl::Field<"disable_minute_", std::optional<bool>> disable_minute;
    rfl::Field<"disable_month_", std::optional<bool>> disable_month;
    rfl::Field<"disable_weekday_", std::optional<bool>> disable_weekday;
    rfl::Field<"disable_year_", std::optional<bool>> disable_year;
  };

  /// The command needed to produce a Substring preprocessor.
  struct SubstringOp {
    using Tag = rfl::Literal<"Substring">;
    rfl::Field<"begin_", size_t> begin;
    rfl::Field<"length_", size_t> length;
    rfl::Field<"unit_", std::string> unit;
  };

  /// The command needed to produce a TextFieldSplitter preprocessor.
  struct TextFieldSplitterOp {
    using Tag = rfl::Literal<"TextFieldSplitter">;
    rfl::Field<"dummy_", std::optional<int>> dummy;
  };

  using ReflectionType =
      rfl::TaggedUnion<"type_", CategoryTrimmerOp, EMailDomainOp, ImputationOp,
                       MappingOp, SeasonalOp, SubstringOp, TextFieldSplitterOp>;

  ReflectionType val_;
};

}  // namespace commands

#endif  // COMMANDS_DATA_FRAME_OR_VIEW_HPP_
