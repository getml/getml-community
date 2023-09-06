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
#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/TaggedUnion.hpp"

namespace commands {

class Preprocessor {
 public:
  /// The command needed to produce a CategoryTrimmer.
  using CategoryTrimmerOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"CategoryTrimmer">>,
                      rfl::Field<"max_num_categories_", size_t>,
                      rfl::Field<"min_freq_", size_t>>;

  /// The command needed to produce an EmailDomain preprocessor.
  using EMailDomainOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"EMailDomain">>>;

  /// The command needed to produce an Imputation preprocessor.
  using ImputationOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Imputation">>,
                      rfl::Field<"add_dummies_", bool>>;

  /// The command needed to produce the mapping preprocessor, which is not
  /// supported.
  using MappingOp = NotSupportedInCommunity<"Mapping">;

  /// The command needed to produce a Seasonal preprocessor.
  using SeasonalOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Seasonal">>,
                      rfl::Field<"disable_hour_", std::optional<bool>>,
                      rfl::Field<"disable_minute_", std::optional<bool>>,
                      rfl::Field<"disable_month_", std::optional<bool>>,
                      rfl::Field<"disable_weekday_", std::optional<bool>>,
                      rfl::Field<"disable_year_", std::optional<bool>>>;

  /// The command needed to produce a Substring preprocessor.
  using SubstringOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Substring">>,
                      rfl::Field<"begin_", size_t>,
                      rfl::Field<"length_", size_t>,
                      rfl::Field<"unit_", std::string>>;

  /// The command needed to produce a TextFieldSplitter preprocessor.
  using TextFieldSplitterOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"TextFieldSplitter">>>;

  using NamedTupleType =
      rfl::TaggedUnion<"type_", CategoryTrimmerOp, EMailDomainOp, ImputationOp,
                       MappingOp, SeasonalOp, SubstringOp, TextFieldSplitterOp>;

  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_DATA_FRAME_OR_VIEW_HPP_
