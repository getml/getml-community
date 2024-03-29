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
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/TaggedUnion.hpp"

namespace commands {

class Preprocessor {
 public:
  /// The command needed to produce a CategoryTrimmer.
  using CategoryTrimmerOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"CategoryTrimmer">>,
                      fct::Field<"max_num_categories_", size_t>,
                      fct::Field<"min_freq_", size_t>>;

  /// The command needed to produce an EmailDomain preprocessor.
  using EMailDomainOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"EMailDomain">>>;

  /// The command needed to produce an Imputation preprocessor.
  using ImputationOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Imputation">>,
                      fct::Field<"add_dummies_", bool>>;

  /// The command needed to produce the mapping preprocessor, which is not
  /// supported.
  using MappingOp = NotSupportedInCommunity<"Mapping">;

  /// The command needed to produce a Seasonal preprocessor.
  using SeasonalOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Seasonal">>,
                      fct::Field<"disable_hour_", std::optional<bool>>,
                      fct::Field<"disable_minute_", std::optional<bool>>,
                      fct::Field<"disable_month_", std::optional<bool>>,
                      fct::Field<"disable_weekday_", std::optional<bool>>,
                      fct::Field<"disable_year_", std::optional<bool>>>;

  /// The command needed to produce a Substring preprocessor.
  using SubstringOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Substring">>,
                      fct::Field<"begin_", size_t>,
                      fct::Field<"length_", size_t>,
                      fct::Field<"unit_", std::string>>;

  /// The command needed to produce a TextFieldSplitter preprocessor.
  using TextFieldSplitterOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"TextFieldSplitter">>>;

  using NamedTupleType =
      fct::TaggedUnion<"type_", CategoryTrimmerOp, EMailDomainOp, ImputationOp,
                       MappingOp, SeasonalOp, SubstringOp, TextFieldSplitterOp>;

  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_DATA_FRAME_OR_VIEW_HPP_
