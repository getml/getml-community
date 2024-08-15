// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PREPROCESSORS_TEXTFIELDSPLITTER_HPP_
#define ENGINE_PREPROCESSORS_TEXTFIELDSPLITTER_HPP_

#include <memory>
#include <rfl/Field.hpp>
#include <rfl/Literal.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>
#include <utility>
#include <vector>

#include "commands/Fingerprint.hpp"
#include "commands/Preprocessor.hpp"
#include "engine/Int.hpp"
#include "engine/preprocessors/Params.hpp"
#include "engine/preprocessors/Preprocessor.hpp"
#include "helpers/ColumnDescription.hpp"
#include "helpers/StringIterator.hpp"

namespace engine {
namespace preprocessors {

class TextFieldSplitter : public Preprocessor {
 public:
  using MarkerType = typename helpers::ColumnDescription::MarkerType;

  using TextFieldSplitterOp =
      typename commands::Preprocessor::TextFieldSplitterOp;

  struct SaveLoad {
    rfl::Field<"cols_", std::vector<rfl::Ref<helpers::ColumnDescription>>> cols;
  };

  using ReflectionType = SaveLoad;

 public:
  TextFieldSplitter(const TextFieldSplitterOp&,
                    const std::vector<commands::Fingerprint>& _dependencies)
      : dependencies_(_dependencies) {}

  ~TextFieldSplitter() = default;

 public:
  /// Identifies which features should be extracted from which time stamps.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  fit_transform(const Params& _params) final;

  /// Loads the predictor
  void load(const std::string& _fname) final;

  /// Stores the preprocessor.
  void save(const std::string& _fname,
            const typename helpers::Saver::Format& _format) const final;

  /// Generates SQL code for the text field splitting.
  std::vector<std::string> to_sql(
      const helpers::StringIterator& _categories,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator) const final;

  /// Transforms the data frames by adding the desired time series
  /// transformations.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  transform(const Params& _params) const final;

 public:
  /// Creates a deep copy.
  rfl::Ref<Preprocessor> clone(
      const std::optional<std::vector<commands::Fingerprint>>& _dependencies =
          std::nullopt) const final {
    const auto c = rfl::Ref<TextFieldSplitter>::make(*this);
    if (_dependencies) {
      c->dependencies_ = *_dependencies;
    }
    return c;
  }

  /// Returns the fingerprint of the preprocessor (necessary to build
  /// the dependency graphs).
  commands::Fingerprint fingerprint() const final {
    using FingerprintType =
        typename commands::Fingerprint::TextFieldSplitterFingerprint;
    return commands::Fingerprint(FingerprintType{
        .dependencies = dependencies_,
        .op = commands::Preprocessor::TextFieldSplitterOp{std::nullopt}});
  }

  /// Necessary for the automated parsing to work.
  ReflectionType reflection() const { return ReflectionType{cols_}; }

  /// Returns the type of the preprocessor.
  std::string type() const final { return Preprocessor::TEXT_FIELD_SPLITTER; }

 private:
  /// Adds a rowid to the data frame.
  containers::DataFrame add_rowid(const containers::DataFrame& _df) const;

  /// Fits and transforms an individual data frame.
  std::vector<rfl::Ref<helpers::ColumnDescription>> fit_df(
      const containers::DataFrame& _df, const MarkerType _marker) const;

  /// Generates a new data frame.
  containers::DataFrame make_new_df(
      const std::shared_ptr<memmap::Pool> _pool, const std::string& _df_name,
      const containers::Column<strings::String>& _col) const;

  /// Removes the text fields from the data frame.
  containers::DataFrame remove_text_fields(
      const containers::DataFrame& _df) const;

  /// Splits the text fields on a particular column.
  std::pair<containers::Column<Int>, containers::Column<strings::String>>
  split_text_fields_on_col(
      const containers::Column<strings::String>& _col) const;

  /// Transforms a single data frame.
  void transform_df(const MarkerType _marker, const containers::DataFrame& _df,
                    std::vector<containers::DataFrame>* _peripheral_dfs) const;

 private:
  /// List of all columns to which the text field splitter transformation
  /// applies.
  std::vector<rfl::Ref<helpers::ColumnDescription>> cols_;

  /// The dependencies inserted into the the preprocessor.
  std::vector<commands::Fingerprint> dependencies_;
};

}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_TEXTFIELDSPLITTER_HPP_
