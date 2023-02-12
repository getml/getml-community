// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PREPROCESSORS_TEXTFIELDSPLITTER_HPP_
#define ENGINE_PREPROCESSORS_TEXTFIELDSPLITTER_HPP_

#include <memory>
#include <utility>
#include <vector>

#include "commands/Preprocessor.hpp"
#include "engine/containers/containers.hpp"
#include "engine/preprocessors/FitParams.hpp"
#include "engine/preprocessors/Preprocessor.hpp"
#include "engine/preprocessors/TransformParams.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "helpers/helpers.hpp"
#include "strings/strings.hpp"

namespace engine {
namespace preprocessors {

class TextFieldSplitter : public Preprocessor {
 public:
  using TextFieldSplitterOp =
      typename commands::Preprocessor::TextFieldSplitterOp;

  using f_cols =
      fct::Field<"cols_",
                 std::vector<std::shared_ptr<helpers::ColumnDescription>>>;

  using NamedTupleType = fct::NamedTuple<f_cols>;

 public:
  TextFieldSplitter(const TextFieldSplitterOp& _op,
                    const std::vector<DependencyType>& _dependencies)
      : dependencies_(_dependencies) {}

  ~TextFieldSplitter() = default;

 public:
  /// Identifies which features should be extracted from which time stamps.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  fit_transform(const FitParams& _params) final;

  /// Loads the predictor
  void load(const std::string& _fname) final;

  /// Stores the preprocessor.
  void save(const std::string& _fname) const final;

  /// Generates SQL code for the text field splitting.
  std::vector<std::string> to_sql(
      const helpers::StringIterator& _categories,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator) const final;

  /// Transforms the data frames by adding the desired time series
  /// transformations.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  transform(const TransformParams& _params) const final;

 public:
  /// Creates a deep copy.
  fct::Ref<Preprocessor> clone(const std::optional<std::vector<DependencyType>>&
                                   _dependencies = std::nullopt) const final {
    const auto c = fct::Ref<TextFieldSplitter>::make(*this);
    if (_dependencies) {
      c->dependencies_ = *_dependencies;
    }
    return c;
  }

  /// Returns the fingerprint of the preprocessor (necessary to build
  /// the dependency graphs).
  commands::PreprocessorFingerprint fingerprint() const final {
    using FingerprintType = typename commands::PreprocessorFingerprint::
        TextFieldSplitterFingerprint;
    return commands::PreprocessorFingerprint(FingerprintType(
        fct::make_field<"dependencies_">(dependencies_),
        fct::make_field<"type_">(fct::Literal<"TextFieldSplitter">())));
  }

  /// Necessary for the automated parsing to work.
  NamedTupleType named_tuple() const { return NamedTupleType(f_cols(cols_)); }

  /// Returns the type of the preprocessor.
  std::string type() const final { return Preprocessor::TEXT_FIELD_SPLITTER; }

 private:
  /// Adds a rowid to the data frame.
  containers::DataFrame add_rowid(const containers::DataFrame& _df) const;

  /// Fits and transforms an individual data frame.
  std::vector<std::shared_ptr<helpers::ColumnDescription>> fit_df(
      const containers::DataFrame& _df, const std::string& _marker) const;

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
  void transform_df(const std::string& _marker,
                    const containers::DataFrame& _df,
                    std::vector<containers::DataFrame>* _peripheral_dfs) const;

 private:
  /// List of all columns to which the text field splitter transformation
  /// applies.
  std::vector<std::shared_ptr<helpers::ColumnDescription>> cols_;

  /// The dependencies inserted into the the preprocessor.
  std::vector<DependencyType> dependencies_;
};

}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_TEXTFIELDSPLITTER_HPP_

