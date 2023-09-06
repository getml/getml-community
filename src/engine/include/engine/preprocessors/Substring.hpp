// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PREPROCESSORS_SUBSTRING_HPP_
#define ENGINE_PREPROCESSORS_SUBSTRING_HPP_

#include <memory>
#include <utility>
#include <vector>

#include "commands/Fingerprint.hpp"
#include "commands/Preprocessor.hpp"
#include "containers/containers.hpp"
#include "engine/preprocessors/Params.hpp"
#include "engine/preprocessors/Preprocessor.hpp"
#include "engine/preprocessors/PreprocessorImpl.hpp"
#include "helpers/ColumnDescription.hpp"
#include "helpers/Macros.hpp"
#include "helpers/StringIterator.hpp"
#include "helpers/Subrole.hpp"
#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"
#include "strings/strings.hpp"

namespace engine {
namespace preprocessors {

class Substring : public Preprocessor {
 public:
  using MarkerType = typename helpers::ColumnDescription::MarkerType;

  using SubstringOp = typename commands::Preprocessor::SubstringOp;

  using f_cols =
      rfl::Field<"cols_", std::vector<rfl::Ref<helpers::ColumnDescription>>>;

  using NamedTupleType = rfl::NamedTuple<f_cols>;

 public:
  Substring(const SubstringOp& _op,
            const std::vector<commands::Fingerprint>& _dependencies)
      : begin_(_op.get<"begin_">()),
        dependencies_(_dependencies),
        length_(_op.get<"length_">()),
        unit_(_op.get<"unit_">()) {}

  ~Substring() = default;

 public:
  /// Identifies which features should be extracted from which time stamps.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  fit_transform(const Params& _params) final;

  /// Loads the predictor
  void load(const std::string& _fname) final;

  /// Stores the preprocessor.
  void save(const std::string& _fname,
            const typename helpers::Saver::Format& _format) const final;

  /// Transforms the data frames by adding the desired time series
  /// transformations.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  transform(const Params& _params) const final;

 public:
  /// Creates a deep copy.
  rfl::Ref<Preprocessor> clone(
      const std::optional<std::vector<commands::Fingerprint>>& _dependencies =
          std::nullopt) const final {
    const auto c = rfl::Ref<Substring>::make(*this);
    if (_dependencies) {
      c->dependencies_ = *_dependencies;
    }
    return c;
  }

  /// Returns the fingerprint of the preprocessor (necessary to build
  /// the dependency graphs).
  commands::Fingerprint fingerprint() const final {
    using FingerprintType =
        typename commands::Fingerprint::SubstringFingerprint;
    return commands::Fingerprint(FingerprintType(
        rfl::make_field<"dependencies_">(dependencies_),
        rfl::make_field<"type_">(rfl::Literal<"Substring">()),
        rfl::make_field<"begin_">(begin_), rfl::make_field<"length_">(length_),
        rfl::make_field<"unit_">(unit_)));
  }

  /// Necessary for the automated parsing to work.
  NamedTupleType named_tuple() const { return NamedTupleType(f_cols(cols_)); }

  /// The preprocessor does not generate any SQL scripts.
  std::vector<std::string> to_sql(
      const helpers::StringIterator& _categories,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator) const final {
    return {};
  }

  /// Returns the type of the preprocessor.
  std::string type() const final { return Preprocessor::SUBSTRING; }

 private:
  /// Extracts the substring during fitting.
  std::optional<containers::Column<Int>> extract_substring(
      const containers::Column<strings::String>& _col,
      containers::Encoding* _categories) const;

  /// Extracts the substring during transformation.
  containers::Column<Int> extract_substring(
      const containers::Encoding& _categories,
      const containers::Column<strings::String>& _col) const;

  /// Extracts the substring as a string.
  containers::Column<strings::String> extract_substring_string(
      const containers::Column<strings::String>& _col) const;

  /// Fits and transforms an individual data frame.
  containers::DataFrame fit_transform_df(const containers::DataFrame& _df,
                                         const MarkerType _marker,
                                         const size_t _table,
                                         containers::Encoding* _categories);

  /// Generates a string column from the _categories and the int column.
  containers::Column<strings::String> make_str_col(
      const containers::Encoding& _categories,
      const containers::Column<Int>& _col) const;

  /// Transforms a single data frame.
  containers::DataFrame transform_df(const containers::Encoding& _categories,
                                     const containers::DataFrame& _df,
                                     const MarkerType _marker,
                                     const size_t _table) const;

 private:
  /// Extracts the columns and adds it to the data frame
  template <class T>
  void extract_and_add(const MarkerType _marker, const size_t _table,
                       const containers::Column<T>& _original_col,
                       containers::Encoding* _categories,
                       containers::DataFrame* _df) {
    const auto whitelist = std::vector<helpers::Subrole>(
        {helpers::Subrole::substring, helpers::Subrole::substring_only});

    const auto blacklist =
        std::vector<helpers::Subrole>({helpers::Subrole::exclude_preprocessors,
                                       helpers::Subrole::email_only});

    if (!helpers::SubroleParser::contains_any(_original_col.subroles(),
                                              whitelist)) {
      return;
    }

    if (helpers::SubroleParser::contains_any(_original_col.subroles(),
                                             blacklist)) {
      return;
    }

    if (unit_ != "" && _original_col.unit() != unit_) {
      return;
    }

    const auto col = extract_substring(_original_col, _categories);

    if (col) {
      PreprocessorImpl::add(_marker, _table, _original_col.name(), &cols_);
      _df->add_int_column(*col, containers::DataFrame::ROLE_CATEGORICAL);
    }
  }

  /// Extracts the substring during fitting.
  std::optional<containers::Column<Int>> extract_substring(
      const containers::Column<Int>& _col,
      containers::Encoding* _categories) const {
    const auto str_col = make_str_col(*_categories, _col);
    return extract_substring(str_col, _categories);
  }

  /// Extracts the substring during transformation.
  containers::Column<Int> extract_substring(
      const containers::Encoding& _categories,
      const containers::Column<Int>& _col) const {
    const auto str_col = make_str_col(_categories, _col);
    return extract_substring(_categories, str_col);
  }

  /// Generates the colname for the newly created column.
  std::string make_name(const std::string& _colname) const {
    return helpers::Macros::substring() + _colname + helpers::Macros::begin() +
           std::to_string(begin_ + 1) + helpers::Macros::length() +
           std::to_string(length_) + helpers::Macros::close_bracket();
  }

  /// Generates the unit for the newly created column.
  std::string make_unit(const std::string& _unit) const {
    return _unit + ", " + std::to_string(begin_) + ", " +
           std::to_string(length_);
  }

 private:
  /// The beginning of the substring to extract.
  size_t begin_;

  /// List of all columns to which the email domain transformation applies.
  std::vector<rfl::Ref<helpers::ColumnDescription>> cols_;

  /// The dependencies inserted into the the preprocessor.
  std::vector<commands::Fingerprint> dependencies_;

  /// The length of the substring to extract.
  size_t length_;

  /// The unit to which we want to apply the substring preprocessor
  std::string unit_;
};

}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_SUBSTRING_HPP_

