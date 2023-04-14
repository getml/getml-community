// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PREPROCESSORS_SEASONAL_HPP_
#define ENGINE_PREPROCESSORS_SEASONAL_HPP_

#include <memory>
#include <utility>
#include <vector>

#include "commands/Fingerprint.hpp"
#include "commands/Preprocessor.hpp"
#include "containers/containers.hpp"
#include "engine/Float.hpp"
#include "engine/Int.hpp"
#include "engine/preprocessors/FitParams.hpp"
#include "engine/preprocessors/Preprocessor.hpp"
#include "engine/preprocessors/TransformParams.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "helpers/ColumnDescription.hpp"
#include "helpers/StringIterator.hpp"
#include "strings/strings.hpp"

namespace engine {
namespace preprocessors {

class Seasonal : public Preprocessor {
 public:
  using SeasonalOp = typename commands::Preprocessor::SeasonalOp;

  using f_hour =
      fct::Field<"hour_",
                 std::vector<std::shared_ptr<helpers::ColumnDescription>>>;

  using f_minute =
      fct::Field<"minute_",
                 std::vector<std::shared_ptr<helpers::ColumnDescription>>>;

  using f_month =
      fct::Field<"month_",
                 std::vector<std::shared_ptr<helpers::ColumnDescription>>>;

  using f_weekday =
      fct::Field<"weekday_",
                 std::vector<std::shared_ptr<helpers::ColumnDescription>>>;

  using f_year =
      fct::Field<"year_",
                 std::vector<std::shared_ptr<helpers::ColumnDescription>>>;

  using NamedTupleType =
      fct::NamedTuple<f_hour, f_minute, f_month, f_weekday, f_year>;

 private:
  static constexpr bool ADD_ZERO = true;
  static constexpr bool DONT_ADD_ZERO = false;

 public:
  Seasonal(const SeasonalOp& _op,
           const std::vector<commands::Fingerprint>& _dependencies)
      : dependencies_(_dependencies) {}

  ~Seasonal() = default;

 public:
  /// Identifies which features should be extracted from which time stamps.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  fit_transform(const FitParams& _params) final;

  /// Loads the predictor
  void load(const std::string& _fname) final;

  /// Stores the preprocessor.
  void save(const std::string& _fname) const final;

  /// Transforms the data frames by adding the desired time series
  /// transformations.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  transform(const TransformParams& _params) const final;

 public:
  /// Creates a deep copy.
  fct::Ref<Preprocessor> clone(
      const std::optional<std::vector<commands::Fingerprint>>& _dependencies =
          std::nullopt) const final {
    const auto c = fct::Ref<Seasonal>::make(*this);
    if (_dependencies) {
      c->dependencies_ = *_dependencies;
    }
    return c;
  }

  /// Returns the fingerprint of the preprocessor (necessary to build
  /// the dependency graphs).
  commands::Fingerprint fingerprint() const final {
    using FingerprintType = typename commands::Fingerprint::SeasonalFingerprint;
    return commands::Fingerprint(
        FingerprintType(fct::make_field<"dependencies_">(dependencies_),
                        fct::make_field<"type_">(fct::Literal<"Seasonal">())));
  }

  /// Necessary for the automated parsing to work.
  NamedTupleType named_tuple() const {
    return f_hour(hour_) * f_minute(minute_) * f_month(month_) *
           f_weekday(weekday_) * f_year(year_);
  }

  /// The preprocessor does not generate any SQL scripts.
  std::vector<std::string> to_sql(
      const helpers::StringIterator& _categories,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator) const final {
    return {};
  }

  /// Returns the type of the preprocessor.
  std::string type() const final { return Preprocessor::SEASONAL; }

 private:
  /// Extracts the hour of a time stamp. Returns std::nullopt, if the column
  /// generates warning.
  std::optional<containers::Column<Int>> extract_hour(
      const containers::Column<Float>& _col,
      containers::Encoding* _categories) const;

  /// Extracts the hour of a time stamp.
  containers::Column<Int> extract_hour(
      const containers::Encoding& _categories,
      const containers::Column<Float>& _col) const;

  /// Extracts the minute of a time stamp. Returns std::nullopt, if the column
  /// generates warning.
  std::optional<containers::Column<Int>> extract_minute(
      const containers::Column<Float>& _col,
      containers::Encoding* _categories) const;

  /// Extracts the minute of a time stamp.
  containers::Column<Int> extract_minute(
      const containers::Encoding& _categories,
      const containers::Column<Float>& _col) const;

  /// Extracts the month of a time stamp. Returns std::nullopt, if the column
  /// generates warning.
  std::optional<containers::Column<Int>> extract_month(
      const containers::Column<Float>& _col,
      containers::Encoding* _categories) const;

  /// Extracts the month of a time stamp.
  containers::Column<Int> extract_month(
      const containers::Encoding& _categories,
      const containers::Column<Float>& _col) const;

  /// Extracts the weekday of a time stamp. Returns std::nullopt, if the
  /// column generates warnings.
  std::optional<containers::Column<Int>> extract_weekday(
      const containers::Column<Float>& _col,
      containers::Encoding* _categories) const;

  /// Extracts the weekday of a time stamp.
  containers::Column<Int> extract_weekday(
      const containers::Encoding& _categories,
      const containers::Column<Float>& _col) const;

  /// Extracts the year of a time stamp. Returns std::nullopt, if the column
  /// generates warning.
  std::optional<containers::Column<Float>> extract_year(
      const containers::Column<Float>& _col);

  /// Extracts the year of a time stamp.
  containers::Column<Float> extract_year(
      const containers::Column<Float>& _col) const;

  /// Fits and transforms an individual data frame.
  containers::DataFrame fit_transform_df(const containers::DataFrame& _df,
                                         const std::string& _marker,
                                         const size_t _table,
                                         containers::Encoding* _categories);

  // Transforms a float column to a categorical column.
  containers::Column<Int> to_int(const containers::Column<Float>& _col,
                                 const bool _add_zero,
                                 containers::Encoding* _categories) const;

  /// Transforms a float column to a categorical column.
  containers::Column<Int> to_int(const containers::Encoding& _categories,
                                 const bool _add_zero,
                                 const containers::Column<Float>& _col) const;

  /// Transforms a single data frame.
  containers::DataFrame transform_df(const containers::Encoding& _categories,
                                     const containers::DataFrame& _df,
                                     const std::string& _marker,
                                     const size_t _table) const;

 private:
  /// Undertakes a transformation based on the template class
  /// Operator.
  template <class Operator>
  containers::Column<Int> to_categorical(
      const containers::Column<Float>& _col, const bool _add_zero,
      const Operator& _op, containers::Encoding* _categories) const {
    auto result = containers::Column<Float>(_col.pool(), _col.nrows());
    std::transform(_col.begin(), _col.end(), result.begin(), _op);
    return to_int(result, _add_zero, _categories);
  }

  /// Undertakes a transformation based on the template class
  /// Operator.
  template <class Operator>
  containers::Column<Int> to_categorical(
      const containers::Encoding& _categories,
      const containers::Column<Float>& _col, const bool _add_zero,
      const Operator& _op) const {
    auto result = containers::Column<Float>(_col.pool(), _col.nrows());
    std::transform(_col.begin(), _col.end(), result.begin(), _op);
    return to_int(_categories, _add_zero, result);
  }

  /// Undertakes a transformation based on the template class
  /// Operator.
  template <class Operator>
  containers::Column<Float> to_numerical(const containers::Column<Float>& _col,
                                         const Operator& _op) const {
    auto result = containers::Column<Float>(_col.pool(), _col.nrows());
    std::transform(_col.begin(), _col.end(), result.begin(), _op);
    return result;
  }

 private:
  /// The dependencies inserted into the the preprocessor.
  std::vector<commands::Fingerprint> dependencies_;

  /// List of all columns to which the hour transformation applies.
  std::vector<std::shared_ptr<helpers::ColumnDescription>> hour_;

  /// List of all columns to which the minute transformation applies.
  std::vector<std::shared_ptr<helpers::ColumnDescription>> minute_;

  /// List of all columns to which the month transformation applies.
  std::vector<std::shared_ptr<helpers::ColumnDescription>> month_;

  /// List of all columns to which the weekday transformation applies.
  std::vector<std::shared_ptr<helpers::ColumnDescription>> weekday_;

  /// List of all columns to which the year transformation applies.
  std::vector<std::shared_ptr<helpers::ColumnDescription>> year_;
};

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_SEASONAL_HPP_

