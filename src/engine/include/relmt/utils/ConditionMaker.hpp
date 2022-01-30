#ifndef RELMT_UTILS_CONDITIONMAKER_HPP_
#define RELMT_UTILS_CONDITIONMAKER_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "helpers/helpers.hpp"
#include "strings/strings.hpp"
#include "transpilation/transpilation.hpp"

// ----------------------------------------------------------------------------

#include "relmt/Float.hpp"
#include "relmt/Int.hpp"
#include "relmt/containers/containers.hpp"

// ----------------------------------------------------------------------------

#include "relmt/utils/StandardScaler.hpp"

// ----------------------------------------------------------------------------

namespace relmt {
namespace utils {

class ConditionMaker {
 private:
  typedef std::vector<std::shared_ptr<const std::vector<strings::String>>>
      VocabForDf;

 public:
  ConditionMaker(const Float _lag, const size_t _peripheral_used,
                 const std::shared_ptr<const StandardScaler>& _input_scaler,
                 const std::shared_ptr<const StandardScaler>& _output_scaler,
                 const std::shared_ptr<const std::vector<bool>>& _is_ts)
      : input_scaler_(_input_scaler),
        is_ts_(_is_ts),
        lag_(_lag),
        output_scaler_(_output_scaler),
        peripheral_used_(_peripheral_used) {}

  ~ConditionMaker() = default;

  /// Generates a condition for when the column is greater than the critical
  /// value.
  std::string condition_greater(
      const helpers::StringIterator& _categories,
      const VocabForDf& _vocab_popul, const VocabForDf& _vocab_perip,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator,
      const std::string& _feature_prefix, const helpers::Schema& _input,
      const helpers::Schema& _output, const containers::Split& _split) const;

  /// Generates a condition for when the column is smaller than the critical
  /// value.
  std::string condition_smaller(
      const helpers::StringIterator& _categories,
      const VocabForDf& _vocab_popul, const VocabForDf& _vocab_perip,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator,
      const std::string& _feature_prefix, const helpers::Schema& _input,
      const helpers::Schema& _output, const containers::Split& _split) const;

  /// Generates the equation at the end of the condition.
  std::string make_equation(
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator,
      const std::string& _feature_prefix, const helpers::Schema& _input,
      const helpers::Schema& _output, const std::vector<Float>& _weights) const;

 private:
  /// Returns a list of the categories.
  std::string list_categories(const helpers::StringIterator& _categories,
                              const containers::Split& _split) const;

  /// Returns a list of the words.
  std::string list_words(
      const std::vector<strings::String>& _vocabulary,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator,
      const containers::Split& _split, const std::string& _name,
      const bool _is_greater) const;

  /// Generates the column name to insert into the conditions.
  std::string make_colname(
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator,
      const std::string& _colname, const std::string& _alias) const;

  /// Generates parts of the equation.
  std::string make_equation_part(
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator,
      const std::string& _raw_name, const std::string& _alias,
      const Float _weight, const Float _mean, const bool _is_ts) const;

  /// Transforms the time stamps diff into SQLite-compliant code,
  /// when the colnames are already known.
  std::string make_time_stamp_diff(const std::string& _colname1,
                                   const std::string& _colname2,
                                   const bool _is_greater) const;

  /// Transforms the time stamps windows into SQLite-compliant code.
  std::string make_time_stamp_window(
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator,
      const helpers::Schema& _input, const helpers::Schema& _output,
      const Float _diff, const bool _is_greater) const;

  /// The weights were trained on the rescaled columns. This rescales the
  /// weights such that they are appropriate for the original columns.
  std::vector<Float> rescale(const std::vector<Float>& _weights) const;

 private:
  /// Trivial (const) accessor
  const utils::StandardScaler& input_scaler() const {
    assert_true(input_scaler_);
    return *input_scaler_;
  }

  /// Returns the timediff string for time comparisons
  std::string make_diffstr(const Float _timediff,
                           const std::string _timeunit) const {
    return (_timediff >= 0.0)
               ? "'+" + std::to_string(_timediff) + " " + _timeunit + "'"
               : "'" + std::to_string(_timediff) + " " + _timeunit + "'";
  }

  /// Trivial (const) accessor
  const utils::StandardScaler& output_scaler() const {
    assert_true(output_scaler_);
    return *output_scaler_;
  }

 private:
  /// The scaler used for the output table.
  const std::shared_ptr<const StandardScaler> input_scaler_;

  /// Signifies whether column are a time stamp.
  const std::shared_ptr<const std::vector<bool>> is_ts_;

  /// The lag variable used for the moving time window.
  const Float lag_;

  /// The scaler used for the output table.
  const std::shared_ptr<const StandardScaler> output_scaler_;

  /// The number of the peripheral table used.
  const size_t peripheral_used_;
};

}  // namespace utils
}  // namespace relmt

// ----------------------------------------------------------------------------

#endif  // RELMT_UTILS_CONDITIONMAKER_HPP_
