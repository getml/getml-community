#ifndef RELBOOST_UTILS_CONDITIONMAKER_HPP_
#define RELBOOST_UTILS_CONDITIONMAKER_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "helpers/helpers.hpp"
#include "strings/strings.hpp"

// ----------------------------------------------------------------------------

#include "relboost/Float.hpp"
#include "relboost/Int.hpp"
#include "relboost/containers/containers.hpp"

// ----------------------------------------------------------------------------

namespace relboost {
namespace utils {
// ------------------------------------------------------------------------

class ConditionMaker {
 private:
  typedef std::vector<std::shared_ptr<const std::vector<strings::String>>>
      VocabForDf;

 public:
  ConditionMaker(const Float _lag, const size_t _peripheral_used)
      : lag_(_lag), peripheral_used_(_peripheral_used) {}

  ~ConditionMaker() = default;

  /// Identifies matches between population table and peripheral tables.
  std::string condition_greater(
      const helpers::StringIterator& _categories,
      const VocabForDf& _vocab_popul, const VocabForDf& _vocab_perip,
      const std::shared_ptr<const helpers::SQLDialectGenerator>&
          _sql_dialect_generator,
      const std::string& _feature_prefix, const helpers::Schema& _input,
      const helpers::Schema& _output, const containers::Split& _split) const;

  std::string condition_smaller(
      const helpers::StringIterator& _categories,
      const VocabForDf& _vocab_popul, const VocabForDf& _vocab_perip,
      const std::shared_ptr<const helpers::SQLDialectGenerator>&
          _sql_dialect_generator,
      const std::string& _feature_prefix, const helpers::Schema& _input,
      const helpers::Schema& _output, const containers::Split& _split) const;

 private:
  /// Returns a list of the categories.
  std::string list_categories(const helpers::StringIterator& _categories,
                              const containers::Split& _split) const;

  /// Returns a list of the words.
  std::string list_words(
      const std::vector<strings::String>& _vocabulary,
      const std::shared_ptr<const helpers::SQLDialectGenerator>&
          _sql_dialect_generator,
      const containers::Split& _split, const std::string& _name,
      const bool _is_greater) const;

  /// Generates the column name to insert into the conditions.
  std::string make_colname(
      const std::shared_ptr<const helpers::SQLDialectGenerator>&
          _sql_dialect_generator,
      const std::string& _colname, const std::string& _alias) const;

  /// Transforms the time stamps diff into SQLite-compliant code,
  /// when the colnames are already known.
  std::string make_time_stamp_diff(const std::string& _colname1,
                                   const std::string& _colname2,
                                   const bool _is_greater) const;

  /// Transforms the time stamps windows into SQLite-compliant code.
  std::string make_time_stamp_window(
      const std::shared_ptr<const helpers::SQLDialectGenerator>&
          _sql_dialect_generator,
      const helpers::Schema& _input, const helpers::Schema& _output,
      const Float _diff, const bool _is_greater) const;

 private:
  /// The lag variable used for the moving time window.
  const Float lag_;

  /// The number of the peripheral table used.
  const size_t peripheral_used_;
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_UTILS_CONDITIONMAKER_HPP_
