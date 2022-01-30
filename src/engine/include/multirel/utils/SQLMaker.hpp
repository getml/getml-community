#ifndef MULTIREL_UTILS_SQLMAKER_HPP_
#define MULTIREL_UTILS_SQLMAKER_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <vector>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"
#include "strings/strings.hpp"
#include "transpilation/transpilation.hpp"

// ----------------------------------------------------------------------------

#include "multirel/descriptors/descriptors.hpp"

// ----------------------------------------------------------------------------

namespace multirel {
namespace utils {

class SQLMaker {
 private:
  typedef std::vector<std::shared_ptr<const std::vector<strings::String>>>
      VocabForDf;

 public:
  SQLMaker(const Float _lag, const size_t _peripheral_used,
           const descriptors::SameUnits& _same_units,
           const std::shared_ptr<const transpilation::SQLDialectGenerator>&
               _sql_dialect_generator)
      : lag_(_lag),
        peripheral_used_(_peripheral_used),
        same_units_(_same_units),
        sql_dialect_generator_(_sql_dialect_generator) {
    assert_true(sql_dialect_generator_);
  }

  ~SQLMaker() = default;

  /// Creates a condition that must be greater than a critical value.
  std::string condition_greater(const helpers::StringIterator& _categories,
                                const VocabForDf& _vocab_popul,
                                const VocabForDf& _vocab_perip,
                                const std::string& _feature_prefix,
                                const helpers::Schema& _input,
                                const helpers::Schema& _output,
                                const descriptors::Split& _split,
                                const bool _add_null) const;

  /// Creates a condition that must be smaller than a critical value.
  std::string condition_smaller(const helpers::StringIterator& _categories,
                                const VocabForDf& _vocab_popul,
                                const VocabForDf& _vocab_perip,
                                const std::string& _feature_prefix,
                                const helpers::Schema& _input,
                                const helpers::Schema& _output,
                                const descriptors::Split& _split,
                                const bool _add_null) const;

  /// Creates a select statement (SELECT AGGREGATION(VALUE TO TO BE
  /// AGGREGATED)).
  std::string select_statement(const std::string& _feature_prefix,
                               const helpers::Schema& _input,
                               const helpers::Schema& _output,
                               const size_t _column_used,
                               const enums::DataUsed& _data_used,
                               const std::string& _agg_type) const;

  /// Creates the value to be aggregated (for instance a column name or the
  /// difference between two columns)
  std::string value_to_be_aggregated(const std::string& _feature_prefix,
                                     const helpers::Schema& _input,
                                     const helpers::Schema& _output,
                                     const size_t _column_used,
                                     const enums::DataUsed& _data_used) const;

 private:
  /// Returns the column name signified by _column_used and _data_used.
  std::string get_name(const std::string& _feature_prefix,
                       const helpers::Schema& _input,
                       const helpers::Schema& _output,
                       const size_t _column_used,
                       const enums::DataUsed& _data_used) const;

  /// Extracts the proper name from a same units struct.
  std::pair<std::string, std::string> get_names(
      const std::string& _feature_prefix, const helpers::Schema& _input,
      const helpers::Schema& _output,
      const std::shared_ptr<const descriptors::SameUnitsContainer> _same_units,
      const size_t _column_used) const;

  /// Returns the column name signified by _column_used and _data_used as a
  /// time stamp
  std::string get_ts_name(const helpers::Schema& _input,
                          const helpers::Schema& _output,
                          const size_t _column_used,
                          const enums::DataUsed& _data_used,
                          const std::string& _diffstr) const;

  /// Extracts the proper name from a same units struct.
  std::pair<std::string, std::string> get_ts_names(
      const helpers::Schema& _input, const helpers::Schema& _output,
      const std::shared_ptr<const descriptors::SameUnitsContainer> _same_units,
      const size_t _column_used) const;

  /// Returns a list of the categories.
  std::string list_categories(const helpers::StringIterator& _categories,
                              const descriptors::Split& _split) const;

  /// Returns a list of the words.
  std::string list_words(const std::vector<strings::String>& _vocabulary,
                         const descriptors::Split& _split,
                         const std::string& _name,
                         const bool _is_greater) const;

  /// Generates the colname to display in the SQL code.
  std::string make_staging_table_colname(const std::string& _colname,
                                         const std::string& _alias) const;

  /// Transforms the time stamps diff into SQLite-compliant given the
  /// colnames.
  std::string make_time_stamp_diff(const std::string& _colname1,
                                   const std::string& _colname2,
                                   const bool _is_greater) const;

  /// Makes a window
  std::string make_time_stamp_window(const helpers::Schema& _input,
                                     const helpers::Schema& _output,
                                     const Float _diff,
                                     const bool _is_greater) const;

 private:
  /// Returns the timediff string for time comparisons
  std::string make_diffstr(const Float _timediff,
                           const std::string _timeunit) const {
    return (_timediff >= 0.0)
               ? "'+" + std::to_string(_timediff) + " " + _timeunit + "'"
               : "'" + std::to_string(_timediff) + " " + _timeunit + "'";
  }

 private:
  /// The lag variable used for the moving time window.
  const Float lag_;

  /// The number of the peripheral table used.
  const size_t peripheral_used_;

  /// Contains information on the same units
  const descriptors::SameUnits same_units_;

  /// Generates the right code for the required SQL dialect.
  const std::shared_ptr<const transpilation::SQLDialectGenerator>
      sql_dialect_generator_;
};

}  // namespace utils
}  // namespace multirel

// ----------------------------------------------------------------------------

#endif  // MULTIREL_UTILS_SQLMAKER_HPP_
