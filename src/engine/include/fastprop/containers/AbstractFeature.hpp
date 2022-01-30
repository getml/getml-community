#ifndef FASTPROP_CONTAINERS_ABSTRACTFEATURE_HPP_
#define FASTPROP_CONTAINERS_ABSTRACTFEATURE_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include <memory>
#include <vector>

// ----------------------------------------------------------------------------

#include "fastprop/Float.hpp"
#include "fastprop/Int.hpp"
#include "fastprop/enums/enums.hpp"
#include "strings/strings.hpp"
#include "transpilation/transpilation.hpp"

// ----------------------------------------------------------------------------

#include "fastprop/containers/Condition.hpp"

// ----------------------------------------------------------------------------

namespace fastprop {
namespace containers {

struct AbstractFeature {
  static constexpr Int NO_CATEGORICAL_VALUE = -1;

  typedef std::vector<std::shared_ptr<const std::vector<strings::String>>>
      VocabForDf;

  typedef std::vector<VocabForDf> Vocabulary;

  AbstractFeature(const enums::Aggregation _aggregation,
                  const std::vector<Condition> &_conditions,
                  const enums::DataUsed _data_used, const size_t _input_col,
                  const size_t _output_col, const size_t _peripheral);

  AbstractFeature(const enums::Aggregation _aggregation,
                  const std::vector<Condition> &_conditions,
                  const enums::DataUsed _data_used, const size_t _input_col,
                  const size_t _peripheral);

  AbstractFeature(const enums::Aggregation _aggregation,
                  const std::vector<Condition> &_conditions,
                  const size_t _input_col, const size_t _peripheral,
                  const enums::DataUsed _data_used,
                  const Int _categorical_value);

  AbstractFeature(const Poco::JSON::Object &_obj);

  ~AbstractFeature();

  /// Expresses the abstract feature as a JSON object.
  Poco::JSON::Object::Ptr to_json_obj() const;

  /// Expresses the abstract feature as SQL code.
  std::string to_sql(
      const helpers::StringIterator &_categories,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>
          &_sql_dialect_generator,
      const std::string &_feature_prefix, const std::string &_feature_num,
      const helpers::Schema &_input, const helpers::Schema &_output) const;

  /// The aggregation to apply
  const enums::Aggregation aggregation_;

  /// The value to which we compare the categorical column.
  const Int categorical_value_;

  /// The conditions applied to the aggregation.
  const std::vector<Condition> conditions_;

  /// The kind of data used
  const enums::DataUsed data_used_;

  /// The colnum of the column in the input table
  const size_t input_col_;

  /// The colnum of the column in the output table (only relevant for
  /// same_unit features).
  const size_t output_col_;

  /// The number of the peripheral table used
  const size_t peripheral_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace fastprop

#endif  // FASTPROP_CONTAINERS_ABSTRACTFEATURE_HPP_
