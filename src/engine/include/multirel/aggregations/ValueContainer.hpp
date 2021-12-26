#ifndef MULTIREL_AGGREGATIONS_VALUECONTAINER_HPP_
#define MULTIREL_AGGREGATIONS_VALUECONTAINER_HPP_

// ----------------------------------------------------------------------------

#include "multirel/enums/enums.hpp"

// ----------------------------------------------------------------------------

#include "multirel/Float.hpp"
#include "multirel/Int.hpp"
#include "multirel/containers/containers.hpp"

// ----------------------------------------------------------------------------

#include "multirel/aggregations/AggregationType.hpp"

// ----------------------------------------------------------------------------

namespace multirel {
namespace aggregations {
// ----------------------------------------------------------------------------

/// ValueContainer contains the values to be aggregated.
template <enums::DataUsed data_used_, bool is_population_>
class ValueContainer {
 private:
  constexpr static bool categorical_column_ =
      AggregationType::IsCategorical<data_used_>::value;

  constexpr static bool comparison_with_peripheral_ =
      !AggregationType::IsCategorical<data_used_>::value &&
      AggregationType::IsComparison<data_used_>::value && !is_population_;

  constexpr static bool comparison_with_population_ =
      !AggregationType::IsCategorical<data_used_>::value &&
      AggregationType::IsComparison<data_used_>::value && is_population_;

  constexpr static bool numerical_column_ =
      !AggregationType::IsCategorical<data_used_>::value &&
      !AggregationType::IsComparison<data_used_>::value &&
      data_used_ != enums::DataUsed::not_applicable &&
      data_used_ != enums::DataUsed::x_subfeature;

  constexpr static bool subfeature_ =
      !AggregationType::IsCategorical<data_used_>::value &&
      !AggregationType::IsComparison<data_used_>::value &&
      data_used_ == enums::DataUsed::x_subfeature;

  constexpr static bool needs_categorical_column_ = categorical_column_;

  constexpr static bool needs_comparison_ =
      AggregationType::IsComparison<data_used_>::value;

  constexpr static bool needs_numerical_column_ =
      !categorical_column_ && data_used_ != enums::DataUsed::not_applicable;

  typedef typename std::conditional<
      subfeature_, containers::ColumnView<Float, std::map<Int, Int>>,
      containers::Column<Float>>::type ColumnOrSubfeature;

 public:
  typedef typename std::conditional<needs_categorical_column_,
                                    containers::Column<Int>, int>::type
      CategoricalColumn;

  typedef typename std::conditional<
      needs_comparison_, containers::ColumnView<Float, std::vector<size_t>>,
      int>::type ComparisonColumn;

  typedef typename std::conditional<needs_numerical_column_, ColumnOrSubfeature,
                                    int>::type NumericalColumn;

  typedef
      typename std::conditional<data_used_ != enums::DataUsed::not_applicable,
                                Float, void>::type ReturnType;

 public:
  ValueContainer(const CategoricalColumn& _value_to_be_aggregated_categorical,
                 const ComparisonColumn& _value_to_be_compared,
                 const NumericalColumn& _value_to_be_aggregated)
      : value_to_be_aggregated_(_value_to_be_aggregated),
        value_to_be_aggregated_categorical_(
            _value_to_be_aggregated_categorical),
        value_to_be_compared_(_value_to_be_compared){};

  ~ValueContainer() = default;

 public:
  /// Extracts the value to be aggregated.
  inline ReturnType value_to_be_aggregated(
      const containers::Match* _match) const {
    if constexpr (categorical_column_) {
      return static_cast<Float>(
          value_to_be_aggregated_categorical_[_match->ix_x_perip]);
    }

    if constexpr (comparison_with_population_) {
      return value_to_be_compared_[_match->ix_x_popul] -
             value_to_be_aggregated_[_match->ix_x_perip];
    }

    if constexpr (comparison_with_peripheral_) {
      return value_to_be_compared_.col()[_match->ix_x_perip] -
             value_to_be_aggregated_[_match->ix_x_perip];
    }

    if constexpr (numerical_column_) {
      return value_to_be_aggregated_[_match->ix_x_perip];
    }

    if constexpr (subfeature_) {
      return value_to_be_aggregated_[static_cast<Int>(_match->ix_x_perip)];
    }
  }

 private:
  /// Value to be aggregated - note the the length is usually different
  /// from yhat
  const NumericalColumn value_to_be_aggregated_;

  /// Value to be aggregated to be used for aggregations that can be
  /// categorical
  const CategoricalColumn value_to_be_aggregated_categorical_;

  /// Value to be compared - this applies when the value to be aggregated
  /// is a timestamp difference or same unit numerical
  /// Note the the length is usually different from value_to_be_aggregated_,
  /// but always equal to the length of yhat_.
  const ComparisonColumn value_to_be_compared_;
};

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace multirel

#endif  // MULTIREL_AGGREGATIONS_VALUECONTAINER_HPP_
