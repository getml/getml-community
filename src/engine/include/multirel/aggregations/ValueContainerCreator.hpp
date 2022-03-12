#ifndef MULTIREL_AGGREGATIONS_VALUECONTAINERCREATOR_HPP_
#define MULTIREL_AGGREGATIONS_VALUECONTAINERCREATOR_HPP_

// ----------------------------------------------------------------------------

#include <type_traits>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"
#include "fct/fct.hpp"
#include "helpers/helpers.hpp"

// ----------------------------------------------------------------------------

#include "multirel/enums/enums.hpp"

// ----------------------------------------------------------------------------

#include "multirel/aggregations/AbstractTransformAggregation.hpp"
#include "multirel/aggregations/AggregationType.hpp"
#include "multirel/aggregations/TransformAggregationParams.hpp"
#include "multirel/aggregations/ValueContainer.hpp"

// ----------------------------------------------------------------------------
namespace multirel {
namespace aggregations {
// ----------------------------------------------------------------------------

template <enums::DataUsed data_used_, bool is_population_>
class ValueContainerCreator {
 private:
  typedef ValueContainer<data_used_, is_population_> ValueContainerType;

  typedef typename ValueContainerType::CategoricalColumn CategoricalColumn;

  typedef typename ValueContainerType::ComparisonColumn ComparisonColumn;

  typedef typename ValueContainerType::NumericalColumn NumericalColumn;

  typedef typename std::conditional<
      data_used_ == enums::DataUsed::same_unit_discrete ||
          data_used_ == enums::DataUsed::same_unit_discrete_ts,
      ValueContainerType, void>::type SameUnitDiscreteType;

  typedef typename std::conditional<
      data_used_ == enums::DataUsed::same_unit_numerical ||
          data_used_ == enums::DataUsed::same_unit_numerical_ts,
      ValueContainerType, void>::type SameUnitNumericalType;

 public:
  /// An aggregation contains a value to be aggregated. This is set by
  /// this function.
  static ValueContainerType create(
      const descriptors::SameUnitsContainer &_same_units_discrete,
      const descriptors::SameUnitsContainer &_same_units_numerical,
      const descriptors::ColumnToBeAggregated &_column_to_be_aggregated,
      const containers::DataFrameView &_population,
      const containers::DataFrame &_peripheral,
      const containers::Subfeatures &_subfeatures);

 private:
  /// Create the value to be aggregated for same_unit_discrete.
  static SameUnitDiscreteType create_same_unit_discrete(
      const descriptors::SameUnitsContainer &_same_units_discrete,
      const size_t _ix_column_used,
      const containers::DataFrameView &_population,
      const containers::DataFrame &_peripheral);

  /// Create the value to be aggregated for same_unit_numerical.
  static SameUnitNumericalType create_same_unit_numerical(
      const descriptors::SameUnitsContainer &_same_units_numerical,
      const size_t _ix_column_used,
      const containers::DataFrameView &_population,
      const containers::DataFrame &_peripheral);
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <enums::DataUsed data_used_, bool is_population_>
ValueContainer<data_used_, is_population_>
ValueContainerCreator<data_used_, is_population_>::create(
    const descriptors::SameUnitsContainer &_same_units_discrete,
    const descriptors::SameUnitsContainer &_same_units_numerical,
    const descriptors::ColumnToBeAggregated &_column_to_be_aggregated,
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const containers::Subfeatures &_subfeatures) {
  const auto ix_column_used = _column_to_be_aggregated.ix_column_used;

  if constexpr (data_used_ == enums::DataUsed::x_perip_numerical) {
    const auto col = NumericalColumn(_peripheral.numerical_col(ix_column_used));

    return ValueContainerType(0, 0, col);
  }

  if constexpr (data_used_ == enums::DataUsed::x_perip_discrete) {
    const auto col = NumericalColumn(_peripheral.discrete_col(ix_column_used));

    return ValueContainerType(0, 0, col);
  }

  if constexpr (data_used_ == enums::DataUsed::time_stamps_diff) {
    const auto numerical = NumericalColumn(_peripheral.time_stamp_col());

    const auto comparison = ComparisonColumn(_population.time_stamp_col());

    return ValueContainerType(0, comparison, numerical);
  }

  if constexpr (data_used_ == enums::DataUsed::same_unit_numerical ||
                data_used_ == enums::DataUsed::same_unit_numerical_ts) {
    static_assert(std::is_same<ValueContainerType, SameUnitNumericalType>(),
                  "Does not return!");

    return create_same_unit_numerical(_same_units_numerical, ix_column_used,
                                      _population, _peripheral);
  }

  if constexpr (data_used_ == enums::DataUsed::same_unit_discrete ||
                data_used_ == enums::DataUsed::same_unit_discrete_ts) {
    static_assert(std::is_same<ValueContainerType, SameUnitDiscreteType>(),
                  "Does not return!");

    return create_same_unit_discrete(_same_units_discrete, ix_column_used,
                                     _population, _peripheral);
  }

  if constexpr (data_used_ == enums::DataUsed::x_perip_categorical) {
    const auto col =
        CategoricalColumn(_peripheral.categorical_col(ix_column_used));

    return ValueContainerType(col, 0, 0);
  }

  if constexpr (data_used_ == enums::DataUsed::x_subfeature) {
    const auto col = NumericalColumn(_subfeatures.at(ix_column_used));

    return ValueContainerType(0, 0, col);
  }

  if constexpr (data_used_ == enums::DataUsed::not_applicable) {
    return ValueContainerType(0, 0, 0);
  }
}

// ----------------------------------------------------------------------------

template <enums::DataUsed data_used_, bool is_population_>
typename ValueContainerCreator<data_used_, is_population_>::SameUnitDiscreteType
ValueContainerCreator<data_used_, is_population_>::create_same_unit_discrete(
    const descriptors::SameUnitsContainer &_same_units_discrete,
    const size_t _ix_column_used, const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral) {
  if constexpr (std::is_same<ValueContainerType, SameUnitDiscreteType>()) {
    assert_true(static_cast<Int>(_same_units_discrete.size()) >
                _ix_column_used);

#ifndef NDEBUG
    const auto data_used1 =
        std::get<0>(_same_units_discrete.at(_ix_column_used)).data_used;
#endif  // NDEBUG

    const auto data_used2 =
        std::get<1>(_same_units_discrete.at(_ix_column_used)).data_used;

    const auto ix_column_used1 =
        std::get<0>(_same_units_discrete.at(_ix_column_used)).ix_column_used;

    const auto ix_column_used2 =
        std::get<1>(_same_units_discrete.at(_ix_column_used)).ix_column_used;

    assert_true(data_used1 == enums::DataUsed::x_perip_discrete);

    assert_true(_peripheral.num_discretes() > ix_column_used1);

    const auto numerical = _peripheral.discrete_col(ix_column_used1);

    if (data_used2 == enums::DataUsed::x_popul_discrete) {
      assert_true(_population.num_discretes() > ix_column_used2);

      const auto comparison = _population.discrete_col(ix_column_used2);

      return ValueContainerType(0, comparison, numerical);
    }

    assert_true(data_used2 == enums::DataUsed::x_perip_discrete);

    assert_true(_peripheral.num_discretes() > ix_column_used2);

    const auto comparison = _peripheral.discrete_col(ix_column_used2);

    return ValueContainerType(0, comparison, numerical);
  }
}

// ----------------------------------------------------------------------------

template <enums::DataUsed data_used_, bool is_population_>
typename ValueContainerCreator<data_used_,
                               is_population_>::SameUnitNumericalType
ValueContainerCreator<data_used_, is_population_>::create_same_unit_numerical(
    const descriptors::SameUnitsContainer &_same_units_numerical,
    const size_t _ix_column_used, const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral) {
  if constexpr (std::is_same<ValueContainerType, SameUnitNumericalType>()) {
    assert_true(static_cast<Int>(_same_units_numerical.size()) >
                _ix_column_used);

#ifndef NDEBUG
    const auto data_used1 =
        std::get<0>(_same_units_numerical.at(_ix_column_used)).data_used;
#endif  // NDEBUG

    const auto data_used2 =
        std::get<1>(_same_units_numerical.at(_ix_column_used)).data_used;

    const auto ix_column_used1 =
        std::get<0>(_same_units_numerical.at(_ix_column_used)).ix_column_used;

    const auto ix_column_used2 =
        std::get<1>(_same_units_numerical.at(_ix_column_used)).ix_column_used;

    assert_true(data_used1 == enums::DataUsed::x_perip_numerical);

    assert_true(_peripheral.num_numericals() > ix_column_used1);

    const auto numerical = _peripheral.numerical_col(ix_column_used1);

    if (data_used2 == enums::DataUsed::x_popul_numerical) {
      assert_true(_population.num_numericals() > ix_column_used2);

      const auto comparison = _population.numerical_col(ix_column_used2);

      return ValueContainerType(0, comparison, numerical);
    }

    assert_true(data_used2 == enums::DataUsed::x_perip_numerical);

    assert_true(_peripheral.num_numericals() > ix_column_used2);

    const auto comparison = _peripheral.numerical_col(ix_column_used2);

    return ValueContainerType(0, comparison, numerical);
  }
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace multirel

#endif  // MULTIREL_AGGREGATIONS_VALUECONTAINERCREATOR_HPP_

