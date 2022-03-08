#ifndef MULTIREL_AGGREGATIONS_AGGREGATIONPARSER_HPP_
#define MULTIREL_AGGREGATIONS_AGGREGATIONPARSER_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <stdexcept>
#include <type_traits>

// ----------------------------------------------------------------------------

#include "multirel/Float.hpp"
#include "multirel/Int.hpp"

// ----------------------------------------------------------------------------

#include "multirel/aggregations/AbstractFitAggregation.hpp"
#include "multirel/aggregations/AbstractTransformAggregation.hpp"
#include "multirel/aggregations/FitAggregation.hpp"
#include "multirel/aggregations/TransformAggregation.hpp"

// ----------------------------------------------------------------------------
namespace multirel {
namespace aggregations {
// ----------------------------------------------------------------------------

template <typename BaseAggregationType, typename ParamsType>
class AggregationParser {
 public:
  /// Returns the appropriate aggregation from the aggregation string and
  /// other information.
  static std::shared_ptr<BaseAggregationType> parse_aggregation(
      const ParamsType& _params);

 private:
  /// Creates the aggregation.
  template <class AggType, enums::DataUsed data_used_, bool is_population_>
  static std::shared_ptr<BaseAggregationType> make_shared_ptr(
      const ParamsType& _params) {
    static_assert(
        std::is_same<BaseAggregationType, AbstractFitAggregation>() ||
            std::is_same<BaseAggregationType, AbstractTransformAggregation>(),
        "Unsupported AbstractAggregation");

    if constexpr (std::is_same<BaseAggregationType, AbstractFitAggregation>()) {
      return std::make_shared<
          FitAggregation<AggType, data_used_, is_population_>>(_params);
    }

    if constexpr (std::is_same<BaseAggregationType,
                               AbstractTransformAggregation>()) {
      return std::make_shared<
          TransformAggregation<AggType, data_used_, is_population_>>(_params);
    }
  };

  /// Actually creates the aggregation based on the AggType and other
  /// information.
  template <typename AggType>
  static std::shared_ptr<BaseAggregationType> make_aggregation(
      const ParamsType& _params) {
    switch (_params.column_to_be_aggregated_.data_used) {
      case enums::DataUsed::x_perip_numerical:

        return make_shared_ptr<AggType, enums::DataUsed::x_perip_numerical,
                               false>(_params);

      case enums::DataUsed::x_perip_discrete:

        return make_shared_ptr<AggType, enums::DataUsed::x_perip_discrete,
                               false>(_params);

      case enums::DataUsed::time_stamps_diff:

        return make_shared_ptr<AggType, enums::DataUsed::time_stamps_diff,
                               true>(_params);

      case enums::DataUsed::same_unit_numerical:
      case enums::DataUsed::same_unit_numerical_ts: {
        const auto ix_column_used =
            _params.column_to_be_aggregated_.ix_column_used;

        assert_true(ix_column_used < _params.same_units_numerical_.size());

        const auto data_used2 =
            std::get<1>(_params.same_units_numerical_.at(ix_column_used))
                .data_used;

        if (data_used2 == enums::DataUsed::x_popul_numerical) {
          return make_shared_ptr<AggType, enums::DataUsed::same_unit_numerical,
                                 true>(_params);
        }

        if (data_used2 == enums::DataUsed::x_perip_numerical) {
          return make_shared_ptr<AggType, enums::DataUsed::same_unit_numerical,
                                 false>(_params);
        }

        assert_true(!"Unknown data_used2 in make_aggregation(...)!");

        return std::shared_ptr<BaseAggregationType>();
      }

      case enums::DataUsed::same_unit_discrete:
      case enums::DataUsed::same_unit_discrete_ts: {
        const auto ix_column_used =
            _params.column_to_be_aggregated_.ix_column_used;

        assert_true(ix_column_used < _params.same_units_discrete_.size());

        const enums::DataUsed data_used2 =
            std::get<1>(_params.same_units_discrete_.at(ix_column_used))
                .data_used;

        if (data_used2 == enums::DataUsed::x_popul_discrete) {
          return make_shared_ptr<AggType, enums::DataUsed::same_unit_discrete,
                                 true>(_params);
        }

        if (data_used2 == enums::DataUsed::x_perip_discrete) {
          return make_shared_ptr<AggType, enums::DataUsed::same_unit_discrete,
                                 false>(_params);
        }

        assert_true(!"Unknown data_used2 in make_aggregation(...)!");

        return std::shared_ptr<BaseAggregationType>();
      }

      case enums::DataUsed::x_perip_categorical:

        return make_shared_ptr<AggType, enums::DataUsed::x_perip_categorical,
                               false>(_params);

      case enums::DataUsed::x_subfeature:

        return make_shared_ptr<AggType, enums::DataUsed::x_subfeature, false>(
            _params);

      case enums::DataUsed::not_applicable:

        assert_true(!"Only COUNT does not return!");

        return std::shared_ptr<BaseAggregationType>();

      default:

        assert_true(!"Unknown enums::DataUsed in make_aggregation(...)!");

        return std::shared_ptr<BaseAggregationType>();
    }
  }
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <typename BaseAggregationType, typename ParamsType>
std::shared_ptr<BaseAggregationType>
AggregationParser<BaseAggregationType, ParamsType>::parse_aggregation(
    const ParamsType& _params) {
  if (_params.aggregation_type_ == AggregationType::Avg::type()) {
    return make_aggregation<AggregationType::Avg>(_params);
  }

  if (_params.aggregation_type_ == AggregationType::Count::type()) {
    assert_true(_params.column_to_be_aggregated_.data_used ==
                enums::DataUsed::not_applicable);

    return make_shared_ptr<AggregationType::Count,
                           enums::DataUsed::not_applicable, true>(_params);
  }

  if (_params.aggregation_type_ == AggregationType::CountDistinct::type()) {
    return make_aggregation<AggregationType::CountDistinct>(_params);
  }

  if (_params.aggregation_type_ ==
      AggregationType::CountMinusCountDistinct::type()) {
    return make_aggregation<AggregationType::CountMinusCountDistinct>(_params);
  }

  if (_params.aggregation_type_ == AggregationType::First::type()) {
    return make_aggregation<AggregationType::First>(_params);
  }

  if (_params.aggregation_type_ == AggregationType::Last::type()) {
    return make_aggregation<AggregationType::Last>(_params);
  }

  if (_params.aggregation_type_ == AggregationType::Max::type()) {
    return make_aggregation<AggregationType::Max>(_params);
  }

  if (_params.aggregation_type_ == AggregationType::Median::type()) {
    return make_aggregation<AggregationType::Median>(_params);
  }

  if (_params.aggregation_type_ == AggregationType::Min::type()) {
    return make_aggregation<AggregationType::Min>(_params);
  }

  if (_params.aggregation_type_ == AggregationType::Skewness::type()) {
    return make_aggregation<AggregationType::Skewness>(_params);
  }

  if (_params.aggregation_type_ == AggregationType::Stddev::type()) {
    return make_aggregation<AggregationType::Stddev>(_params);
  }

  if (_params.aggregation_type_ == AggregationType::Sum::type()) {
    return make_aggregation<AggregationType::Sum>(_params);
  }

  if (_params.aggregation_type_ == AggregationType::Var::type()) {
    return make_aggregation<AggregationType::Var>(_params);
  }

  const auto warning_message =
      "Aggregation of type '" + _params.aggregation_type_ + "' not known!";

  throw std::runtime_error(warning_message);
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace multirel

#endif  // MULTIREL_AGGREGATIONS_AGGREGATIONPARSER_HPP_
