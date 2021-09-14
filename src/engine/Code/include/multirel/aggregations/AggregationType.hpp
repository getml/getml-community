#ifndef MULTIREL_AGGREGATIONS_AGGREGATIONTYPE_HPP_
#define MULTIREL_AGGREGATIONS_AGGREGATIONTYPE_HPP_

namespace multirel
{
namespace aggregations
{
// ----------------------------------------------------------------------------

namespace AggregationType
{
using Aggregation = helpers::enums::Parser<helpers::enums::Aggregation>;

struct Avg
{
    static const std::string type() { return Aggregation::AVG; }
};

struct Count
{
    static const std::string type() { return Aggregation::COUNT; }
};

struct CountDistinct
{
    static const std::string type() { return Aggregation::COUNT_DISTINCT; }
};

struct CountMinusCountDistinct
{
    static const std::string type()
    {
        return Aggregation::COUNT_MINUS_COUNT_DISTINCT;
    }
};

struct First
{
    static const std::string type() { return Aggregation::FIRST; }
};

struct Last
{
    static const std::string type() { return Aggregation::LAST; }
};

struct Max
{
    static const std::string type() { return Aggregation::MAX; }
};

struct Median
{
    static const std::string type() { return Aggregation::MEDIAN; }
};

struct Min
{
    static const std::string type() { return Aggregation::MIN; }
};

struct Skewness
{
    static const std::string type() { return Aggregation::SKEW; }
};

struct Stddev
{
    static const std::string type() { return Aggregation::STDDEV; }
};

struct Sum
{
    static const std::string type() { return Aggregation::SUM; }
};

struct Var
{
    static const std::string type() { return Aggregation::VAR; }
};

// ----------------------------------------------------------------------------

template <typename AggType>
struct ApplicableToCategoricalData
{
    static constexpr bool value =
        std::is_same<AggType, AggregationType::CountDistinct>::value ||
        std::is_same<AggType, AggregationType::CountMinusCountDistinct>::value;
};

// ----------------------------------------------------------------------------

template <enums::DataUsed data_used_>
struct IsCategorical
{
    static constexpr bool value =
        ( data_used_ == enums::DataUsed::same_unit_categorical ||
          data_used_ == enums::DataUsed::x_perip_categorical ||
          data_used_ == enums::DataUsed::x_popul_categorical );
};

// ----------------------------------------------------------------------------

template <enums::DataUsed data_used_>
struct IsComparison
{
    static constexpr bool value =
        ( data_used_ == enums::DataUsed::time_stamps_diff ||
          data_used_ == enums::DataUsed::same_unit_numerical ||
          data_used_ == enums::DataUsed::same_unit_numerical_ts ||
          data_used_ == enums::DataUsed::same_unit_discrete ||
          data_used_ == enums::DataUsed::same_unit_discrete_ts );
};

// ----------------------------------------------------------------------------

}  // namespace AggregationType
}  // namespace aggregations
}  // namespace multirel

#endif  // MULTIREL_AGGREGATIONS_AGGREGATIONTYPE_HPP_
