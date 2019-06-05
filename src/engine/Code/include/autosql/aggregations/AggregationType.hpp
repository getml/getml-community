#ifndef AUTOSQL_AGGREGATIONS_AGGREGATIONTYPE_HPP_
#define AUTOSQL_AGGREGATIONTYPE_HPP_

namespace autosql
{
namespace aggregations
{
// ----------------------------------------------------------------------------

namespace AggregationType
{
struct Avg
{
    static const std::string type() { return "AVG"; }
};

struct Count
{
    static const std::string type() { return "COUNT"; }
};

struct CountDistinct
{
    static const std::string type() { return "COUNT DISTINCT"; }
};

struct CountMinusCountDistinct
{
    static const std::string type() { return "COUNT MINUS COUNT DISTINCT"; }
};

struct Max
{
    static const std::string type() { return "MAX"; }
};

struct Median
{
    static const std::string type() { return "MEDIAN"; }
};

struct Min
{
    static const std::string type() { return "MIN"; }
};

struct Skewness
{
    static const std::string type() { return "SKEWNESS"; }
};

struct Stddev
{
    static const std::string type() { return "STDDEV"; }
};

struct Sum
{
    static const std::string type() { return "SUM"; }
};

struct Var
{
    static const std::string type() { return "VAR"; }
};
}  // namespace AggregationType

// ----------------------------------------------------------------------------

namespace AggregationType
{
template <typename AggType>
struct ApplicableToCategoricalData
{
    static constexpr bool value =
        std::is_same<AggType, AggregationType::CountDistinct>::value ||
        std::is_same<AggType, AggregationType::CountMinusCountDistinct>::value;
};
}  // namespace AggregationType

// ----------------------------------------------------------------------------

namespace AggregationType
{
template <DataUsed data_used_>
struct IsCategorical
{
    static constexpr bool value =
        ( data_used_ == DataUsed::same_unit_categorical ||
          data_used_ == DataUsed::x_perip_categorical ||
          data_used_ == DataUsed::x_popul_categorical );
};
}  // namespace AggregationType

// ----------------------------------------------------------------------------

namespace AggregationType
{
template <DataUsed data_used_>
struct IsComparison
{
    static constexpr bool value =
        ( data_used_ == DataUsed::time_stamps_diff ||
          data_used_ == DataUsed::same_unit_numerical ||
          data_used_ == DataUsed::same_unit_discrete );
};
}  // namespace AggregationType

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace autosql

#endif  // AUTOSQL_AGGREGATIONS_AGGREGATIONTYPE_HPP_
