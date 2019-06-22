#ifndef AUTOSQL_TYPES_HPP_
#define AUTOSQL_TYPES_HPP_

// ----------------------------------------------------------------------------
// Dependencies

#include <cstdint>

#include <unordered_map>
#include <vector>

// ----------------------------------------------------------------------------

#define AUTOSQL_INT std::int32_t
#define AUTOSQL_FLOAT double
#define AUTOSQL_UNSIGNED_LONG std::uint_least64_t

#define AUTOSQL_MODEL_MAP \
    std::map<std::string, std::shared_ptr<decisiontrees::DecisionTreeEnsemble>>

#define AUTOSQL_SAME_UNITS_CONTAINER       \
    std::vector<std::tuple<                \
        descriptors::ColumnToBeAggregated, \
        descriptors::ColumnToBeAggregated>>

// ----------------------------------------------------------------------------

#endif  // AUTOSQL_TYPES_HPP_