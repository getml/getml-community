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

#define AUTOSQL_SAME_UNITS_CONTAINER \
    std::vector<                     \
        std::tuple<enums::ColumnToBeAggregated, enums::ColumnToBeAggregated>>

#define AUTOSQL_SAMPLES std::vector<Sample>

#define AUTOSQL_SAMPLE_CONTAINER std::vector<Sample*>

#define AUTOSQL_SAMPLE_ITERATOR AUTOSQL_SAMPLE_CONTAINER::iterator

#define AUTOSQL_INDEX std::unordered_map<size_t, std::vector<size_t>>

// ----------------------------------------------------------------------------

#endif  // AUTOSQL_TYPES_HPP_