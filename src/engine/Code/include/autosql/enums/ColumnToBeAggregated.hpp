#ifndef AUTOSQL_ENUMS_COLUMNTOBEAGGREGATED_HPP_
#define AUTOSQL_ENUMS_COLUMNTOBEAGGREGATED_HPP_

// ----------------------------------------------------------------------------

namespace autosql
{
namespace enums
{
// ----------------------------------------------------------------------------

struct ColumnToBeAggregated
{
    size_t ix_column_used;

    DataUsed data_used;

    AUTOSQL_INT ix_perip_used;
};

// ----------------------------------------------------------------------------
}  // namespace enums
}  // namespace autosql

#endif  // AUTOSQL_ENUMS_COLUMNTOBEAGGREGATED_HPP_
