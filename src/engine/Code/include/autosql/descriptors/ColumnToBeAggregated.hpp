#ifndef AUTOSQL_DESCRIPTORS_COLUMNTOBEAGGREGATED_HPP_
#define AUTOSQL_DESCRIPTORS_COLUMNTOBEAGGREGATED_HPP_

// ----------------------------------------------------------------------------

namespace autosql
{
namespace descriptors
{
// ----------------------------------------------------------------------------

struct ColumnToBeAggregated
{
    size_t ix_column_used;

    enums::DataUsed data_used;

    AUTOSQL_INT ix_perip_used;
};

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace autosql

#endif  // AUTOSQL_DESCRIPTORS_COLUMNTOBEAGGREGATED_HPP_
