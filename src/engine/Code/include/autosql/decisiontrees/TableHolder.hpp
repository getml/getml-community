#ifndef AUTOSQL_DECISIONTREES_TABLEHOLDER_HPP_
#define AUTOSQL_DECISIONTREES_TABLEHOLDER_HPP_

namespace autosql
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

struct TableHolder
{
    TableHolder( const size_t _num_peripheral )
        : peripheral_tables(
              std::vector<containers::DataFrame>( _num_peripheral ) ),
          subtables(
              std::vector<containers::Optional<TableHolder>>( _num_peripheral ) )
    {
    }

    TableHolder() = default;

    ~TableHolder() = default;

    // ------------------------------

    /// The TableHolder has a population table, which may or may not be
    /// identical with the actual population table.
    containers::DataFrameView main_table;

    /// The TableHolder can have peripheral tables.
    std::vector<containers::DataFrame> peripheral_tables;

    /// The table holder may or may not have subtables.
    std::vector<containers::Optional<TableHolder>> subtables;
};

// ----------------------------------------------------------------------------
}
}
#endif  // AUTOSQL_DECISIONTREES_TABLEHOLDER_HPP_
