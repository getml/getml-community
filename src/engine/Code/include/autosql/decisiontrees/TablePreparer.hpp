#ifndef AUTOSQL_DECISIONTREES_TABLEPREPARER_HPP_
#define AUTOSQL_DECISIONTREES_TABLEPREPARER_HPP_

namespace autosql
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

class TablePreparer
{
    // -------------------------------------------------------------------------

   public:
    /// Turns the raw tables into tables in such a form that each join key
    /// in _population_table corresponds to exactly one _peripheral_table
    /// and the corresponding join key in the _peripheral_table can be
    /// easily identified.
    static TableHolder prepare_tables(
        const Placeholder &_placeholder_population,
        const std::vector<std::string> &_placeholder_peripheral,
        std::vector<containers::DataFrame> &_peripheral_tables_raw,
        containers::DataFrameView &_population_table_raw );

    // -------------------------------------------------------------------------

   private:
    /// Append the correct join key to the main table.
    static void append_join_key_and_index(
        const size_t _i,
        const Placeholder &_placeholder_population,
        containers::DataFrameView &_population_table_raw,
        TableHolder &_table_holder );

    /// Append the correct time stamps to the main table.
    static void append_time_stamps(
        const size_t _i,
        const Placeholder &_placeholder_population,
        containers::DataFrameView &_population_table_raw,
        TableHolder &_table_holder );

    /// Makes sure that the input is plausible.
    static void check_plausibility(
        const Placeholder &_placeholder_population,
        const std::vector<std::string> &_placeholder_peripheral,
        const std::vector<containers::DataFrame> &_peripheral_tables_raw,
        const containers::DataFrameView &_population_table_raw );

    /// Identify the correct peripheral table to use.
    static SQLNET_INT identify_peripheral(
        const size_t _i,
        const Placeholder &_placeholder_population,
        const std::vector<std::string> &_placeholder_peripheral );

    /// Prepares any subtables that might be there. Needed for the snowflake
    /// data model.
    static void prepare_children(
        const Placeholder &_placeholder_population,
        const std::vector<std::string> &_placeholder_peripheral,
        std::vector<containers::DataFrame> &_peripheral_tables_raw,
        TableHolder &_table_holder );

    /// Sets the correct join key in the peripheral table, so we know which join
    /// key to use.
    static void set_join_key_used(
        const size_t _i,
        const SQLNET_INT _dist,
        const Placeholder &_placeholder_population,
        const std::vector<containers::DataFrame> &_peripheral_tables_raw,
        TableHolder &_table_holder );

    /// Sets the correct time stamps in the peripheral table, so we know which
    /// time stamps to use.
    static void set_time_stamps_used(
        const size_t _i,
        const SQLNET_INT _dist,
        const Placeholder &_placeholder_population,
        const std::vector<containers::DataFrame> &_peripheral_tables_raw,
        TableHolder &_table_holder );

    /// Sets the correct upper time stamps in the peripheral table, if any.
    static void set_upper_time_stamps(
        const size_t _i,
        const SQLNET_INT _dist,
        const Placeholder &_placeholder_population,
        const std::vector<containers::DataFrame> &_peripheral_tables_raw,
        TableHolder &_table_holder );

    // -------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace autosql
#endif  // AUTOSQL_DECISIONTREES_TABLEPREPARER_HPP_
