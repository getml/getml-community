#ifndef RELBOOST_ENSEMBLE_TABLEHOLDER_HPP_
#define RELBOOST_ENSEMBLE_TABLEHOLDER_HPP_

namespace relboost
{
namespace ensemble
{
// ----------------------------------------------------------------------------

struct TableHolder
{
    TableHolder(
        const Placeholder& _placeholder,
        const containers::DataFrameView& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::vector<std::string>& _peripheral_names )
        : main_tables_(
              TableHolder::parse_main_tables( _placeholder, _population ) ),
          peripheral_tables_( TableHolder::parse_peripheral_tables(
              _placeholder, _peripheral, _peripheral_names ) ),
          subtables_( TableHolder::parse_subtables(
              _placeholder, _peripheral, _peripheral_names ) )

    {
        assert_true( main_tables_.size() == peripheral_tables_.size() );
        assert_true( main_tables_.size() == subtables_.size() );
    }

    ~TableHolder() = default;

    // ------------------------------

    /// Creates the main tables during construction.
    static std::vector<containers::DataFrameView> parse_main_tables(
        const Placeholder& _placeholder,
        const containers::DataFrameView& _population );

    /// Creates the peripheral tables during construction.
    static std::vector<containers::DataFrame> parse_peripheral_tables(
        const Placeholder& _placeholder,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::vector<std::string>& _peripheral_names );

    /// Creates the subtables during construction.
    static std::vector<containers::Optional<TableHolder>> parse_subtables(
        const Placeholder& _placeholder,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::vector<std::string>& _peripheral_names );

    // ------------------------------

    /// The TableHolder has a population table, which may or may not be
    /// identical with the actual population table.
    const std::vector<containers::DataFrameView> main_tables_;

    /// The TableHolder can have peripheral tables.
    const std::vector<containers::DataFrame> peripheral_tables_;

    /// The TableHolder may or may not have subtables.
    const std::vector<containers::Optional<TableHolder>> subtables_;
};

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relboost

#endif  // RELBOOST_ENSEMBLE_TABLEHOLDER_HPP_
