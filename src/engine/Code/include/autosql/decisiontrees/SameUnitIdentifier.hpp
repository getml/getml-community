#ifndef AUTOSQL_DECISIONTREES_SAMEUNITIDENTIFIER_HPP_
#define AUTOSQL_DECISIONTREES_SAMEUNITIDENTIFIER_HPP_

namespace autosql
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

class SameUnitIdentifier
{
    // -------------------------------------------------------------------------

   public:
    /// Identifies the same units between the peripheral tables
    /// and the population table.
    static std::vector<descriptors::SameUnits> identify_same_units(
        const std::vector<containers::DataFrame>& _peripheral_tables,
        const containers::DataFrame& _population_table );

    // -------------------------------------------------------------------------

   private:
    /// Parses the units of _data and adds them to
    /// _unit_map
    template <class MatrixType>
    static void add_to_unit_map(
        const DataUsed _data_used,
        const SQLNET_INT _ix_perip_used,
        const MatrixType _data,
        std::map<std::string, std::vector<ColumnToBeAggregated>>&
            _unit_map );

    /// Once the unit maps have been fitted, this transforms it to a vector of
    /// SQLNET_SAME_UNITS_CONTAINER objects.
    static void unit_map_to_same_unit_container(
        const std::map<
            std::string,
            std::vector<ColumnToBeAggregated>>& _unit_map,
        std::vector<SQLNET_SAME_UNITS_CONTAINER>& _same_units );

    // -------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <class MatrixType>
void SameUnitIdentifier::add_to_unit_map(
    const DataUsed _data_used,
    const SQLNET_INT _ix_perip_used,
    const MatrixType _data,
    std::map<std::string, std::vector<ColumnToBeAggregated>>&
        _unit_map )
{
    for ( SQLNET_INT col = 0; col < _data.ncols(); ++col )
        {
            const auto& unit = _data.unit( col );

            if ( unit != "" )
                {
                    auto it = _unit_map.find( unit );

                    ColumnToBeAggregated new_column = {
                        col,            // ix_column_used
                        _data_used,     // data_used
                        _ix_perip_used  // ix_perip_used
                    };

                    if ( it == _unit_map.end() )
                        {
                            _unit_map.insert(
                                std::pair<
                                    std::string,
                                    std::vector<
                                        ColumnToBeAggregated>>(
                                    unit, {new_column} ) );
                        }
                    else
                        {
                            it->second.push_back( new_column );
                        }
                }
        }
}

// ----------------------------------------------------------------------------
}
}
#endif  // AUTOSQL_DECISIONTREES_SAMEUNITIDENTIFIER_HPP_
