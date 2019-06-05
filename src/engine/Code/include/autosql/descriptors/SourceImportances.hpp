#ifndef AUTOSQL_DESCRIPTORS_SOURCEIMPORTANCES_HPP_
#define AUTOSQL_DESCRIPTORS_SOURCEIMPORTANCES_HPP_

namespace autosql
{
namespace descriptors
{
// ----------------------------------------------------------------------------

/// Features importances can be seen as an equivalent to feature importances
/// in ensemble learners.
struct SourceImportances
{
    // ------------------------------------------------------

    /// Importances for aggregations
    std::map<SourceImportancesColumn, AUTOSQL_FLOAT> aggregation_imp_;

    /// Importances for conditions
    std::map<SourceImportancesColumn, AUTOSQL_FLOAT> condition_imp_;

    // ------------------------------------------------------------------------

    /// Transforms the SourceImportances into a JSON object.
    Poco::JSON::Object to_json_obj() const;

    // ------------------------------------------------------------------------

    /// Transforms the SourceImportances into a JSON string
    std::string to_json() const { return JSON::stringify( to_json_obj() ); }

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}
}
#endif  // AUTOSQL_DESCRIPTORS_SOURCEIMPORTANCES_HPP_
