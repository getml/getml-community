#ifndef AUTOSQL_DECISIONTREES_SUBTREEHELPER_HPP_
#define AUTOSQL_DECISIONTREES_SUBTREEHELPER_HPP_

// ----------------------------------------------------------------------------

namespace autosql
{
namespace ensemble
{
// ------------------------------------------------------------------------

class SubtreeHelper
{
   public:
    /// Calls transform on the subfeatures, returning predictions.
    static std::vector<containers::Predictions> make_predictions(
        const decisiontrees::TableHolder& _table_holder,
        const std::vector<containers::Optional<DecisionTreeEnsemble>>&
            _subfeatures_avg,
        const std::vector<containers::Optional<DecisionTreeEnsemble>>&
            _subfeatures_sum );

    /// Builds appropriate views on the features. The purpose of the ColumnView
    /// is to reverse the effect of the row indices in the DataFrameView.
    /*static containers::Subfeatures make_subfeatures(
        const containers::Optional<TableHolder>& _subtable,
        const std::vector<std::vector<AUTOSQL_FLOAT>>& _predictions );*/
};

// ------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace autosql

// ----------------------------------------------------------------------------

#endif  // AUTOSQL_DECISIONTREES_SUBTREEHELPER_HPP_