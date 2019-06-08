#ifndef AUTOSQL_DECISIONTREES_SUBTREEHELPER_HPP_
#define AUTOSQL_DECISIONTREES_SUBTREEHELPER_HPP_

// ----------------------------------------------------------------------------

namespace autosql
{
namespace decisiontrees
{
// ------------------------------------------------------------------------

class SubtreeHelper
{
   public:
    static std::shared_ptr<const std::vector<AUTOSQL_INT>>
    create_population_indices(
        const size_t _nrows,
        const AUTOSQL_SAMPLE_CONTAINER& _sample_container );


    /// Calls transform on the subtrees, thus returning the features.
    static std::vector<std::vector<AUTOSQL_FLOAT>> make_predictions(
        const containers::Optional<TableHolder>& _subtable,
        const bool _use_timestamps,
        const std::vector<DecisionTree>& _subtrees );

    /// Builds appropriate views on the features. The purpose of the ColumnView
    /// is to reverse the effect of the row indices in the DataFrameView.
    static std::vector<containers::ColumnView<
        AUTOSQL_FLOAT,
        std::map<AUTOSQL_INT, AUTOSQL_INT>>>
    make_subfeatures(
        const containers::Optional<TableHolder>& _subtable,
        const std::vector<std::vector<AUTOSQL_FLOAT>>& _predictions );

   private:
    static std::shared_ptr<const std::map<AUTOSQL_INT, AUTOSQL_INT>>
    create_output_map( const std::vector<size_t>& _rows );
};

// ------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace autosql

// ----------------------------------------------------------------------------

#endif  // AUTOSQL_DECISIONTREES_SUBTREEHELPER_HPP_