#ifndef HELPERS_MAPPINGCONTAINERMAKER_HPP_
#define HELPERS_MAPPINGCONTAINERMAKER_HPP_

namespace helpers
{
// -------------------------------------------------------------------------

class MappingContainerMaker
{
   public:
    typedef MappedContainer::MappedColumns MappedColumns;
    typedef MappingContainer::MappingForDf MappingForDf;

   public:
    /// Fits the mapping container.
    static std::shared_ptr<const MappingContainer> fit(
        const size_t _min_df,
        const Placeholder& _placeholder,
        const DataFrame& _population,
        const std::vector<DataFrame>& _peripheral,
        const std::vector<std::string>& _peripheral_names );

    /// Transform categorical columns by mapping them onto
    /// the corresponding weights.
    static std::optional<const MappedContainer> transform(
        const std::shared_ptr<const MappingContainer>& _mapping,
        const Placeholder& _placeholder,
        const DataFrame& _population,
        const std::vector<DataFrame>& _peripheral,
        const std::vector<std::string>& _peripheral_names );

   private:
    /// Finds the correspondings rownums to the input indices.
    static std::vector<size_t> find_output_ix(
        const std::vector<size_t>& _input_ix,
        const DataFrame& _output_table,
        const DataFrame& _input_table );

    /// Generates the mapping for a categorical column.
    static MappingForDf fit_on_categoricals(
        const size_t _min_df,
        const std::vector<DataFrame>& _main_tables,
        const std::vector<DataFrame>& _peripheral_tables );

    /// Fits a new mapping on the table holder.
    static std::shared_ptr<const MappingContainer> fit_on_table_holder(
        const size_t _min_df,
        const TableHolder& _table_holder,
        const std::vector<DataFrame>& _main_tables,
        const std::vector<DataFrame>& _peripheral_tables );

    /// Infers the number of targets on which the mapping was fitted.
    static size_t infer_num_targets( const MappingForDf& _mapping );

    /// Generates a new mapping based on the rownum_map for a particular column.
    static std::shared_ptr<const std::map<Int, std::vector<Float>>>
    make_mapping(
        const size_t _min_df,
        const std::map<Int, std::vector<size_t>>& _rownum_map,
        const std::vector<DataFrame>& _main_tables,
        const std::vector<DataFrame>& _peripheral_tables );

    /// Returns a map of all the rownums associated with a categorical value.
    static std::map<Int, std::vector<size_t>> make_rownum_map(
        const Column<Int>& _col );

    /// Maps the categories on their corresponding weights.
    static MappedColumns transform_categorical(
        const MappingForDf& _mapping,
        const std::vector<Column<Int>>& _categorical );

    /// Transforms a table holder to get the extra columns.
    static std::shared_ptr<const MappedContainer> transform_table_holder(
        const std::shared_ptr<const MappingContainer>& _mapping,
        const TableHolder& _table_holder );
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_MAPPINGCONTAINERMAKER_HPP_
