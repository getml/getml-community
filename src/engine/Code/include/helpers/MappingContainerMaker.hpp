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
        const std::vector<std::string>& _peripheral_names,
        const WordIndexContainer& _word_indices );

    /// Transform categorical columns by mapping them onto
    /// the corresponding weights.
    static std::optional<const MappedContainer> transform(
        const std::shared_ptr<const MappingContainer>& _mapping,
        const Placeholder& _placeholder,
        const DataFrame& _population,
        const std::vector<DataFrame>& _peripheral,
        const std::vector<std::string>& _peripheral_names,
        const WordIndexContainer& _word_indices );

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

    /// Generates the mapping for a text column.
    static MappingForDf fit_on_text(
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
    static std::map<Int, std::vector<size_t>> make_rownum_map_categorical(
        const Column<Int>& _col );

    /// Returns a map of all the rownums associated with a word.
    static std::map<Int, std::vector<size_t>> make_rownum_map_text(
        const textmining::WordIndex& _word_index );

    /// Maps the categories on their corresponding weights.
    static MappedColumns transform_categorical(
        const MappingForDf& _mapping,
        const std::vector<Column<Int>>& _categorical );

    /// Applies the mapping to a categorical column.
    static Column<Float> transform_categorical_column(
        const MappingForDf& _mapping,
        const std::vector<Column<Int>>& _categorical,
        const size_t _num_targets,
        const size_t _colnum,
        const size_t _target_num );

    /// Transforms a table holder to get the extra columns.
    static std::shared_ptr<const MappedContainer> transform_table_holder(
        const std::shared_ptr<const MappingContainer>& _mapping,
        const TableHolder& _table_holder );

    /// Maps the text fields on their corresponding weights.
    static MappedColumns transform_text(
        const MappingForDf& _mapping,
        const std::vector<Column<strings::String>>& _text,
        const typename DataFrame::WordIndices& _word_indices );

    /// Applies the mapping to a text column.
    static Column<Float> transform_text_column(
        const MappingForDf& _mapping,
        const std::vector<Column<strings::String>>& _text,
        const typename DataFrame::WordIndices& _word_indices,
        const size_t _num_targets,
        const size_t _colnum,
        const size_t _target_num );
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_MAPPINGCONTAINERMAKER_HPP_
