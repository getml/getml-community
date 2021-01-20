#ifndef DFS_CONTAINERS_ABSTRACTFEATURE_HPP_
#define DFS_CONTAINERS_ABSTRACTFEATURE_HPP_

namespace dfs
{
namespace containers
{
// -------------------------------------------------------------------------

struct AbstractFeature
{
    AbstractFeature(
        const enums::Aggregation _aggregation,
        const enums::DataUsed _data_used,
        const size_t _input_col,
        const size_t _output_col,
        const size_t _peripheral );

    AbstractFeature(
        const enums::Aggregation _aggregation,
        const enums::DataUsed _data_used,
        const size_t _input_col,
        const size_t _peripheral );

    AbstractFeature( const Poco::JSON::Object &_obj );

    ~AbstractFeature();

    /// Expresses the abstract feature as a JSON object.
    Poco::JSON::Object::Ptr to_json_obj() const;

    /// Expresses the abstract feature as SQL code.
    std::string to_sql(
        const std::vector<strings::String> &_categories,
        const std::string &_feature_prefix,
        const std::string &_feature_num,
        const Placeholder &_input,
        const Placeholder &_output ) const;

    /// The aggregation to apply
    const enums::Aggregation aggregation_;

    /// The kind of data used
    const enums::DataUsed data_used_;

    /// The colnum of the column in the input table
    const size_t input_col_;

    /// The name of the column in the input table
    const std::string input_colname_;

    /// The colnum of the column in the output table (only relevant for
    /// same_unit features).
    const size_t output_col_;

    /// The name of the column in the output table
    const std::string output_colname_;

    /// The number of the peripheral table used
    const size_t peripheral_;

    /// The name of the  table
    const std::string table_name_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace dfs

#endif  // DFS_CONTAINERS_ABSTRACTFEATURE_HPP_
