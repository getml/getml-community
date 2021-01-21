#ifndef DFS_CONTAINERS_CONDITION_HPP_
#define DFS_CONTAINERS_CONDITION_HPP_

namespace dfs
{
namespace containers
{
// -------------------------------------------------------------------------

struct Condition
{
    Condition(
        const enums::DataUsed _data_used,
        const size_t _input_col,
        const size_t _output_col,
        const size_t _peripheral );

    Condition( const Poco::JSON::Object &_obj );

    ~Condition();

    /// Expresses the abstract feature as a JSON object.
    Poco::JSON::Object::Ptr to_json_obj() const;

    /// Expresses the abstract feature as SQL code.
    std::string to_sql(
        const std::string &_feature_prefix,
        const Placeholder &_input,
        const Placeholder &_output ) const;

    /// The kind of data used
    const enums::DataUsed data_used_;

    /// The colnum of the column in the input table
    const size_t input_col_;

    /// The colnum of the column in the output table
    const size_t output_col_;

    /// The number of the peripheral table used
    const size_t peripheral_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace dfs

#endif  // DFS_CONTAINERS_CONDITION_HPP_
