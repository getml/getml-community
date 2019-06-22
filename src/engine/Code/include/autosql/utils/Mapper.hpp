#ifndef AUTOSQL_UTILS_MAPPER_HPP_
#define AUTOSQL_UTILS_MAPPER_HPP_

// ----------------------------------------------------------------------------

namespace autosql
{
namespace utils
{
// ------------------------------------------------------------------------

struct Mapper
{
    /// The rows map reverses the effects of rows_ in
    /// containers::DataFrameView.
    static std::shared_ptr<const std::map<Int, Int>>
    create_rows_map( const std::shared_ptr<const std::vector<size_t>>& _rows );
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace autosql

// ----------------------------------------------------------------------------

#endif  // AUTOSQL_UTILS_MAPPER_HPP_