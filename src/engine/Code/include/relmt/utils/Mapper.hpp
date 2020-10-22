#ifndef RELMT_UTILS_MAPPER_HPP_
#define RELMT_UTILS_MAPPER_HPP_

// ----------------------------------------------------------------------------

namespace relmt
{
namespace utils
{
// ------------------------------------------------------------------------

struct Mapper
{
    /// The rows map reverses the effects of rows_ in
    /// containers::DataFrameView.
    static std::shared_ptr<const std::map<Int, Int>> create_rows_map(
        const std::shared_ptr<const std::vector<size_t>>& _rows );
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace relmt

// ----------------------------------------------------------------------------

#endif  // RELMT_UTILS_MAPPER_HPP_
