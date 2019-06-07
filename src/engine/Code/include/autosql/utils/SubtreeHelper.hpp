#ifndef AUTOSQL_UTILS_SUBTREEHELPER_HPP_
#define AUTOSQL_UTILS_SUBTREEHELPER_HPP_

// ----------------------------------------------------------------------------

namespace autosql
{
namespace utils
{
// ------------------------------------------------------------------------

struct SubtreeHelper
{
    static std::shared_ptr<const std::vector<AUTOSQL_INT>>
    create_population_indices(
        const size_t _nrows,
        const AUTOSQL_SAMPLE_CONTAINER& _sample_container );

    static std::shared_ptr<const std::map<AUTOSQL_INT, AUTOSQL_INT>>
    create_output_map( const std::vector<size_t>& _rows );
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace autosql

// ----------------------------------------------------------------------------

#endif  // AUTOSQL_UTILS_SUBTREEHELPER_HPP_