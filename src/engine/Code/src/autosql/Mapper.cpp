#include "autosql/utils/utils.hpp"

namespace autosql
{
namespace utils
{
// ----------------------------------------------------------------------------

std::shared_ptr<const std::map<Int, Int>>
Mapper::create_rows_map(
    const std::shared_ptr<const std::vector<size_t>>& _rows )
{
    auto rows_map = std::make_shared<std::map<Int, Int>>();

    auto size = static_cast<Int>( _rows->size() );

    for ( Int i = 0; i < size; ++i )
        {
            ( *rows_map )[static_cast<Int>( ( *_rows )[i] )] = i;
        }

    return rows_map;
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace autosql
