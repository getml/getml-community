#include "autosql/utils/utils.hpp"

namespace autosql
{
namespace utils
{
// ----------------------------------------------------------------------------

std::shared_ptr<const std::map<AUTOSQL_INT, AUTOSQL_INT>>
Mapper::create_rows_map(
    const std::shared_ptr<const std::vector<size_t>>& _rows )
{
    auto rows_map = std::make_shared<std::map<AUTOSQL_INT, AUTOSQL_INT>>();

    auto size = static_cast<AUTOSQL_INT>( _rows->size() );

    for ( AUTOSQL_INT i = 0; i < size; ++i )
        {
            ( *rows_map )[( *_rows )[i]] = i;
        }

    return rows_map;
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace autosql
