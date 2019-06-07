#include "autosql/utils/utils.hpp"

namespace autosql
{
namespace utils
{
// ----------------------------------------------------------------------------

std::shared_ptr<const std::vector<AUTOSQL_INT>>
SubtreeHelper::create_population_indices(
    const size_t _nrows, const AUTOSQL_SAMPLE_CONTAINER& _sample_container )
{
    std::set<AUTOSQL_INT> population_indices;

    for ( auto& sample : _sample_container )
        {
            assert( sample->ix_x_perip >= 0 );

            assert( sample->ix_x_perip < _nrows );

            population_indices.insert( sample->ix_x_perip );
        }

    return std::make_shared<const std::vector<AUTOSQL_INT>>(
        population_indices.begin(), population_indices.end() );
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::map<AUTOSQL_INT, AUTOSQL_INT>>
SubtreeHelper::create_output_map( const std::vector<size_t>& _rows )
{
    auto output_map = std::make_shared<std::map<AUTOSQL_INT, AUTOSQL_INT>>();

    for ( size_t i = 0; i < _rows.size(); ++i )
        {
            ( *output_map )[static_cast<AUTOSQL_INT>( _rows[i] )] =
                static_cast<AUTOSQL_INT>( i );
        }

    return output_map;
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace autosql