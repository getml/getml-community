#include "descriptors/descriptors.hpp"

namespace autosql
{
namespace descriptors
{
// ----------------------------------------------------------------------------

Poco::JSON::Object SourceImportances::to_json_obj() const
{
    // ------------------------------------
    // Extract aggregation importances

    Poco::JSON::Array aggregation_importances;

    for ( auto& elem : aggregation_imp_ )
        {
            Poco::JSON::Object obj;

            obj.set( "column", elem.first.column_ );

            obj.set( "table", elem.first.table_ );

            obj.set( "value", elem.second );

            aggregation_importances.add( obj );
        }

    // ------------------------------------
    // Extract condition importances

    Poco::JSON::Array condition_importances;

    for ( auto& elem : condition_imp_ )
        {
            Poco::JSON::Object obj;

            obj.set( "column", elem.first.column_ );

            obj.set( "table", elem.first.table_ );

            obj.set( "value", elem.second );

            condition_importances.add( obj );
        }

    // ------------------------------------
    // Combine in one object

    Poco::JSON::Object obj;

    obj.set( "aggregation_importances", aggregation_importances );

    obj.set( "condition_importances", condition_importances );

    // ------------------------------------

    return obj;
}

// ----------------------------------------------------------------------------
}
}
