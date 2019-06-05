#ifndef AUTOSQL_DESCRIPTORS_SOURCEIMPORTANCESCOLUMN_HPP_
#define AUTOSQL_DESCRIPTORS_SOURCEIMPORTANCESCOLUMN_HPP_

namespace autosql
{
namespace descriptors
{
// ----------------------------------------------------------------------------

struct SourceImportancesColumn
{
    /// Name of the column
    std::string column_;

    /// Name of the table
    std::string table_;
};

inline bool operator<(
    const SourceImportancesColumn &_elem1,
    const SourceImportancesColumn &_elem2 )
{
    if ( _elem1.table_ < _elem2.table_ )
        {
            return true;
        }
    else
        {
            return _elem1.column_ < _elem2.column_;
        }
}

// ----------------------------------------------------------------------------
}
}
#endif  // AUTOSQL_DESCRIPTORS_SOURCEIMPORTANCESCOLUMN_HPP_
