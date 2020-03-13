
#include "database/database.hpp"

namespace database
{
// ----------------------------------------------------------------------------

std::string DatabaseSniffer::sniff(
    const std::shared_ptr<Connector>& _conn, const std::string& _table_name )
{
    assert_true( _conn );

    const auto colnames = _conn->get_colnames( _table_name );

    const auto coltypes = _conn->get_coltypes( _table_name, colnames );

    assert_true( colnames.size() == coltypes.size() );

    return io::StatementMaker::make_statement(
        _table_name, "python", colnames, coltypes );
}

// ----------------------------------------------------------------------------
}  // namespace database
