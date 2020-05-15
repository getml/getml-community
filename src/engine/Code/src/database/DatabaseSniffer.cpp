#include "database/database.hpp"

namespace database
{
// ----------------------------------------------------------------------------

std::string DatabaseSniffer::sniff(
    const std::shared_ptr<const Connector>& _conn,
    const std::string& _dialect,
    const Poco::JSON::Object& _description,
    const std::string& _source_table_name,
    const std::string& _target_table_name )
{
    assert_true( _conn );

    const auto colnames = _conn->get_colnames( _source_table_name );

    const auto coltypes = _conn->get_coltypes( _source_table_name, colnames );

    assert_true( colnames.size() == coltypes.size() );

    return io::StatementMaker::make_statement(
        _target_table_name, _dialect, _description, colnames, coltypes );
}

// ----------------------------------------------------------------------------
}  // namespace database
