#include "engine/containers/containers.hpp"

namespace engine
{
namespace containers
{
// ----------------------------------------------------------------------------

std::vector<size_t> DataFramePrinter::calc_max_sizes(
    const std::vector<size_t> &_max_sizes,
    const std::vector<std::string> &_row ) const
{
    auto max_sizes = _max_sizes;

    assert_true( _row.size() == max_sizes.size() );

    for ( size_t j = 0; j < _row.size(); ++j )
        {
            if ( _row.at( j ).size() > max_sizes.at( j ) )
                {
                    max_sizes.at( j ) = _row.at( j ).size();
                }
        }

    return max_sizes;
}

// ----------------------------------------------------------------------------

std::string DataFramePrinter::get_html(
    const std::vector<std::string> &_colnames,
    const std::vector<std::string> &_roles,
    const std::vector<std::string> &_units,
    const std::vector<std::vector<std::string>> &_rows,
    const std::int32_t _border ) const
{
    // ------------------------------------------------------------------------

    assert_true( _colnames.size() == _roles.size() );

    assert_true( _colnames.size() == _units.size() );

    // ------------------------------------------------------------------------

    std::string html = "<table border=\"" + std::to_string( _border ) +
                       "\" class=\"dataframe\">";

    // ------------------------------------------------------------------------

    html += "<thead>";

    html += make_html_head_line( _colnames );

    html += make_html_head_line( _roles );

    if ( !is_empty( _units ) )
        {
            html += make_html_head_line( _units );
        }

    html += "</thead>";

    // ------------------------------------------------------------------------

    html += "<tbody>";

    for ( const auto &row : _rows )
        {
            assert_true( row.size() == _colnames.size() );

            html += make_html_body_line( row );
        }

    html += "</tbody>";

    // ------------------------------------------------------------------------

    html += "</table>";

    // ------------------------------------------------------------------------

    return html;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::string DataFramePrinter::get_string(
    const std::vector<std::string> &_colnames,
    const std::vector<std::string> &_roles,
    const std::vector<std::string> &_units,
    const std::vector<std::vector<std::string>> &_rows ) const
{
    // ------------------------------------------------------------------------

    const auto colnames = truncate_row( _colnames );

    const auto roles = truncate_row( _roles );

    const auto units = truncate_row( _units );

    auto rows = _rows;

    for ( auto &row : rows )
        {
            row = truncate_row( row );
        }

    // ------------------------------------------------------------------------

    assert_true( colnames.size() == roles.size() );

    assert_true( colnames.size() == units.size() );

    // ------------------------------------------------------------------------

    auto max_sizes = std::vector<size_t>( colnames.size() );

    // ------------------------------------------------------------------------

    max_sizes = calc_max_sizes( max_sizes, colnames );

    max_sizes = calc_max_sizes( max_sizes, roles );

    max_sizes = calc_max_sizes( max_sizes, units );

    for ( const auto &row : rows )
        {
            max_sizes = calc_max_sizes( max_sizes, row );
        }

    // ------------------------------------------------------------------------

    auto result = make_row_string( max_sizes, colnames );

    result += make_row_string( max_sizes, roles );

    if ( !is_empty( units ) )
        {
            result += make_row_string( max_sizes, units );
        }

    const auto length =
        std::accumulate( max_sizes.begin(), max_sizes.end(), 0 ) +
        max_sizes.size() * 3 + 1;

    result += std::string( length, '-' );

    result += "\n";

    for ( const auto &row : rows )
        {
            result += make_row_string( max_sizes, row );
        }

    // ------------------------------------------------------------------------

    return result;

    // ------------------------------------------------------------------------
}

// ------------------------------------------------------------------------

bool DataFramePrinter::is_empty( const std::vector<std::string> &_row ) const
{
    const auto is_not_empty = []( const std::string &str ) {
        return str != "";
    };

    return std::none_of( _row.begin(), _row.end(), is_not_empty );
}

// ------------------------------------------------------------------------

std::string DataFramePrinter::make_html_head_line(
    const std::vector<std::string> &_row ) const
{
    std::string line = "<tr style=\"text-align: right;\">";

    for ( const auto &field : _row )
        {
            line += "<th>";
            line += field;
            line += "</th>";
        }

    line += "</tr>";

    return line;
}

// ------------------------------------------------------------------------

std::string DataFramePrinter::make_html_body_line(
    const std::vector<std::string> &_row ) const
{
    std::string line = "<tr>";

    for ( const auto &field : _row )
        {
            line += "<td>";
            line += field;
            line += "</td>";
        }

    line += "</tr>";

    return line;
}

// ------------------------------------------------------------------------

std::string DataFramePrinter::make_row_string(
    const std::vector<size_t> _max_sizes,
    const std::vector<std::string> &_row ) const
{
    std::string result = "| ";

    for ( size_t j = 0; j < _row.size(); ++j )
        {
            result += _row.at( j );

            assert_true( _max_sizes.at( j ) >= _row.at( j ).size() );

            const auto n_fill = _max_sizes.at( j ) - _row.at( j ).size() + 1;

            result += std::string( n_fill, ' ' );

            if ( j < _row.size() - 1 || _row.size() == ncols_ )
                {
                    result += "| ";
                }
        }

    result += "\n";

    return result;
}

// ----------------------------------------------------------------------------

std::vector<std::string> DataFramePrinter::truncate_row(
    const std::vector<std::string> &_row ) const
{
    if ( _row.size() < 9 )
        {
            return _row;
        }

    auto truncated = std::vector<std::string>( _row.begin(), _row.begin() + 8 );

    truncated.push_back( "..." );

    return truncated;
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine
