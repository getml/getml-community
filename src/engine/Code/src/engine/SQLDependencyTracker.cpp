#include "engine/utils/utils.hpp"

namespace engine
{
namespace utils
{
// ------------------------------------------------------------------------

Poco::JSON::Object::Ptr SQLDependencyTracker::find_dependencies(
    const Tuples& _tuples, const size_t _i ) const
{
    const auto& sql_code = std::get<2>( _tuples.at( _i ) );

    const auto is_dependency = [&sql_code,
                                &_tuples]( const size_t _j ) -> bool {
        const auto& table_name = std::get<0>( _tuples.at( _j ) );
        return sql_code.find( "\"" + table_name + "\"" ) != std::string::npos;
    };

    const auto iota = stl::iota<size_t>( 0, _i );

    const auto dependencies =
        stl::collect::array( iota | std::views::filter( is_dependency ) );

    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    obj->set( "table_name_", std::get<0>( _tuples.at( _i ) ) );

    obj->set( "file_name_", std::get<1>( _tuples.at( _i ) ) );

    obj->set( "dependencies_", dependencies );

    return obj;
}

// ------------------------------------------------------------------------

std::string SQLDependencyTracker::infer_table_name(
    const std::string& _sql ) const
{
    const std::string drop_table = "DROP TABLE IF EXISTS \"";

    const auto pos = _sql.find( drop_table );

    if ( pos == std::string::npos )
        {
            throw std::runtime_error(
                "Could not find beginning of DROP TABLE statement." );
        }

    const auto begin = pos + drop_table.size();

    const auto end = _sql.find( "\";", begin );

    if ( end == std::string::npos )
        {
            throw std::runtime_error(
                "Could not find end of DROP TABLE statement." );
        }

    return helpers::SQLGenerator::to_lower( _sql.substr( begin, end - begin ) );
}

// ------------------------------------------------------------------------

void SQLDependencyTracker::save_dependencies( const std::string& _sql ) const
{
    const auto tuples = save_sql( _sql );

    const auto to_obj =
        [this, &tuples]( const size_t _i ) -> Poco::JSON::Object::Ptr {
        return find_dependencies( tuples, _i );
    };

    const auto iota = stl::iota<size_t>( 0, tuples.size() );

    const auto dependencies =
        stl::collect::array( iota | std::views::transform( to_obj ) );

    auto obj = Poco::JSON::Object();

    obj.set( "dependencies_", dependencies );

    const auto json_str = jsonutils::JSON::stringify( obj );

    write_to_file( "dependencies.json", json_str );
}

// ------------------------------------------------------------------------

typename SQLDependencyTracker::Tuples SQLDependencyTracker::save_sql(
    const std::string& _sql ) const
{
    const auto sql = helpers::StringSplitter::split( _sql, "\n\n\n" );

    const auto get_table_name =
        [this]( const std::string& _sql ) -> std::string {
        return infer_table_name( _sql );
    };

    const auto table_names = stl::collect::vector<std::string>(
        sql | std::views::transform( get_table_name ) );

    const auto to_file_name = [this]( const size_t _i ) -> std::string {
        return std::to_string( _i ) + ".sql";
    };

    const auto iota = stl::iota<size_t>( 0, table_names.size() );

    const auto file_names = stl::collect::vector<std::string>(
        iota | std::views::transform( to_file_name ) );

    for ( size_t i = 0; i < file_names.size(); ++i )
        {
            write_to_file( file_names.at( i ), sql.at( i ) );
        }

    const auto make_tuple =
        [&table_names, &file_names, &sql]( const size_t _i ) {
            return std::make_tuple(
                table_names.at( _i ),
                file_names.at( _i ),
                helpers::SQLGenerator::to_lower( sql.at( _i ) ) );
        };

    return stl::collect::vector<Tuples::value_type>(
        iota | std::views::transform( make_tuple ) );
}

// ------------------------------------------------------------------------

void SQLDependencyTracker::write_to_file(
    const std::string& _fname, const std::string& _content ) const
{
    std::filesystem::create_directories( folder_ );

    std::ofstream file;

    file.open( folder_ + _fname );
    file << _content;
    file.close();
}

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace engine
