#include "database/database.hpp"

namespace database
{
// ----------------------------------------------------------------------------

Poco::JSON::Object SapHana::describe() const
{
    Poco::JSON::Object obj;

    obj.set( "default_schema", default_schema_ );

    obj.set( "dialect", dialect() );

    obj.set( "host", host_ );

    obj.set( "port", port_ );

    obj.set( "ping_interval", ping_interval_ );

    obj.set( "user", user_ );

    return obj;
}

// ----------------------------------------------------------------------------

void SapHana::execute( const std::string& _sql )
{
    const auto queries = split( _sql );

    for ( const auto& query : queries )
        {
            goutils::SapHana::exec(
                user_,
                password_,
                host_,
                port_,
                default_schema_,
                ping_interval_,
                query );
        }
}

// ----------------------------------------------------------------------------

std::vector<char*> SapHana::extract_ptrs(
    const std::vector<typename SapHana::RecordType>& _batch ) const
{
    std::vector<char*> ptrs;

    for ( size_t i = 0; i < _batch.size(); ++i )
        {
            assert_true( _batch[i] );

            for ( size_t j = 0; j < _batch[i]->size(); ++j )
                {
                    assert_true( ( *_batch[i] )[j] );
                    ptrs.push_back( ( *_batch[i] )[j].get() );
                }
        }

    return ptrs;
}

// ----------------------------------------------------------------------------

std::vector<std::string> SapHana::get_colnames(
    const std::string& _table ) const
{
    const auto sql_query = goutils::SapHana::select_stmt( _table );

    const auto [colnames, _] = goutils::SapHana::colnames(
        user_,
        password_,
        host_,
        port_,
        default_schema_,
        ping_interval_,
        sql_query );

    assert_true( colnames );

    const auto to_string = []( const auto& _ptr ) -> std::string {
        return std::string( _ptr.get() );
    };

    return stl::collect::vector<std::string>(
        *colnames | VIEWS::transform( to_string ) );
}

// ----------------------------------------------------------------------------

std::vector<io::Datatype> SapHana::get_coltypes(
    const std::string& _table, const std::vector<std::string>& _colnames ) const
{
    const auto [typenames, _] = goutils::SapHana::coltypes(
        user_,
        password_,
        host_,
        port_,
        default_schema_,
        ping_interval_,
        _table );

    assert_true( typenames );

    return stl::collect::vector<io::Datatype>(
        *typenames | VIEWS::transform( GoutilsTypeInferrer::to_datatype ) );
}

// ----------------------------------------------------------------------------

Poco::JSON::Object SapHana::get_content(
    const std::string& _tname,
    const std::int32_t _draw,
    const std::int32_t _start,
    const std::int32_t _length )
{
    // ----------------------------------------

    const auto nrows = get_nrows( _tname );

    const auto colnames = get_colnames( _tname );

    const auto ncols = colnames.size();

    // ----------------------------------------

    Poco::JSON::Object obj;

    // ----------------------------------------

    obj.set( "draw", _draw );

    obj.set( "recordsTotal", nrows );

    obj.set( "recordsFiltered", nrows );

    if ( nrows == 0 )
        {
            obj.set( "data", Poco::JSON::Array() );
            return obj;
        }

    // ----------------------------------------

    if ( _length < 0 )
        {
            throw std::invalid_argument( "length must be positive!" );
        }

    if ( _start < 0 )
        {
            throw std::invalid_argument( "start must be positive!" );
        }

    if ( _start >= nrows )
        {
            throw std::invalid_argument(
                "start must be smaller than number of rows!" );
        }

    // ----------------------------------------

    const auto begin = _start;

    const auto end = ( _start + _length > nrows ) ? nrows : _start + _length;

    const auto query = make_get_content_query( _tname, colnames, begin, end );

    // ----------------------------------------

    const auto iterator = select( query );

    assert_true( iterator );

    // ----------------------------------------

    const auto make_data = [&iterator, begin, end, ncols]() {
        Poco::JSON::Array data;

        for ( auto i = begin; i < end; ++i )
            {
                Poco::JSON::Array row;

                for ( size_t j = 0; j < ncols; ++j )
                    {
                        row.add( iterator->get_string() );
                    }

                data.add( row );
            }

        return data;
    };

    // ----------------------------------------

    obj.set( "data", make_data() );

    // ----------------------------------------

    return obj;

    // ----------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<std::string> SapHana::list_tables()
{
    const auto sql =
        "SELECT \"TABLE_NAME\" FROM public.tables WHERE \"SCHEMA_NAME\" = \'" +
        default_schema_ + "\'";

    const auto iterator = select( sql );

    std::vector<std::string> tables;

    while ( !iterator->end() )
        {
            tables.push_back( iterator->get_string() );
        }

    return tables;
}

// ----------------------------------------------------------------------------

std::vector<typename SapHana::RecordType> SapHana::make_batch(
    io::Reader* _reader ) const
{
    constexpr size_t batch_size = 100000;

    std::vector<typename SapHana::RecordType> records;

    for ( size_t i = 0; i < batch_size; ++i )
        {
            const auto line = _reader->next_line();

            const auto record = goutils::Helpers::to_vec( line );

            records.push_back( record );

            if ( _reader->eof() )
                {
                    break;
                }
        }

    return records;
}

// ----------------------------------------------------------------------------

std::string SapHana::make_get_content_query(
    const std::string& _table,
    const std::vector<std::string>& _colnames,
    const std::int32_t _begin,
    const std::int32_t _end ) const
{
    assert_true( _end >= _begin );

    std::stringstream query;

    query << "SELECT ";

    for ( size_t i = 0; i < _colnames.size(); ++i )
        {
            query << "\"" << _colnames[i] << "\"";

            if ( i != _colnames.size() - 1 )
                {
                    query << ", ";
                }
        }

    query << " FROM \"";

    query << _table;

    query << "\" LIMIT " << _end - _begin;

    if ( _begin != 0 )
        {
            query << " OFFSET " << _begin;
        }

    query << ";";

    return query.str();
}

// ----------------------------------------------------------------------------

std::vector<std::string> SapHana::merge_procedures(
    const std::vector<std::string>& _splitted ) const
{
    std::vector<std::string> merged;

    for ( size_t i = 0; i < _splitted.size(); )
        {
            if ( ( _splitted.at( i ).find( "CREATE PROCEDURE" ) ==
                   std::string::npos ) &&
                 ( _splitted.at( i ).find( "CREATE OR REPLACE PROCEDURE" ) ==
                   std::string::npos ) )
                {
                    merged.push_back( _splitted.at( i ) );
                    ++i;
                    continue;
                }

            std::string to_be_merged;

            size_t j = i;

            for ( ; j < _splitted.size(); ++j )
                {
                    to_be_merged += _splitted.at( j );

                    if ( _splitted.at( j ).find( "END;" ) != std::string::npos )
                        {
                            break;
                        }
                }

            merged.push_back( to_be_merged );

            i = j + 1;
        }

    return merged;
}

// ----------------------------------------------------------------------------

void SapHana::read(
    const std::string& _table, const size_t _skip, io::Reader* _reader )
{
    while ( !_reader->eof() )
        {
            const auto batch = make_batch( _reader );

            const auto nrows = batch.size();

            const auto ncols =
                nrows > 0 ? batch.at( 0 )->size() : static_cast<size_t>( 0 );

            auto ptrs = extract_ptrs( batch );

            goutils::SapHana::load(
                user_,
                password_,
                host_,
                port_,
                default_schema_,
                ping_interval_,
                _table,
                nrows,
                ncols,
                ptrs.data() );
        }
}

// ----------------------------------------------------------------------------

std::shared_ptr<Iterator> SapHana::select(
    const std::vector<std::string>& _colnames,
    const std::string& _tname,
    const std::string& _where )
{
    std::stringstream sql;

    sql << "SELECT ";

    for ( size_t i = 0; i < _colnames.size(); ++i )
        {
            if ( _colnames.at( i ) != "COUNT(*)" )
                {
                    sql << "\"" << _colnames.at( i ) << "\"";
                }
            else
                {
                    sql << _colnames.at( i );
                }

            if ( i != _colnames.size() - 1 )
                {
                    sql << ", ";
                }
        }

    sql << " FROM \"" << _tname << "\"";

    if ( _where != "" )
        {
            sql << "WHERE " << _where;
        }

    sql << ";";

    return select( sql.str() );
}

// ----------------------------------------------------------------------------

std::shared_ptr<Iterator> SapHana::select( const std::string& _sql )
{
    const auto [colnames_ptr, _] = goutils::SapHana::colnames(
        user_, password_, host_, port_, default_schema_, ping_interval_, _sql );

    assert_true( colnames_ptr );

    const auto to_string = []( const auto& _ptr ) -> std::string {
        return std::string( _ptr.get() );
    };

    const auto colnames = stl::collect::vector<std::string>(
        *colnames_ptr | VIEWS::transform( to_string ) );

    const auto data = goutils::SapHana::query(
        user_, password_, host_, port_, default_schema_, ping_interval_, _sql );

    return std::make_shared<GoutilsIterator>( colnames, data, time_formats_ );
}

// ----------------------------------------------------------------------------

std::vector<std::string> SapHana::split( const std::string& _sql ) const
{
    std::vector<std::string> splitted;

    size_t begin = 0;

    while ( true )
        {
            const auto pos = _sql.find( ";", begin );

            const auto len =
                pos == std::string::npos ? std::string::npos : pos - begin;

            const auto token = _sql.substr( begin, len );

            const auto trimmed = io::Parser::trim( token );

            if ( trimmed != "" )
                {
                    splitted.push_back( trimmed + ";" );
                }

            if ( pos == std::string::npos )
                {
                    break;
                }

            begin += token.size() + 1;
        }

    return merge_procedures( splitted );
}

// ----------------------------------------------------------------------------
}  // namespace database
