#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

std::string FileHandler::create_project_directory(
    const std::string& _project_name, const config::Options& _options )
{
    if ( _project_name == "" )
        {
            throw std::invalid_argument(
                "Project name can not be an "
                "empty string!" );
        }

    auto project_directory =
        _options.all_projects_directory_ + _project_name + "/";

    Poco::File( project_directory ).createDirectories();

    Poco::File( project_directory + "autosql-models/" ).createDirectories();

    Poco::File( project_directory + "data/" ).createDirectories();

    Poco::File( project_directory + "relboost-models/" ).createDirectories();

    return project_directory;
}

// ------------------------------------------------------------------------

containers::DataFrame FileHandler::load(
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const std::shared_ptr<containers::Encoding>& _categories,
    const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
    const std::string& _project_directory,
    const std::string& _name/*,
    licensing::LicenseChecker& _license_checker*/ )
{
    const auto path = _project_directory + "data/" + _name + "/";

    // ---------------------------------------------------------------------
    // Make sure that the directory exists and is directory

    {
        Poco::File file( path );

        if ( !file.exists() )
            {
                throw std::runtime_error(
                    "File or directory '" + path + "' not found!" );
            }

        if ( !file.isDirectory() )
            {
                throw std::runtime_error(
                    "'" + path + "' is not a directory!" );
            }
    }

    // ---------------------------------------------------------------------
    // Load data

    auto df = containers::DataFrame( _categories, _join_keys_encoding );

    df.name() = _name;

    df.load( path );

    /*if ( _license_checker.token().mem_size > 0 )
        {
            auto temporary_data_frames = _data_frames;

            temporary_data_frames[_name] = df;

            _license_checker.check_memory_size( temporary_data_frames, df );
        }*/

    // ---------------------------------------------------------------------

    return df;

    // ---------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void FileHandler::load_encodings(
    const std::string& _path,
    containers::Encoding* _categories,
    containers::Encoding* _join_keys_encodings )
{
    if ( utils::Endianness::is_little_endian() )
        {
            if ( Poco::File( _path + "categories" ).exists() )
                {
                    *_categories = FileHandler::read_strings_little_endian(
                        _path + "categories" );
                }

            if ( Poco::File( _path + "join_keys_encoding" ).exists() )
                {
                    *_join_keys_encodings =
                        FileHandler::read_strings_little_endian(
                            _path + "join_keys_encoding" );
                }
        }
    else
        {
            if ( Poco::File( _path + "categories" ).exists() )
                {
                    *_categories = FileHandler::read_strings_big_endian(
                        _path + "categories" );
                }

            if ( Poco::File( _path + "join_keys_encoding" ).exists() )
                {
                    *_join_keys_encodings =
                        FileHandler::read_strings_big_endian(
                            _path + "join_keys_encoding" );
                }
        }
}

// ----------------------------------------------------------------------------

std::vector<std::string> FileHandler::read_strings_big_endian(
    const std::string& _fname )
{
    // ---------------------------------------------------------------------
    // Define a lambda function for the encoding of the categories and
    // join keys

    auto read_string = []( std::ifstream& _input ) {
        std::string str;

        size_t str_size = 0;

        _input.read( reinterpret_cast<char*>( &str_size ), sizeof( size_t ) );

        str.resize( str_size );

        _input.read( &str[0], str_size );

        return str;
    };

    // ---------------------------------------------------------------------
    // Read strings

    std::ifstream input( _fname, std::ios::binary );

    auto strings = std::vector<std::string>( 0 );

    while ( !input.eof() )
        {
            std::string str = read_string( input );

            if ( !input.eof() || str != "" )
                {
                    strings.push_back( str );
                }
        }

    // ---------------------------------------------------------------------

    return strings;

    // ---------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<std::string> FileHandler::read_strings_little_endian(
    const std::string& _fname )
{
    // ---------------------------------------------------------------------
    // Define a lambda function for the encoding of the categories and
    // join keys

    auto read_string = []( std::ifstream& _input ) {
        std::string str;

        size_t str_size = 0;

        _input.read( reinterpret_cast<char*>( &str_size ), sizeof( size_t ) );

        utils::Endianness::reverse_byte_order( &str_size );

        str.resize( str_size );

        _input.read( &str[0], str_size );

        return str;
    };

    // ---------------------------------------------------------------------
    // Read strings

    std::ifstream input( _fname, std::ios::binary );

    auto strings = std::vector<std::string>( 0 );

    while ( !input.eof() )
        {
            std::string str = read_string( input );

            if ( !input.eof() || str != "" )
                {
                    strings.push_back( str );
                }
        }

    // ---------------------------------------------------------------------

    return strings;

    // ---------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void FileHandler::save_encodings(
    const std::string& _path,
    const containers::Encoding& _categories,
    const containers::Encoding& _join_keys_encodings )
{
    if ( utils::Endianness::is_little_endian() )
        {
            if ( _categories.size() > 0 )
                {
                    FileHandler::write_string_little_endian(
                        _path + "categories", _categories );
                }

            if ( _join_keys_encodings.size() > 0 )
                {
                    FileHandler::write_string_little_endian(
                        _path + "join_keys_encoding", _join_keys_encodings );
                }
        }
    else
        {
            if ( _categories.size() > 0 )
                {
                    FileHandler::write_string_big_endian(
                        _path + "categories", _categories );
                }

            if ( _join_keys_encodings.size() > 0 )
                {
                    FileHandler::write_string_big_endian(
                        _path + "join_keys_encoding", _join_keys_encodings );
                }
        }
}

// ----------------------------------------------------------------------------

void FileHandler::write_string_big_endian(
    const std::string& _fname, const containers::Encoding& _strings )
{
    std::ofstream output( _fname, std::ios::binary );

    auto write_string = [&output]( const std::string& _str ) {
        size_t str_size = _str.size();

        output.write(
            reinterpret_cast<const char*>( &str_size ), sizeof( size_t ) );

        output.write( &( _str[0] ), _str.size() );
    };

    std::for_each( _strings.begin(), _strings.end(), write_string );
}

// ----------------------------------------------------------------------------

void FileHandler::write_string_little_endian(
    const std::string& _fname, const containers::Encoding& _strings )
{
    std::ofstream output( _fname, std::ios::binary );

    auto write_string = [&output]( const std::string& _str ) {
        size_t str_size = _str.size();

        utils::Endianness::reverse_byte_order( &str_size );

        output.write(
            reinterpret_cast<const char*>( &str_size ), sizeof( size_t ) );

        output.write( &( _str[0] ), _str.size() );
    };

    std::for_each( _strings.begin(), _strings.end(), write_string );
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine
