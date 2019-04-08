#ifndef ENGINE_HANDLERS_FILEHANDLER_HPP_
#define ENGINE_HANDLERS_FILEHANDLER_HPP_

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

struct FileHandler
{
    /// Creates a project directory, including the subfolders
    /// "data" and "models", if they do not already exist
    static std::string create_project_directory(
        const std::string& _project_name, const config::Options& _options );

    /// Determines the appropriate file ending for MatrixType
    template <class MatrixType>
    static std::string file_ending();

    /// Loads matrix from disc
    template <class MatrixType>
    static MatrixType load(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Loads data frame from disc
    static containers::DataFrame load(
        const std::map<std::string, containers::DataFrame>& _data_frames,
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        const std::string& _project_directory,
        const std::string& _name/*,
        licensing::LicenseChecker& _license_checker*/ );

    /// Load the encodings
    static void load_encodings(
        const std::string& _path,
        containers::Encoding* _categories,
        containers::Encoding* _join_keys_encodings );

    /// Reads categories or join keys encoding from file
    static std::vector<std::string> read_strings_big_endian(
        const std::string& _fname );

    /// Reads categories or join keys encoding from file
    static std::vector<std::string> read_strings_little_endian(
        const std::string& _fname );

    /// Removes DataFrame or Model
    template <class Type>
    static void remove(
        const std::string& _name,
        const std::string& _project_directory,
        const Poco::JSON::Object& _cmd,
        std::map<std::string, Type>* _map );

    /// Saves matrix to disc
    template <class MatrixType>
    static void save(
        const std::string& _name,
        const std::map<std::string, MatrixType>& _map,
        Poco::Net::StreamSocket* _socket );

    /// Saves the encodings to disk
    static void save_encodings(
        const std::string& _path,
        const containers::Encoding& _categories,
        const containers::Encoding& _join_keys_encodings );

    /// Writes categories or join keys encoding to file
    static void write_string_big_endian(
        const std::string& _fname, const containers::Encoding& _strings );

    /// Writes categories or join keys encoding to file
    static void write_string_little_endian(
        const std::string& _fname, const containers::Encoding& _strings );
};

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------

template <class MatrixType>
std::string FileHandler::file_ending()
{
    if ( std::is_same<MatrixType, containers::Matrix<ENGINE_FLOAT>>::value )
        {
            return ".mat";
        }
    else if ( std::is_same<MatrixType, containers::Matrix<ENGINE_INT>>::value )
        {
            return ".key";
        }
    else
        {
            return "";
        }
}

// ------------------------------------------------------------------------

template <class MatrixType>
MatrixType FileHandler::load(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    // ---------------------------------------------------------------------
    // Create file name

    std::string fname = _name + FileHandler::file_ending<MatrixType>();

    // ---------------------------------------------------------------------
    // Make sure that the file exists

    if ( !Poco::File( fname ).exists() )
        {
            throw std::runtime_error(
                "File '" + Poco::Path( fname ).makeAbsolute().toString() +
                "' not found!" );
        }

    communication::Sender::send_string( "Found!", _socket );

    // ---------------------------------------------------------------------
    // Load data

    MatrixType mat;

    mat.load( fname );

    // ---------------------------------------------------------------------

    return mat;
}

// ------------------------------------------------------------------------

template <class Type>
void FileHandler::remove(
    const std::string& _name,
    const std::string& _project_directory,
    const Poco::JSON::Object& _cmd,
    std::map<std::string, Type>* _map )
{
    bool mem_only = false;

    if ( _cmd.has( "mem_only_" ) )
        {
            mem_only = JSON::get_value<bool>( _cmd, "mem_only_" );
        }

    _map->erase( _name );

    if ( !mem_only && _project_directory != "" )
        {
            auto file = Poco::File( _project_directory + "models/" + _name );

            if ( std::is_same<Type, containers::DataFrame>() )
                {
                    file = Poco::File( _project_directory + "data/" + _name );
                }

            if ( file.exists() )
                {
                    file.remove( true );
                }
        }
}

// ------------------------------------------------------------------------

template <class MatrixType>
void FileHandler::save(
    const std::string& _name,
    const std::map<std::string, MatrixType>& _map,
    Poco::Net::StreamSocket* _socket )

{
    auto mat = utils::Getter::get( _map, _name );

    communication::Sender::send_string( "Found!", _socket );

    std::string fname = _name;

    fname.append( FileHandler::file_ending<MatrixType>() );

    mat.save( fname );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_FILEHANDLER_HPP_
