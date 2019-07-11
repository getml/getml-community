#ifndef ENGINE_HANDLERS_DATAFRAMEMANAGER_HPP_
#define ENGINE_HANDLERS_DATAFRAMEMANAGER_HPP_

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

class DataFrameManager
{
   public:
    DataFrameManager(
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<database::Connector> _connector,
        const std::shared_ptr<std::map<std::string, containers::DataFrame>>
            _data_frames,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        /*const std::shared_ptr<engine::licensing::LicenseChecker>&
            _license_checker,*/
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::shared_ptr<const monitoring::Monitor>& _monitor,
        const std::shared_ptr<multithreading::ReadWriteLock>& _read_write_lock )
        : categories_( _categories ),
          connector_( _connector ),
          data_frames_( _data_frames ),
          join_keys_encoding_( _join_keys_encoding ),
          // license_checker_( _license_checker ),
          logger_( _logger ),
          monitor_( _monitor ),
          read_write_lock_( _read_write_lock )
    {
    }

    ~DataFrameManager() = default;

    // ------------------------------------------------------------------------

   public:
    /// Adds a new categorical column to an existing data frame.
    void add_categorical_column(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Adds a new column to an existing data frame.
    void add_column(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Creates a new data frame and adds it to the map of data frames.
    void add_data_frame(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Appends data to an existing data frame.
    void append_to_data_frame(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Creates a new data frame from a table in the database.
    void from_db(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        const bool _append,
        Poco::Net::StreamSocket* _socket );

    /// Creates a new data frame from a JSON string.
    void from_json(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        const bool _append,
        Poco::Net::StreamSocket* _socket );

    /// Sends a boolean columm to the client
    void get_boolean_column(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Sends a categorical columm to the client
    void get_categorical_column(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Sends a column to the client
    void get_column(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Sends a data frame back to the client, column-by-column.
    void get_data_frame( Poco::Net::StreamSocket* _socket );

    /// Sends the content of a data frame in a format that is compatible with
    /// DataTables.js server-side processing.
    void get_data_frame_content(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Get the size of a data frame
    void get_nbytes(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Refreshes a data frame.
    void refresh( const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Removes a column from a DataFrame.
    void remove_column(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Changes the unit of _col.
    void set_unit(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Changes the unit of _col.
    void set_unit_categorical(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Creates a new data frame by selecting from an existing one.
    void select(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Sends summary statistics back to the client.
    void summarize(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    // ------------------------------------------------------------------------

   private:
    /// Adds a categorical matrix to a data frame.
    void add_categorical_column(
        const Poco::JSON::Object& _cmd,
        containers::DataFrame* _df,
        Poco::Net::StreamSocket* _socket );

    /// Adds a matrix to a data frame.
    void add_column(
        const Poco::JSON::Object& _cmd,
        containers::DataFrame* _df,
        Poco::Net::StreamSocket* _socket );

    /// Tells the receive_data(...) method to no longer receive data and checks
    /// the memory size.
    void close(
        const containers::DataFrame& _df, Poco::Net::StreamSocket* _socket );

    /// Receives the actual data contained in a DataFrame
    void receive_data(
        containers::DataFrame* _df, Poco::Net::StreamSocket* _socket );

    // ------------------------------------------------------------------------

   private:
    /// Trivial accessor
    std::map<std::string, containers::DataFrame>& data_frames()
    {
        return *data_frames_;
    }

    /// Trivial accessor
    std::shared_ptr<database::Connector> connector()
    {
        assert( connector_ );
        return connector_;
    }

    /// Trivial accessor
    const monitoring::Logger& logger() { return *logger_; }

    // ------------------------------------------------------------------------

   private:
    /// Maps integeres to category names
    const std::shared_ptr<containers::Encoding> categories_;

    /// Connector to the underlying database.
    const std::shared_ptr<database::Connector> connector_;

    /// The data frames currently held in memory
    const std::shared_ptr<std::map<std::string, containers::DataFrame>>
        data_frames_;

    /// Maps integers to join key names
    const std::shared_ptr<containers::Encoding> join_keys_encoding_;

    /// For checking the license and memory usage
    // const std::shared_ptr<engine::licensing::LicenseChecker>
    // license_checker_;

    /// For logging
    const std::shared_ptr<const monitoring::Logger> logger_;

    /// For communication with the monitor
    const std::shared_ptr<const monitoring::Monitor> monitor_;

    /// For coordinating the read and write process of the data
    const std::shared_ptr<multithreading::ReadWriteLock> read_write_lock_;

    // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_DATAFRAMEMANAGER_HPP_
