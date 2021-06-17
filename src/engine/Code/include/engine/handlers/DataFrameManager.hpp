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
    static constexpr const char* FLOAT_COLUMN =
        containers::Column<bool>::FLOAT_COLUMN;
    static constexpr const char* STRING_COLUMN =
        containers::Column<bool>::STRING_COLUMN;

   public:
    DataFrameManager(
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<DatabaseManager>& _database_manager,
        const std::shared_ptr<std::map<std::string, containers::DataFrame>>
            _data_frames,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        const std::shared_ptr<licensing::LicenseChecker>& _license_checker,
        const std::shared_ptr<const communication::Logger>& _logger,
        const std::shared_ptr<const communication::Monitor>& _monitor,
        const config::Options& _options,
        const std::shared_ptr<multithreading::ReadWriteLock>& _read_write_lock )
        : categories_( _categories ),
          database_manager_( _database_manager ),
          data_frames_( _data_frames ),
          join_keys_encoding_( _join_keys_encoding ),
          license_checker_( _license_checker ),
          logger_( _logger ),
          monitor_( _monitor ),
          options_( _options ),
          read_write_lock_( _read_write_lock )
    {
    }

    ~DataFrameManager() = default;

    // ------------------------------------------------------------------------

   public:
    /// Creates a new data frame and adds it to the map of data frames.
    void add_data_frame(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Adds a new float column to an existing data frame (parsed by the column
    /// operators).
    void add_float_column(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Adds a new float column to an existing data frame (sent by the user, for
    /// instance as a numpy array).
    void add_float_column(
        const Poco::JSON::Object& _cmd, Poco::Net::StreamSocket* _socket );

    /// Adds a string column to an existing data frame (parsed by
    /// the column operators).
    void add_string_column(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Adds a new string column to an existing data frame (sent by the user,
    /// for instance as a numpy array).
    void add_string_column(
        const Poco::JSON::Object& _cmd, Poco::Net::StreamSocket* _socket );

    /// Undertakes an aggregation on an entire column.
    void aggregate(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Appends data to an existing data frame.
    void append_to_data_frame(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Calculates the plots for a categorical column.
    void calc_categorical_column_plots(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Calculates the plots for a column.
    void calc_column_plots(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Concatenates a list of data frames.
    void concat(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Creates a new data frame from a set of CSV files.
    void from_csv(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        const bool _append,
        Poco::Net::StreamSocket* _socket );

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

    /// Creates a new data frame from a database query.
    void from_query(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        const bool _append,
        Poco::Net::StreamSocket* _socket );

    /// Creates a new data frame from a set of S3 files located in a bucket.
    void from_s3(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        const bool _append,
        Poco::Net::StreamSocket* _socket );

    /// Creates a new data frame from a view.
    void from_view(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        const bool _append,
        Poco::Net::StreamSocket* _socket );

    /// Sends a boolean columm to the client
    void get_boolean_column(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Sends a JSON representing the column to the client.
    void get_boolean_column_content(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Returns the number of rows in boolean column.
    void get_boolean_column_nrows(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Sends a categorical columm to the client
    void get_categorical_column(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Sends a string describing the number of rows in a categorical column.
    void get_categorical_column_nrows(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Sends a JSON representing the column to the client.
    void get_categorical_column_content(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Returns the unique values from a categorical column.
    void get_categorical_column_unique(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Sends a column to the client
    void get_column(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Gets the number of rows in a float column.
    void get_column_nrows(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Returns the unique values from a float column.
    void get_column_unique(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Sends a data frame back to the client, column-by-column.
    void get_data_frame( Poco::Net::StreamSocket* _socket );

    /// Expresses the data frame in HTML format, for a Jupyter notebook.
    void get_data_frame_html(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Expresses the data frame as a string.
    void get_data_frame_string(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Sends the content of a data frame in a format that is compatible with
    /// DataTables.js server-side processing.
    void get_data_frame_content(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Sends a JSON representing the column to the client.
    void get_float_column_content(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Get the size of a data frame
    void get_nbytes(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Get the number of rows in a data frame
    void get_nrows(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Get the subroles for a float column.
    void get_subroles(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Get the subroles for a string column.
    void get_subroles_categorical(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Get the unit for a float column.
    void get_unit(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Get the unit for a string column.
    void get_unit_categorical(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Sends a JSON representing the view to the client.
    void get_view_content(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Sends a JSON representing the number of rows in the view, if known.
    void get_view_nrows(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Returns a string describing the last time a data frame has been changed.
    void last_change(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Refreshes a data frame.
    void refresh( const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Removes a column from a DataFrame.
    void remove_column(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Changes the subroles of the column.
    void set_subroles(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Changes the subroles of the column.
    void set_subroles_categorical(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Changes the unit of the column.
    void set_unit(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Changes the unit of the column.
    void set_unit_categorical(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Sends summary statistics back to the client.
    void summarize(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Writes the dataframe to CSV.
    void to_csv(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Writes the dataframe into the data base.
    void to_db(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Writes the dataframe to CSV files located in an S3 bucket.
    void to_s3(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    // ------------------------------------------------------------------------

   private:
    /// Takes care of the process of actually adding the column.
    void add_float_column_to_df(
        const std::string& _role,
        const containers::Column<Float>& _col,
        containers::DataFrame* _df,
        multithreading::WeakWriteLock* _weak_write_lock ) const;

    /// Adds an integer column to the data frame.
    void add_int_column_to_df(
        const std::string& _name,
        const std::string& _role,
        const std::string& _unit,
        const std::vector<std::string>& _vec,
        containers::DataFrame* _df,
        multithreading::WeakWriteLock* _weak_write_lock,
        Poco::Net::StreamSocket* _socket );

    /// Adds an integer column to the data frame.
    void add_int_column_to_df(
        const std::string& _name,
        const std::string& _role,
        const std::string& _unit,
        const std::vector<std::string>& _vec,
        const std::shared_ptr<containers::Encoding>& _local_categories,
        const std::shared_ptr<containers::Encoding>& _local_join_keys_encoding,
        containers::DataFrame* _df ) const;

    /// Adds a string column to the data frame.
    void add_string_column_to_df(
        const std::string& _name,
        const std::string& _role,
        const std::string& _unit,
        const std::vector<std::string>& _vec,
        containers::DataFrame* _df,
        multithreading::WeakWriteLock* _weak_write_lock ) const;

    /// Makes sure that all referenced DataFrames exist, that they
    /// have the same number of rows.
    /// Returns the inferred number of rows and whether any df has
    /// been found.
    std::pair<size_t, bool> check_nrows(
        const Poco::JSON::Object& _obj,
        const std::string& _cmp_df_name = "",
        const size_t _cmp_nrows = 0 ) const;

    template <typename T, typename IterType>
    std::string make_column_string(
        const Int _draw,
        const size_t _nrows,
        IterType _begin,
        IterType _end ) const;

    /// Receives the actual data contained in a DataFrame
    void receive_data(
        const std::shared_ptr<containers::Encoding>& _local_categories,
        const std::shared_ptr<containers::Encoding>& _local_join_keys_encoding,
        containers::DataFrame* _df,
        Poco::Net::StreamSocket* _socket ) const;

    /// Receives a FloatColumn and adds it to the DataFrame.
    void recv_and_add_float_column(
        const Poco::JSON::Object& _cmd,
        containers::DataFrame* _df,
        multithreading::WeakWriteLock* _weak_write_lock,
        Poco::Net::StreamSocket* _socket ) const;

    /// Adds a string column to a data frame.
    void recv_and_add_string_column(
        const Poco::JSON::Object& _cmd,
        containers::DataFrame* _df,
        multithreading::WeakWriteLock* _weak_write_lock,
        Poco::Net::StreamSocket* _socket );

    /// Adds a string column to a data frame.
    void recv_and_add_string_column(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<containers::Encoding>& _local_categories,
        const std::shared_ptr<containers::Encoding>& _local_join_keys_encoding,
        containers::DataFrame* _df,
        Poco::Net::StreamSocket* _socket ) const;

    // ------------------------------------------------------------------------

   private:
    /// Trivial accessor
    containers::Encoding& categories()
    {
        assert_true( categories_ );
        return *categories_;
    }

    /// Trivial accessor
    std::shared_ptr<database::Connector> connector( const std::string& _name )
    {
        assert_true( database_manager_ );
        return database_manager_->connector( _name );
    }

    /// Trivial accessor
    std::map<std::string, containers::DataFrame>& data_frames()
    {
        assert_true( data_frames_ );
        return *data_frames_;
    }

    /// Trivial accessor
    const std::map<std::string, containers::DataFrame>& data_frames() const
    {
        assert_true( data_frames_ );
        return *data_frames_;
    }

    /// Checks whether a data frame of a certain name exists
    bool df_exists( const std::string& _name )
    {
        const auto it = data_frames().find( _name );
        return ( it == data_frames().end() );
    }

    /// Trivial accessor
    containers::Encoding& join_keys_encoding()
    {
        assert_true( join_keys_encoding_ );
        return *join_keys_encoding_;
    }

    /// Trivial accessor
    licensing::LicenseChecker& license_checker()
    {
        assert_true( license_checker_ );
        return *license_checker_;
    }

    /// Trivial accessor
    const licensing::LicenseChecker& license_checker() const
    {
        assert_true( license_checker_ );
        return *license_checker_;
    }

    /// Trivial accessor
    const communication::Logger& logger() { return *logger_; }

    // ------------------------------------------------------------------------

   private:
    /// Maps integeres to category names
    const std::shared_ptr<containers::Encoding> categories_;

    /// Connector to the underlying database.
    const std::shared_ptr<DatabaseManager> database_manager_;

    /// The data frames currently held in memory
    const std::shared_ptr<std::map<std::string, containers::DataFrame>>
        data_frames_;

    /// Maps integers to join key names
    const std::shared_ptr<containers::Encoding> join_keys_encoding_;

    /// For checking the license and memory usage
    const std::shared_ptr<licensing::LicenseChecker> license_checker_;

    /// For logging
    const std::shared_ptr<const communication::Logger> logger_;

    /// For communication with the monitor
    const std::shared_ptr<const communication::Monitor> monitor_;

    /// Settings for the engine and the monitor
    const config::Options options_;

    /// For coordinating the read and write process of the data
    const std::shared_ptr<multithreading::ReadWriteLock> read_write_lock_;

    // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------

template <typename T, typename IterType>
std::string DataFrameManager::make_column_string(
    const Int _draw, const size_t _nrows, IterType _begin, IterType _end ) const
{
    // ----------------------------------------

    Poco::JSON::Object obj;

    // ----------------------------------------

    obj.set( "draw", _draw );

    obj.set( "recordsTotal", _nrows );

    obj.set( "recordsFiltered", _nrows );

    if ( _nrows == 0 )
        {
            obj.set( "data", Poco::JSON::Array() );

            return JSON::stringify( obj );
        }

    // ----------------------------------------

    auto data = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    for ( auto it = _begin; it < _end; ++it )
        {
            auto row = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

            if constexpr ( std::is_same<T, std::string>() )
                {
                    row->add( *it );
                }

            if constexpr ( !std::is_same<T, std::string>() )
                {
                    row->add( io::Parser::to_string( *it ) );
                }

            data->add( row );
        }

    // ----------------------------------------

    obj.set( "data", data );

    // ----------------------------------------

    return JSON::stringify( obj );

    // ----------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_DATAFRAMEMANAGER_HPP_
