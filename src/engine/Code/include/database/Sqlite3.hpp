#ifndef DATABASE_SQLITE3_HPP_
#define DATABASE_SQLITE3_HPP_

namespace database
{
// ----------------------------------------------------------------------------

class Sqlite3 : public Connector
{
    // -------------------------------

   public:
    Sqlite3(
        const std::string& _name,
        const std::vector<std::string>& _time_formats )
        : db_( make_db( _name ) ),
          name_( _name ),
          read_write_lock_( std::make_shared<multithreading::ReadWriteLock>() ),
          time_formats_( _time_formats )
    {
    }

    ~Sqlite3() = default;

    // -------------------------------

   public:
    /// Executes an SQL query.
    void execute( const std::string& _sql ) final;

    /// Returns the content of a table in a format that is compatible
    /// with the DataTables.js server-side processing API.
    Poco::JSON::Object get_content(
        const std::string& _tname,
        const std::int32_t _draw,
        const std::int32_t _start,
        const std::int32_t _length ) final;

    /// Reads a CSV file or another data source into a table.
    void read(
        const std::string& _table,
        const bool _header,
        const size_t _skip,
        csv::Reader* _reader ) final;

    /// Returns the names of the table columns.
    std::vector<std::string> get_colnames(
        const std::string& _table ) const final;

    /// Returns the types of the table columns.
    std::vector<csv::Datatype> get_coltypes(
        const std::string& _table,
        const std::vector<std::string>& _colnames ) const final;

    /// Lists the name of the tables held in the database.
    std::vector<std::string> list_tables() final;

    // -------------------------------

   public:
    /// Returns the dialect of the connector.
    std::string dialect() const final { return "sqlite"; }

    /// Drops a table and cleans up, if necessary.
    void drop_table( const std::string& _tname ) final
    {
        execute( "DROP TABLE " + _tname + "; VACUUM;" );
    }

    /// Returns the number of rows in the table signified by _tname.
    std::int32_t get_nrows( const std::string& _tname ) final
    {
        return select( {"COUNT(*)"}, _tname, "" )->get_int();
    }

    /// Returns a shared_ptr containing a Sqlite3Iterator.
    std::shared_ptr<Iterator> select(
        const std::vector<std::string>& _colnames,
        const std::string& _tname,
        const std::string& _where ) final
    {
        return std::make_shared<Sqlite3Iterator>(
            db_, _colnames, read_write_lock_, time_formats_, _tname, _where );
    }

    /// Returns a shared_ptr containing a Sqlite3Iterator.
    std::shared_ptr<Iterator> select( const std::string& _sql ) final
    {
        return std::make_shared<Sqlite3Iterator>(
            db_, _sql, read_write_lock_, time_formats_ );
    }

    /// Returns the time formats used.
    const std::vector<std::string>& time_formats() const
    {
        return time_formats_;
    }

    // -------------------------------

   private:
    /// Makes sure that the colnames of the CSV file match the colnames of the
    /// target table.
    void check_colnames(
        const std::vector<std::string>& _colnames, csv::Reader* _reader );

    /// Inserts a single line from a CSV file into a table.
    void insert_line(
        const std::vector<std::string>& _line,
        const std::vector<csv::Datatype>& _coltypes,
        sqlite3_stmt* stmt ) const;

    /// Inserts a column in double format.
    void insert_double(
        const std::vector<std::string>& _line,
        const int _colnum,
        sqlite3_stmt* _stmt ) const;

    /// Inserts a column in int format.
    void insert_int(
        const std::vector<std::string>& _line,
        const int _colnum,
        sqlite3_stmt* _stmt ) const;

    /// Inserts a column in text format.
    void insert_text(
        const std::vector<std::string>& _line,
        const int _colnum,
        sqlite3_stmt* _stmt ) const;

    /// Prepares a shared ptr to the database object with sqlite3_close(...) as
    /// a custom deleter. Called by the constructor.
    static std::shared_ptr<sqlite3> make_db( const std::string& _name );

    /// Prepares an insert statement for reading in CSV data.
    std::unique_ptr<sqlite3_stmt, int ( * )( sqlite3_stmt* )>
    make_insert_statement(
        const std::string& _table,
        const std::vector<std::string>& _colnames ) const;

    // -------------------------------

   private:
    /// Trivial (private) accessor
    sqlite3* db() const { return db_.get(); }

    /// Callback function that does nothing.
    static int do_nothing(
        void* NotUsed, int argc, char** argv, char** azColName ) noexcept
    {
        return 0;
    }

    /// Frees the error message, then throws and exception.
    void throw_exception( char* _error_message )
    {
        assert_true( _error_message != nullptr );
        std::string msg = _error_message;
        sqlite3_free( _error_message );
        throw std::runtime_error( msg );
    }

    // -------------------------------

   private:
    /// Shared ptr containing the database object.
    const std::shared_ptr<sqlite3> db_;

    /// Name of the database object.
    const std::string name_;

    /// For coordination.
    const std::shared_ptr<multithreading::ReadWriteLock> read_write_lock_;

    /// Vector containing the time formats.
    const std::vector<std::string> time_formats_;

    // -------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace database

#endif  // DATABASE_SQLITE3_HPP_
