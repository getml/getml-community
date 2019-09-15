#ifndef DATABASE_POSTGRESITERATOR_HPP_
#define DATABASE_POSTGRESITERATOR_HPP_

namespace database
{
// -----------------------------------------------------------------------------

class PostgresIterator : public Iterator
{
    // -------------------------------------------------------------------------

   public:
    PostgresIterator(
        const std::shared_ptr<PGconn>& _connection,
        const std::string& _sql,
        const std::vector<std::string>& _time_formats,
        const std::int32_t _begin = -1,
        const std::int32_t _end = -1 );

    PostgresIterator(
        const std::shared_ptr<PGconn>& _connection,
        const std::vector<std::string>& _colnames,
        const std::vector<std::string>& _time_formats,
        const std::string& _tname,
        const std::string& _where,
        const std::int32_t _begin = -1,
        const std::int32_t _end = -1 );

    ~PostgresIterator();

    // -------------------------------------------------------------------------

   public:
    /// Returns the column names of the query.
    std::vector<std::string> colnames() const final;

    /// Returns a double.
    Float get_double() final;

    /// Returns an int.
    Int get_int() final;

    /// Returns a time stamps and increments the iterator.
    Float get_time_stamp() final;

    /// Returns a string .
    std::string get_string() final;

    // -------------------------------------------------------------------------

   public:
    /// Whether the end is reached.
    bool end() const final { return ( PQntuples( result() ) == 0 ); }

    // -------------------------------------------------------------------------

   private:
    /// Executes an SQL command.
    std::shared_ptr<PGresult> execute( const std::string& _sql ) const;

    /// Generates an SQL statement fro the colnames, the table name and an
    /// optional _where.
    static std::string make_sql(
        const std::vector<std::string>& _colnames,
        const std::string& _tname,
        const std::string& _where );

    // -------------------------------------------------------------------------

   private:
    /// Prevents segfaults before getting the next entry.
    void check()
    {
        if ( end() )
            {
                throw std::invalid_argument( "End of query is reached." );
            }

        if ( colnum_ >= num_cols_ )
            {
                throw std::invalid_argument( "Row number out of bounds." );
            }
    }

    /// Closes the cursor.
    void close_cursor()
    {
        auto raw_ptr = PQexec( connection(), "CLOSE getmlcursor" );
        PQclear( raw_ptr );
        close_required_ = false;
    }

    /// Trivial (private) accessor
    PGconn* connection() const
    {
        assert_true( connection_ );
        return connection_.get();
    }

    /// Ends the transaction.
    void end_transaction()
    {
        auto raw_ptr = PQexec( connection(), "END" );
        PQclear( raw_ptr );
        end_required_ = false;
    }

    /// Fetches the next n rows.
    void fetch_next( const std::int32_t _n )
    {
        result_ = execute(
            "FETCH FORWARD " + std::to_string( _n ) + " FROM getmlcursor;" );
    }

    /// Returns the raw value.
    std::pair<char*, bool> get_value()
    {
        check();

        const bool is_null = PQgetisnull( result(), rownum_, colnum_ );

        if ( is_null )
            {
                increment();

                return std::pair<char*, bool>( nullptr, true );
            }

        const auto val = PQgetvalue( result(), rownum_, colnum_ );

        increment();

        return std::pair<char*, bool>( val, false );
    }

    /// Increments the iterator.
    void increment()
    {
        if ( ++colnum_ == num_cols_ )
            {
                colnum_ = 0;

                if ( ++rownum_ == PQntuples( result() ) )
                    {
                        fetch_next( 10000 );
                        rownum_ = 0;
                    }

                if ( end() )
                    {
                        close_cursor();
                        end_transaction();
                    }
            }
    }

    /// Trivial (private) accessor
    PGresult* result() const
    {
        assert_true( result_ );
        return result_.get();
    }

    /// Skipts the next n rows.
    void skip_next( const std::int32_t _n )
    {
        result_ = execute(
            "MOVE FORWARD " + std::to_string( _n ) + " IN getmlcursor;" );
    }

    // -------------------------------------------------------------------------

   private:
    /// Whether we have to close the cursor upon destruction.
    bool close_required_;

    /// The current column.
    int colnum_;

    /// Shared ptr containing the connection object.
    const std::shared_ptr<PGconn> connection_;

    /// Whether we have to end the transaction upon destruction.
    bool end_required_;

    /// The total number of columns.
    int num_cols_;

    /// Result of the query.
    std::shared_ptr<PGresult> result_;

    /// The current row.
    int rownum_;

    /// Vector containing the time formats.
    const std::vector<std::string> time_formats_;

    // -------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace database

#endif  // DATABASE_POSTGRESITERATOR_HPP_
