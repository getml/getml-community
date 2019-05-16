#ifndef DATABASE_SQLITE3ITERATOR_HPP_
#define DATABASE_SQLITE3ITERATOR_HPP_

namespace database
{
// ----------------------------------------------------------------------------

class Sqlite3Iterator : public Iterator
{
    // -------------------------------

   public:
    Sqlite3Iterator(
        const std::shared_ptr<sqlite3>& _db,
        const std::vector<std::string>& _colnames,
        const std::vector<std::string>& _time_formats,
        const std::string& _tname );

    ~Sqlite3Iterator() = default;

    // -------------------------------

   public:
    /// Returns a double and increments the iterator.
    DATABASE_FLOAT get_double() final;

    /// Returns a double and increments the iterator.
    DATABASE_INT get_int() final;

    /// Returns a string and increments the iterator.
    std::string get_string() final;

    /// Returns a time stamps and increments the iterator.
    DATABASE_FLOAT get_time_stamp() final;

    // -------------------------------

   public:
    /// Whether the end is reached.
    bool end() const final { return end_; }

    // -------------------------------

   private:
    /// Trivial (private) accessor
    sqlite3* db() const { return db_.get(); }

    /// Iterates to the next row, if there is one.
    void next_row() { end_ = ( sqlite3_step( stmt() ) != SQLITE_ROW ); }

    /// Trivial (private) accessor
    sqlite3_stmt* stmt() const
    {
        assert( stmt_ );
        return stmt_.get();
    }

    // -------------------------------

   private:
    /// The current column.
    int colnum_;

    /// Shared ptr containing the database object.
    const std::shared_ptr<sqlite3> db_;

    /// Whether the end is reached.
    bool end_;

    /// The total number of columns.
    const int num_cols_;

    /// Unique ptr to the statement we are iterating through.
    std::unique_ptr<sqlite3_stmt, int ( * )( sqlite3_stmt* )> stmt_;

    /// Vector containing the time formats.
    const std::vector<std::string> time_formats_;

    // -------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace database

#endif  // DATABASE_SQLITE3ITERATOR_HPP_
