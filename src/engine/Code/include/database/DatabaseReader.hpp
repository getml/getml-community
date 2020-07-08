#ifndef DATABASE_DATABASEREADER_HPP_
#define DATABASE_DATABASEREADER_HPP_

namespace database
{
// ----------------------------------------------------------------------------

/// DatabaseReader implements the io::Reader interface to a table from a
/// database.
class DatabaseReader : public io::Reader
{
   public:
    DatabaseReader( const std::shared_ptr<Iterator>& _iterator )
        : iterator_( _iterator ), ncols_( iterator()->colnames().size() )
    {
    }

    ~DatabaseReader() {}

   public:
    /// Returns the colnames.
    std::vector<std::string> colnames() final { return iterator()->colnames(); }

    /// Whether we have reached the end of the iterator.
    bool eof() const final { return iterator()->end(); }

    /// Returns the next line.
    std::vector<std::string> next_line() final
    {
        auto line = std::vector<std::string>( ncols_ );
        for ( auto& field : line )
            {
                field = iterator()->get_string();
            }
        return line;
    }

    /// Implements the quotechar.
    char quotechar() const final { return '"'; }

    /// Implements the sep.
    char sep() const final { return '|'; }

   private:
    /// Trivial accessor.
    const std::shared_ptr<Iterator>& iterator() const
    {
        assert_true( iterator_ );
        return iterator_;
    }

   private:
    /// The underlying database iterator.
    const std::shared_ptr<Iterator> iterator_;

    /// The number of columns.
    const size_t ncols_;
};

// ----------------------------------------------------------------------------
}  // namespace database

#endif  // DATABASE_DATABASEREADER_HPP_
