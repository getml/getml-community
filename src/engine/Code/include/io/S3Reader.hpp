#ifndef IO_S3READER_HPP_
#define IO_S3READER_HPP_

namespace io
{
// ----------------------------------------------------------------------------

class S3Reader : public Reader
{
   public:
    typedef typename goutils::S3::RecordType RecordType;

   public:
    S3Reader(
        const std::string& _bucket,
        const std::optional<std::vector<std::string>>& _colnames,
        const std::string& _key,
        const Int& _limit,
        const std::string& _region,
        const char _sep )
        : colnames_( _colnames ),
          current_row_( 0 ),
          ncols_( 0 ),
          nrows_( 0 ),
          nskipped_( 0 ),
          sep_( _sep )
    {
        std::tie( records_, nrows_, ncols_, nskipped_ ) = goutils::S3::read_csv(
            _bucket, std::string( 1, _sep ), _key, _limit, _region );
        assert_true( records_ );
        assert_true( nrows_ * ncols_ == records_->size() );
    }

    ~S3Reader() = default;

    // -------------------------------

   public:
    /// Returns the next line in the CSV file.
    std::vector<std::string> next_line() final;

    // -------------------------------

   public:
    /// Colnames are either passed by the user or they are the first line of the
    /// CSV file.
    std::vector<std::string> colnames() final
    {
        if ( colnames_ )
            {
                return *colnames_;
            }
        else
            {
                return next_line();
            }
    }

    /// Whether the end of the file has been reached.
    bool eof() const final { return current_row_ >= nrows_; }

    /// Trivial getter.
    char quotechar() const final { return '"'; }

    /// Trivial getter.
    char sep() const final { return sep_; }

    // -------------------------------

   private:
    /// Colnames are either passed by the user or they are the first line of the
    /// CSV file.
    const std::optional<std::vector<std::string>> colnames_;

    /// The row we are currently in.
    size_t current_row_;

    /// The number of columns in the records.
    size_t ncols_;

    /// The number of rows in the records.
    size_t nrows_;

    /// The number of records that have been skipped.
    size_t nskipped_;

    /// The actual records - the complete file will be pulled into memory.
    RecordType records_;

    /// The character used for separating fields.
    const char sep_;

    // -------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace io

#endif  // IO_S3READER_HPP_
