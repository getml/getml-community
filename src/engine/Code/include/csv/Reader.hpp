#ifndef CSV_READER_HPP_
#define CSV_READER_HPP_

namespace csv
{
// ----------------------------------------------------------------------------

class Reader
{
    // -------------------------------

   public:
    Reader( const std::string& _fname, const char _quotechar, const char _sep )
        : filestream_( std::make_shared<std::ifstream>(
              std::ifstream( _fname, std::ifstream::in ) ) ),
          quotechar_( _quotechar ),
          sep_( _sep )
    {
        if ( !filestream_->is_open() )
            {
                throw std::runtime_error(
                    "'" + _fname + "' could not be opened!" );
            }
    }

    ~Reader() = default;

    // -------------------------------

   public:
    /// Returns the next line in the CSV file.
    std::vector<std::string> next_line();

    // -------------------------------

   public:
    /// Whether the end of the file has been reached.
    bool eof() const { return filestream_->eof(); }

    // -------------------------------

   private:
    /// The filestream of the CSV source file.
    const std::shared_ptr<std::ifstream> filestream_;

    /// The character used for quotes.
    const char quotechar_;

    /// The character used for separating fields.
    const char sep_;

    // -------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace csv

#endif  // CSV_READER_HPP_
