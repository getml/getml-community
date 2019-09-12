#ifndef CSV_READER_HPP_
#define CSV_READER_HPP_

namespace csv
{
// ----------------------------------------------------------------------------

class Reader
{
   public:
    /// Whether the end has been reached.
    virtual bool eof() const = 0;

    /// Returns the next line.
    virtual std::vector<std::string> next_line() = 0;

    /// Trivial getter.
    virtual char quotechar() const = 0;

    /// Trivial getter.
    virtual char sep() const = 0;
};

// ----------------------------------------------------------------------------
}  // namespace csv

#endif  // CSV_READER_HPP_
