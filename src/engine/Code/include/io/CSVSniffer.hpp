#ifndef IO_CSVSNIFFER_HPP_
#define IO_CSVSNIFFER_HPP_

namespace io
{
// ----------------------------------------------------------------------------

class CSVSniffer
{
    // -------------------------------

   public:
    CSVSniffer(
        const std::string& _dialect,
        const std::vector<std::string>& _files,
        const bool _header,
        const size_t _num_lines_sniffed,
        const char _quotechar,
        const char _sep,
        const size_t _skip,
        const std::string& _table_name )
        : dialect_( _dialect ),
          files_( _files ),
          header_( _header ),
          num_lines_sniffed_( _num_lines_sniffed ),
          quotechar_( _quotechar ),
          sep_( _sep ),
          skip_( _skip ),
          table_name_( _table_name )
    {
        if ( _files.size() == 0 )
            {
                throw std::runtime_error(
                    "You need to provide at least one input file!" );
            }
    }

    ~CSVSniffer() = default;

    // -------------------------------

   public:
    /// Returns a CREATE TABLE statement inferred from sniffing the files.
    std::string sniff() const;

    // -------------------------------

   private:
    /// Makes sure that the column headers are accurate.
    void check(
        const std::vector<std::string>& _line,
        const std::vector<std::string>& _colnames,
        const std::string& _fname ) const;

    /// Parses the datatype from a string.
    Datatype infer_datatype(
        const Datatype _type, const std::string& _str ) const;

    /// Initializes colnames and datatypes.
    void init(
        const std::vector<std::string>& _line,
        std::vector<std::string>* _colnames,
        std::vector<Datatype>* _datatypes ) const;

    // -------------------------------

   private:
    /// Checks whether a string can be converted to a double.
    bool is_double( const std::string& _str ) const
    {
        const auto [val, success] = Parser::to_double( _str );
        return success;
    }

    /// Checks whether a string can be converted to an integer.
    bool is_int( const std::string& _str ) const
    {
        const auto [val, success] = Parser::to_int( _str );
        return success;
    }

    // -------------------------------

   private:
    /// The SQL dialect in which the CREATE TABLE statement is to be returned.
    const std::string dialect_;

    /// The files to be sniffed.
    const std::vector<std::string> files_;

    /// Whether the CSV files contain header variables.
    const bool header_;

    /// The number of lines sniffed in each file.
    const size_t num_lines_sniffed_;

    /// The character used for quotes.
    const char quotechar_;

    /// The character used for separating fields.
    const char sep_;

    /// The number of lines skipped in each file.
    const size_t skip_;

    /// The name of the table to be produced.
    const std::string table_name_;

    // -------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace io

#endif  // IO_CSVSNIFFER_HPP_
