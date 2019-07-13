#ifndef CSV_SNIFFER_HPP_
#define CSV_SNIFFER_HPP_

namespace csv
{
// ----------------------------------------------------------------------------

class Sniffer
{
    // -------------------------------

   public:
    Sniffer(
        const std::string& _dialect,
        const std::vector<std::string>& _files,
        const bool _header,
        const size_t _num_lines_sniffed,
        const char _quotechar,
        const char _sep,
        const std::string& _table_name,
        const std::vector<std::string>& _time_formats )
        : dialect_( _dialect ),
          files_( _files ),
          header_( _header ),
          num_lines_sniffed_( _num_lines_sniffed ),
          quotechar_( _quotechar ),
          sep_( _sep ),
          table_name_( _table_name ),
          time_formats_( _time_formats )
    {
        if ( _files.size() == 0 )
            {
                throw std::runtime_error(
                    "You need to provide at least one input file!" );
            }
    }

    ~Sniffer() = default;

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

    /// Initializes colnames and datatypes.
    void init(
        const std::vector<std::string>& _line,
        std::vector<std::string>* _colnames,
        std::vector<Datatype>* _datatypes ) const;

    /// Produces the CREATE TABLE statement.
    std::string make_statement(
        const std::vector<std::string>& _colnames,
        const std::vector<Datatype>& _datatypes ) const;

    /// Produces the CREATE TABLE statement for sqlite.
    std::string make_statement_sqlite(
        const std::vector<std::string>& _colnames,
        const std::vector<Datatype>& _datatypes ) const;

    // -------------------------------

   private:
    /// Parses the datatype from a string.
    Datatype infer_datatype(
        const Datatype _type, const std::string& _str ) const;

    /// Transforms a datatype to the string required for the sqlite dialect.
    std::string to_string_sqlite( const Datatype _type ) const;

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

    /// Checks whether a string can be converted to a time stamp.
    bool is_time_stamp( const std::string& _str ) const
    {
        const auto [val, success] =
            Parser::to_time_stamp( _str, time_formats_ );
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

    /// The name of the table to be produced.
    const std::string table_name_;

    /// The time formats to be tried for parsing time stamps. For a full
    /// documentation, see
    /// https://pocoproject.org/docs/Poco.DateTimeFormatter.html.
    const std::vector<std::string> time_formats_;

    // -------------------------------
};  // namespace csv

// ----------------------------------------------------------------------------
}  // namespace csv

#endif  // CSV_SNIFFER_HPP_
