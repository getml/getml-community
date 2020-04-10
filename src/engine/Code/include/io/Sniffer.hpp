#ifndef IO_SNIFFER_HPP_
#define IO_SNIFFER_HPP_

namespace io
{
// ----------------------------------------------------------------------------

template <class ReaderType>
class Sniffer
{
    // -------------------------------

   public:
    /// Constructer for CSVSniffer
    template <typename R = ReaderType>
    Sniffer(
        const std::string& _dialect,
        const std::vector<std::string>& _files,
        const bool _header,
        const size_t _num_lines_sniffed,
        const char _quotechar,
        const char _sep,
        const size_t _skip,
        const std::string& _table_name,
        typename std::enable_if<std::is_same<R, CSVReader>::value>::type* = 0 )
        : bucket_( "" ),
          dialect_( _dialect ),
          files_( _files ),
          header_( _header ),
          num_lines_sniffed_( _num_lines_sniffed ),
          quotechar_( _quotechar ),
          region_( "" ),
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

    /// Constructer for S3Sniffer
    template <typename R = ReaderType>
    Sniffer(
        const std::string& _bucket,
        const std::string& _dialect,
        const std::vector<std::string>& _files,
        const size_t _num_lines_sniffed,
        const std::string& _region,
        const char _sep,
        const size_t _skip,
        const std::string& _table_name,
        typename std::enable_if<std::is_same<R, S3Reader>::value>::type* = 0 )
        : bucket_( _bucket ),
          dialect_( _dialect ),
          files_( _files ),
          header_( true ),
          num_lines_sniffed_( _num_lines_sniffed ),
          quotechar_( '"' ),
          region_( _region ),
          sep_( _sep ),
          skip_( _skip ),
          table_name_( _table_name )
    {
        if ( _files.size() == 0 )
            {
                throw std::runtime_error(
                    "You need to provide at least one input key!" );
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

    /// Makes a CSVReader, when this is the required type for the reader.
    template <
        typename R = ReaderType,
        typename std::enable_if<std::is_same<R, CSVReader>::value, int>::type =
            0>
    CSVReader make_reader( const std::string& _fname ) const
    {
        return CSVReader( _fname, quotechar_, sep_ );
    }

    /// Makes a S3Reader, when this is the required type for the reader.
    template <
        typename R = ReaderType,
        typename std::enable_if<std::is_same<R, S3Reader>::value, int>::type =
            0>
    S3Reader make_reader( const std::string& _fname ) const
    {
        return S3Reader(
            bucket_,
            _fname,
            static_cast<Int>( num_lines_sniffed_ + 1 ),
            region_,
            sep_ );
    }

    // -------------------------------

   private:
    /// The bucket to be used (for S3Sniffer only).
    const std::string bucket_;

    /// The SQL dialect in which the CREATE TABLE statement is to be
    /// returned.
    const std::string dialect_;

    /// The files to be sniffed.
    const std::vector<std::string> files_;

    /// Whether the CSV files contain header variables.
    const bool header_;

    /// The number of lines sniffed in each file.
    const size_t num_lines_sniffed_;

    /// The character used for quotes.
    const char quotechar_;

    /// The region where the bucket is located (for S3Sniffer only).
    const std::string region_;

    /// The character used for separating fields.
    const char sep_;

    /// The number of lines skipped in each file.
    const size_t skip_;

    /// The name of the table to be produced.
    const std::string table_name_;

    // -------------------------------
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <class ReaderType>
void Sniffer<ReaderType>::check(
    const std::vector<std::string>& _line,
    const std::vector<std::string>& _colnames,
    const std::string& _fname ) const
{
    if ( _line.size() != _colnames.size() )
        {
            throw std::invalid_argument(
                "Wrong number of columns in '" + _fname + "'. Expected " +
                std::to_string( _colnames.size() ) + ", saw " +
                std::to_string( _line.size() ) + "." );
        }

    if ( header_ )
        {
            for ( size_t i = 0; i < _line.size(); ++i )
                {
                    if ( _line[i] != _colnames[i] )
                        {
                            throw std::runtime_error(
                                "Column " + std::to_string( i + 1 ) + " in '" +
                                _fname + "' has wrong name. Expected '" +
                                _colnames[i] + "', saw '" + _line[i] + "'." );
                        }
                }
        }
}

// ----------------------------------------------------------------------------

template <class ReaderType>
Datatype Sniffer<ReaderType>::infer_datatype(
    const Datatype _type, const std::string& _str ) const
{
    if ( ( _type == Datatype::integer || _type == Datatype::unknown ) &&
         is_int( _str ) )
        {
            return Datatype::integer;
        }
    else if (
        ( _type == Datatype::double_precision || _type == Datatype::unknown ||
          _type == Datatype::integer ) &&
        ( is_double( _str ) || is_int( _str ) ) )
        {
            return Datatype::double_precision;
        }
    else
        {
            return Datatype::string;
        }
}

// ----------------------------------------------------------------------------

template <class ReaderType>
void Sniffer<ReaderType>::init(
    const std::vector<std::string>& _line,
    std::vector<std::string>* _colnames,
    std::vector<Datatype>* _datatypes ) const
{
    if ( header_ )
        {
            *_colnames = _line;
        }
    else
        {
            for ( size_t i = 0; i < _line.size(); ++i )
                {
                    _colnames->push_back( "COLUMN_" + std::to_string( i + 1 ) );
                }
        }

    *_datatypes = std::vector<Datatype>( _line.size(), Datatype::unknown );
}

// ----------------------------------------------------------------------------

template <class ReaderType>
std::string Sniffer<ReaderType>::sniff() const
{
    // ------------------------------------------------------------------------

    auto colnames = std::vector<std::string>( 0 );

    auto datatypes = std::vector<Datatype>( 0 );

    // ------------------------------------------------------------------------

    for ( auto fname : files_ )
        {
            size_t line_count = 0;

            auto reader = make_reader( fname );

            while ( !reader.eof() && line_count < num_lines_sniffed_ )
                {
                    // --------------------------------------------------------
                    // Read the next line.

                    std::vector<std::string> line = reader.next_line();

                    if ( line.size() == 0 ) continue;

                    // --------------------------------------------------------
                    // Check the line size.

                    ++line_count;

                    if ( line_count - 1 == skip_ )
                        {
                            if ( colnames.size() == 0 )
                                init( line, &colnames, &datatypes );
                            else
                                check( line, colnames, fname );

                            if ( header_ ) continue;
                        }
                    else if ( line_count - 1 < skip_ )
                        {
                            continue;
                        }
                    else if ( line.size() != datatypes.size() )
                        {
                            std::cout << "Corrupted line: " << line_count
                                      << ". Expected " << datatypes.size()
                                      << " fields, saw " << line.size() << "."
                                      << std::endl;

                            continue;
                        }

                    // --------------------------------------------------------
                    // Do the actual parsing.

                    assert_true( datatypes.size() == line.size() );

                    assert_true( datatypes.size() == colnames.size() );

                    for ( size_t i = 0; i < datatypes.size(); ++i )
                        {
                            datatypes[i] =
                                infer_datatype( datatypes[i], line[i] );
                        }

                    // --------------------------------------------------------
                }
        }

    // ------------------------------------------------------------------------

    return StatementMaker::make_statement(
        table_name_, dialect_, colnames, datatypes );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace io

#endif  // IO_SNIFFER_HPP_
