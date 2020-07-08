#ifndef ENGINE_CONTAINER_DATAFRAMEREADER_HPP_
#define ENGINE_CONTAINER_DATAFRAMEREADER_HPP_

namespace engine
{
namespace containers
{
// ----------------------------------------------------------------------------

class DataFrameReader : public io::Reader
{
    // -------------------------------

   public:
    DataFrameReader(
        const DataFrame& _df,
        const std::shared_ptr<engine::containers::Encoding>& _categories,
        const std::shared_ptr<engine::containers::Encoding>&
            _join_keys_encoding,
        const char _quotechar,
        const char _sep )
        : categories_( _categories ),
          colnames_( make_colnames( _df, _quotechar ) ),
          coltypes_( make_coltypes( _df ) ),
          df_( _df ),
          join_keys_encoding_( _join_keys_encoding ),
          rownum_( 0 ),
          quotechar_( _quotechar ),
          sep_( _sep )
    {
        assert_true( colnames().size() == coltypes().size() );
    }

    ~DataFrameReader() = default;

    // -------------------------------

   public:
    /// Returns the next line.
    std::vector<std::string> next_line() final;

    // -------------------------------

   public:
    /// Returns the column names.
    std::vector<std::string> colnames() final { return colnames_; }

    /// Trivial accessor.
    const std::vector<std::string>& colnames() const { return colnames_; }

    /// Trivial accessor.
    const std::vector<io::Datatype>& coltypes() const { return coltypes_; }

    /// Whether the end of the file has been reached.
    bool eof() const final { return ( rownum_ >= df_.nrows() ); }

    /// Trivial getter.
    char quotechar() const final { return quotechar_; }

    /// Trivial getter.
    char sep() const final { return sep_; }

    // -------------------------------

   private:
    /// Generates the column names.
    static std::vector<std::string> make_colnames(
        const DataFrame& _df, char _quotechar );

    /// Generates the column types.
    static std::vector<io::Datatype> make_coltypes( const DataFrame& _df );

    /// Updates the counts of the colnames.
    static void update_counts(
        const std::string& _colname, std::map<std::string, Int>* _counts );

    // -------------------------------

   private:
    /// Trivial (private const) accessor.
    const engine::containers::Encoding& categories() const
    {
        assert_true( categories_ );
        return *categories_;
    }

    /// Trivial (private const) accessor.
    const engine::containers::Encoding& join_keys_encoding() const
    {
        assert_true( join_keys_encoding_ );
        return *join_keys_encoding_;
    }

    // -------------------------------

   private:
    /// The encoding used for the categorical data.
    const std::shared_ptr<const engine::containers::Encoding> categories_;

    /// The colnames of table to be generated.
    const std::vector<std::string> colnames_;

    /// The coltypes of table to be generated.
    const std::vector<io::Datatype> coltypes_;

    /// The filestream of the CSV source file.
    const DataFrame df_;

    /// The encoding used for the join keys.
    const std::shared_ptr<const engine::containers::Encoding>
        join_keys_encoding_;

    /// The row we are currently in.
    size_t rownum_;

    /// The character used for quotes.
    const char quotechar_;

    /// The character used for separating fields.
    const char sep_;

    // -------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINER_DATAFRAMEREADER_HPP_
