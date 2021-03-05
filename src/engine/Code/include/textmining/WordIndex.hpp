#ifndef TEXTMINING_WORDINDEX_HPP_
#define TEXTMINING_WORDINDEX_HPP_

namespace textmining
{
// -------------------------------------------------------------------------

class WordIndex
{
   public:
    WordIndex(
        const stl::Range<const strings::String*>& _range,
        const std::shared_ptr<const std::vector<strings::String>>&
            _vocabulary );

    ~WordIndex();

   public:
    /// Returns the range for the _i'th word in the vocabulary
    stl::Range<const Int*> range( const size_t _i ) const
    {
        assert_true( _i + 1 < indptr_.size() );
        assert_true( indptr_[_i + 1] <= words_.size() );
        return stl::Range(
            words_.data() + indptr_[_i], words_.data() + indptr_[_i + 1] );
    }

    /// The number of rows.
    size_t nrows() const { return nrows_; }

    /// The size of the vocabulary.
    size_t size() const { return vocabulary().size(); }

    /// Trivial (const) accessor.
    const std::vector<strings::String>& vocabulary() const
    {
        assert_true( vocabulary_ );
        return *vocabulary_;
    }

    /// Trivial (const) accessor.
    std::shared_ptr<const std::vector<strings::String>> vocabulary_ptr() const
    {
        assert_true( vocabulary_ );
        return vocabulary_;
    }

    /// Trivial (const) accessor.
    const std::vector<Int>& words() const { return words_; }

   private:
    /// Generates the index and the indptr during construction.
    std::pair<std::vector<size_t>, std::vector<Int>> make_indptr_and_words(
        const stl::Range<const strings::String*>& _range ) const;

   private:
    /// Indicates the beginning and of each word in rownums.
    std::vector<size_t> indptr_;

    /// The number of rows
    size_t nrows_;

    /// The vocabulary.
    std::shared_ptr<const std::vector<strings::String>> vocabulary_;

    /// Indicates the words contained in the text field.
    std::vector<Int> words_;
};

// -------------------------------------------------------------------------
}  // namespace textmining

#endif  // TEXTMINING_WORDINDEX_HPP_
