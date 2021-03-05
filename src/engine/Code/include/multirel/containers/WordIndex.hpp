#ifndef MULTIREL_CONTAINER_WORDINDEX_HPP_
#define MULTIREL_CONTAINER_WORDINDEX_HPP_

namespace multirel
{
namespace containers
{
// -------------------------------------------------------------------------

/// Allows us to find the matches related to a word quickly. This is needed for
/// generating conditions based on individual words.
class WordIndex
{
   public:
    WordIndex(
        const MatchPtrs::iterator _begin,
        const MatchPtrs::iterator _end,
        const textmining::RowIndex &_row_index,
        const std::vector<size_t> &_rownum_indptr )
        : begin_( _begin ),
          end_( _end ),
          row_index_( _row_index ),
          rownum_indptr_( _rownum_indptr )
    {
    }

    ~WordIndex() = default;

    // -------------------------------

   public:
    /// Trivial accessor
    MatchPtrs::iterator begin() const { return begin_; }

    /// Trivial accessor
    MatchPtrs::iterator end() const { return end_; }

    /// Generates copies of the pointers for an individual word.
    void range( const Int _word, containers::MatchPtrs *_matches ) const
    {
        _matches->clear();

        for ( const auto rownum : row_index_.range( _word ) )
            {
                assert_true( rownum + 1 < rownum_indptr_.size() );

                for ( auto it = begin_ + rownum_indptr_[rownum];
                      it != begin_ + rownum_indptr_[rownum + 1];
                      ++it )
                    {
                        _matches->push_back( *it );
                    }
            }
    }

    // -------------------------------

   private:
    /// Iterator to the beginning of the bins.
    const MatchPtrs::iterator begin_;

    /// Iterator to the end of the bins.
    const MatchPtrs::iterator end_;

    /// Index mapping a word to all matching rows.
    const textmining::RowIndex &row_index_;

    /// Index mapping a row to all matches matches in the bins.
    const std::vector<size_t> &rownum_indptr_;

    // -------------------------------
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace multirel

#endif  // MULTIREL_CONTAINER_WORDINDEX_HPP_
