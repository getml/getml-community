#ifndef RELBOOST_CONTAINERS_CATEGORYINDEX_HPP_
#define RELBOOST_CONTAINERS_CATEGORYINDEX_HPP_

namespace relboost
{
namespace containers
{
// -------------------------------------------------------------------------

/// Allows us to find categories quickly.
class CategoryIndex
{
   public:
    CategoryIndex(
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end )
        : begin_( _begin ), end_( _end ), minimum_( 0 )
    {
        assert( end_ >= begin_ );
    }

    ~CategoryIndex() = default;

    // -------------------------------

   public:
    /// Builds the indptr during construction of the CategoryIndex.
    template <enums::DataUsed _data_used>
    void build_indptr(
        const Matrix<RELBOOST_INT>& _categorical,
        const size_t _num_column,
        const std::vector<RELBOOST_INT>& _critical_values );

    // -------------------------------

   public:
    /// Trivial accessor.
    std::vector<const containers::Match*>::iterator begin() const
    {
        return begin_;
    }

    /// Returns iterator to the beginning of a set of categories.
    std::vector<const containers::Match*>::iterator begin(
        const RELBOOST_INT _category ) const
    {
        assert( _category - minimum_ >= 0 );
        assert(
            _category - minimum_ + 1 <
            static_cast<RELBOOST_INT>( indptr_.size() ) );
        return begin_ + indptr_[_category - minimum_];
    }

    /// Trivial accessor.
    std::vector<const containers::Match*>::iterator end() const { return end_; }

    /// Returns iterator to the end of a set of categories.
    std::vector<const containers::Match*>::iterator end(
        const RELBOOST_INT _category ) const
    {
        assert( _category - minimum_ >= 0 );
        assert(
            _category - minimum_ + 1 <
            static_cast<RELBOOST_INT>( indptr_.size() ) );
        return begin_ + indptr_[_category - minimum_ + 1];
    }

    // -------------------------------

   private:
    /// Gets the number of row (for categorical_input)
    template <
        enums::DataUsed _data_used,
        typename std::enable_if<
            _data_used == enums::DataUsed::categorical_input,
            int>::type = 0>
    static size_t get_num_row( const containers::Match* _m )
    {
        return _m->ix_input;
    }

    /// Gets the number of row (for categorical_output)
    template <
        enums::DataUsed _data_used,
        typename std::enable_if<
            _data_used == enums::DataUsed::categorical_output,
            int>::type = 0>
    static size_t get_num_row( const containers::Match* _m )
    {
        return _m->ix_output;
    }

    // -------------------------------

   private:
    /// Points to the first sample.
    const std::vector<const containers::Match*>::iterator begin_;

    /// Points to the first sample.
    const std::vector<const containers::Match*>::iterator end_;

    /// Contains all categories that have been included.
    std::vector<RELBOOST_INT> indptr_;

    /// Minimum values of the samples.
    RELBOOST_INT minimum_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace relboost

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

namespace relboost
{
namespace containers
{
// ----------------------------------------------------------------------------

template <enums::DataUsed _data_used>
void CategoryIndex::build_indptr(
    const Matrix<RELBOOST_INT>& _categorical,
    const size_t _num_column,
    const std::vector<RELBOOST_INT>& _critical_values )
{
    // ------------------------------------------------------------------------

    assert( end_ >= begin_ );

    if ( std::distance( begin_, end_ ) == 0 || _critical_values.size() == 0 )
        {
            indptr_ = std::vector<RELBOOST_INT>( 0 );
            return;
        }

    // ------------------------------------------------------------------------

    const auto minimum = _critical_values.front();
    const auto maximum = _critical_values.back();

    assert( maximum >= minimum );

    const auto dist =
        static_cast<RELBOOST_INT>( std::distance( begin_, end_ ) );

    indptr_ = std::vector<RELBOOST_INT>( maximum - minimum + 2 );

    minimum_ = minimum;

    // ------------------------------------------------------------------------

    RELBOOST_INT i = 0;

    for ( auto cat = minimum; cat <= maximum + 1; ++cat )
        {
            while ( i < dist )
                {
                    const auto num_row = get_num_row<_data_used>( begin_[i] );

                    if ( _categorical( num_row, _num_column ) < cat )
                        {
                            ++i;
                        }
                    else  // ( _begin[i]->categorical_value >= cat )
                        {
                            indptr_[cat - minimum] = i;
                            break;
                        }
                }

            if ( i == dist )
                {
                    indptr_[cat - minimum] = i;
                }
        }

        // ------------------------------------------------------------------------

#ifndef NDEBUG

    for ( auto val : indptr_ )
        {
            assert( val >= 0 );
            assert( val <= dist );
        }

#endif  // NDEBUG

        // ------------------------------------------------------------------------

#ifndef NDEBUG

    if ( indptr_.size() > 0 )
        {
            for ( auto cat = _critical_values.front();
                  cat <= _critical_values.back();
                  ++cat )
                {
                    for ( auto it = begin( cat ); it < end( cat ); ++it )
                        {
                            const auto num_row = get_num_row<_data_used>( *it );

                            assert(
                                _categorical( num_row, _num_column ) == cat );
                        }
                }
        }

#endif  // NDEBUG

    // ------------------------------------------------------------------------
}

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace relboost

#endif  // RELBOOST_CONTAINERS_CATEGORYINDEX_HPP_
