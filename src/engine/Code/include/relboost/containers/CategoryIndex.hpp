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
        assert_true( end_ >= begin_ );
    }

    ~CategoryIndex() = default;

    // -------------------------------

   public:
    /// Builds the indptr during construction of the CategoryIndex.
    template <enums::DataUsed _data_used, typename DataFrameType>
    void build_indptr(
        const DataFrameType& _df,
        const size_t _num_column,
        const std::vector<Int>& _critical_values );

    // -------------------------------

   public:
    /// Trivial accessor.
    std::vector<const containers::Match*>::iterator begin() const
    {
        return begin_;
    }

    /// Returns iterator to the beginning of a set of categories.
    std::vector<const containers::Match*>::iterator begin(
        const Int _category ) const
    {
        if ( indptr_.size() == 0 )
            {
                return begin_;
            }

        assert_true( _category - minimum_ >= 0 );
        assert_true(
            _category - minimum_ + 1 <
            static_cast<Int>( indptr_.size() ) );

        return begin_ + indptr_[_category - minimum_];
    }

    /// Trivial accessor.
    std::vector<const containers::Match*>::iterator end() const { return end_; }

    /// Returns iterator to the end of a set of categories.
    std::vector<const containers::Match*>::iterator end(
        const Int _category ) const
    {
        if ( indptr_.size() == 0 )
            {
                return begin_;
            }

        assert_true( _category - minimum_ >= 0 );
        assert_true(
            _category - minimum_ + 1 <
            static_cast<Int>( indptr_.size() ) );

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
    std::vector<Int> indptr_;

    /// Minimum values of the samples.
    Int minimum_;
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

template <enums::DataUsed _data_used, typename DataFrameType>
void CategoryIndex::build_indptr(
    const DataFrameType& _df,
    const size_t _num_column,
    const std::vector<Int>& _critical_values )
{
    // ------------------------------------------------------------------------

    assert_true( end_ >= begin_ );

    if ( std::distance( begin_, end_ ) == 0 || _critical_values.size() == 0 )
        {
            indptr_ = std::vector<Int>( 0 );
            return;
        }

    // ------------------------------------------------------------------------

    const auto minimum = _critical_values.front();
    const auto maximum = _critical_values.back();

    assert_true( maximum >= minimum );

    const auto dist =
        static_cast<Int>( std::distance( begin_, end_ ) );

    indptr_ = std::vector<Int>( maximum - minimum + 2 );

    minimum_ = minimum;

    // ------------------------------------------------------------------------

    Int i = 0;

    for ( auto cat = minimum; cat <= maximum + 1; ++cat )
        {
            while ( i < dist )
                {
                    const auto num_row = get_num_row<_data_used>( begin_[i] );

                    if ( _df.categorical( num_row, _num_column ) < cat )
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
            assert_true( val >= 0 );
            assert_true( val <= dist );
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

                            assert_true(
                                _df.categorical( num_row, _num_column ) ==
                                cat );
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
