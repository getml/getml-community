#ifndef RELBOOST_CONTAINERS_INTSET_HPP_
#define RELBOOST_CONTAINERS_INTSET_HPP_

namespace relboost
{
namespace containers
{
// -------------------------------------------------------------------------

/// We have a core advantage in that we can know the maximum possible value of
/// in advance. This enables us to implement this set, which enables a drastic
/// speed-up when compared to std::unordered_set (let alone std::set).
class IntSet
{
   public:
    IntSet( const size_t _maximum_value )
        : already_included_( std::vector<bool>( _maximum_value, false ) ),
          maximum_value_( _maximum_value )
    {
    }

    ~IntSet() = default;

    // -------------------------------

    typedef std::vector<size_t>::const_iterator Iterator;

    // -------------------------------

    /// Returns beginning of unique integers
    std::vector<size_t>::const_iterator begin() const
    {
        return unique_integers_.cbegin();
    }

    /// Deletes all entries
    void clear()
    {
        for ( size_t i : unique_integers_ )
            {
                already_included_[i] = false;
            }
        unique_integers_.clear();
    }

    /// Returns end of unique integers
    std::vector<size_t>::const_iterator end() const
    {
        return unique_integers_.cend();
    }

    /// Adds an integer to unique_integers_, if it is not already included
    void insert( const size_t _val )
    {
        assert( _val >= 0 );
        assert( _val < static_cast<size_t>( already_included_.size() ) );

        if ( !already_included_[_val] )
            {
                unique_integers_.push_back( _val );
                already_included_[_val] = true;
            }
    }

    /// Trivial getter
    const size_t maximum_value() const { return maximum_value_; }

    /// Resizes the container.
    void resize( size_t _size ) { *this = std::move( IntSet( _size ) ); }

    /// Whether the IntSet is empty
    size_t size() const { return unique_integers_.size(); }

    /// Trivial getter
    const std::vector<size_t>& unique_integers() const
    {
        return unique_integers_;
    }

    // -------------------------------

   private:
    /// Denotes whether the integer is already included
    std::vector<bool> already_included_;

    /// The maximum integer that can be stored in the IntSet
    size_t maximum_value_;

    /// Contains all integers that have been included
    std::vector<size_t> unique_integers_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace relboost

#endif  // RELBOOST_CONTAINERS_INTSET_HPP_
