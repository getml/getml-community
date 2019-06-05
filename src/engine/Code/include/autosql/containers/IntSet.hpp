#ifndef AUTOSQL_CONTAINER_INTSET_HPP_
#define AUTOSQL_CONTAINER_INTSET_HPP_

namespace autosql
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
    IntSet( const AUTOSQL_INT _maximum_value )
        : already_included_( std::vector<bool>( _maximum_value, false ) ),
          maximum_value_( _maximum_value )
    {
    }

    ~IntSet() = default;

    // -------------------------------

    /// Returns beginning of unique integers
    std::vector<AUTOSQL_INT>::const_iterator begin() const
    {
        return unique_integers_.cbegin();
    }

    /// Deletes all entries
    void clear()
    {
        for ( AUTOSQL_INT i : unique_integers_ )
            {
                already_included_[i] = false;
            }
        unique_integers_.clear();
    }

    /// Returns end of unique integers
    std::vector<AUTOSQL_INT>::const_iterator end() const
    {
        return unique_integers_.cend();
    }

    /// Adds an integer to unique_integers_, if it is not already included
    void insert( const AUTOSQL_INT _val )
    {
        assert( _val >= 0 );
        assert( _val < static_cast<AUTOSQL_INT>( already_included_.size() ) );

        if ( !already_included_[_val] )
            {
                unique_integers_.push_back( _val );
                already_included_[_val] = true;
            }
    }

    /// Trivial getter
    const AUTOSQL_INT maximum_value() const { return maximum_value_; }

    /// Whether the IntSet is empty
    size_t size() const { return unique_integers_.size(); }

    /// Trivial getter
    const std::vector<AUTOSQL_INT>& unique_integers() const
    {
        return unique_integers_;
    }

    // -------------------------------

   private:
    /// Denotes whether the integer is already included
    std::vector<bool> already_included_;

    /// The maximum integer that can be stored in the IntSet
    const AUTOSQL_INT maximum_value_;

    /// Contains all integers that have been included
    std::vector<AUTOSQL_INT> unique_integers_;
};

// -------------------------------------------------------------------------
}
}

#endif  // AUTOSQL_CONTAINER_INTSET_HPP_
