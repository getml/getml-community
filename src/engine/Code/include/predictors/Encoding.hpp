#ifndef PREDICTORS_ENCODING_HPP_
#define PREDICTORS_ENCODING_HPP_

namespace predictors
{
// -------------------------------------------------------------------------

class Encoding
{
   public:
    Encoding() {}

    ~Encoding() = default;

    // -------------------------------

    /// Adds column to map or vector, returning the transformed column.
    CIntColumn fit_transform( const CIntColumn& _val );

    /// Transforms the column to the mapped integers.
    CIntColumn transform( const CIntColumn& _val ) const;

    // -------------------------------

    /// Deletes all entries
    void clear() { *this = Encoding(); }

    /// Number of encoded elements
    size_t size() const
    {
        assert( map_.size() == vector_.size() );
        return vector_.size();
    }

    // -------------------------------

   private:
    /// Maps original integers to condensed integers.
    std::unordered_map<Int, Int> map_;

    /// Maps condensed integers to original integers.
    std::vector<Int> vector_;

    // -------------------------------
};

// -------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_ENCODING_HPP_
