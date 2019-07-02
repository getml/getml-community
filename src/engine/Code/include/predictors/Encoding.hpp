#ifndef PREDICTORS_ENCODING_HPP_
#define PREDICTORS_ENCODING_HPP_

namespace predictors
{
// -------------------------------------------------------------------------

class Encoding
{
   public:
    Encoding() : max_( 1 ), min_( 0 ) {}

    Encoding( const Poco::JSON::Object& _obj )
        : max_( JSON::get_value<Int>( _obj, "max_" ) ),
          min_( JSON::get_value<Int>( _obj, "min_" ) )
    {
    }

    ~Encoding() = default;

    // -------------------------------

    /// Adds column to map or vector, returning the transformed column.
    void fit( const CIntColumn& _val );

    /// Transform Encoding to JSON object.
    Poco::JSON::Object to_json_obj() const;

    /// Transforms the column to the mapped integers.
    CIntColumn transform( const CIntColumn& _val ) const;

    // -------------------------------

    /// Size means the number of elements in this column.
    Int n_unique() const
    {
        if ( min_ > max_ )
            {
                throw std::runtime_error( "Encoding has not been fitted!" );
            }

        return max_ - min_ + 1;
    }

    // -------------------------------

   private:
    /// Maximum integer found.
    Int max_;

    /// Minimum integer found.
    Int min_;

    // -------------------------------
};

// -------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_ENCODING_HPP_
