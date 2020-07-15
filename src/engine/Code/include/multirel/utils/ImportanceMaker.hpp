#ifndef MULTIREL_UTILS_IMPORTANCEMAKER_HPP_
#define MULTIREL_UTILS_IMPORTANCEMAKER_HPP_

// ----------------------------------------------------------------------------

namespace multirel
{
namespace utils
{
// ------------------------------------------------------------------------

class ImportanceMaker
{
   public:
    ImportanceMaker(){};

    ~ImportanceMaker() = default;

   public:
    /// Adds a _value to the column signifed by the other input values.
    void add(
        const containers::Placeholder& _input,
        const containers::Placeholder& _output,
        const enums::DataUsed _data_used,
        const size_t _column,
        const descriptors::SameUnits& _same_units,
        const Float _value );

   public:
    /// Adds all of the colnames with importance 0.0.
    void fill_zeros(
        const containers::Placeholder& _pl, const bool _is_population )
    {
        helper_.fill_zeros( _pl, _is_population );
    }

    /// Merges the map into the existing importances.
    void merge( const std::map<std::string, Float>& _importances )
    {
        helper_.merge( _importances );
    }

    /// Multiplies all importances with the importance factor.
    void multiply( const Float _importance_factor )
    {
        helper_.multiply( _importance_factor );
    }

    /// Makes sure that all importances add up to 1.
    void normalize() { helper_.normalize(); }

    /// Trivial (const) accessor.
    std::map<std::string, Float> importances() const
    {
        return helper_.importances();
    }

   private:
    /// Adds the _value to the column signified by _name in the map.
    void add_to_importances( const std::string& _name, const Float _value )
    {
        helper_.add_to_importances( _name, _value );
    }

   private:
    /// Marks a table as peripheral.
    std::string peripheral() { return helper_.peripheral(); }

    /// Marks a table as population.
    std::string population() { return helper_.population(); }

   private:
    /// Maps the column names to the importance values.
    helpers::ImportanceMaker helper_;
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace multirel

#endif  // MULTIREL_UTILS_IMPORTANCEMAKER_HPP_
