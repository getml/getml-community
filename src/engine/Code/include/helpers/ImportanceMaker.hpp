#ifndef HELPERS_IMPORTANCEMAKER_HPP_
#define HELPERS_IMPORTANCEMAKER_HPP_

// ----------------------------------------------------------------------------

namespace helpers
{
// ------------------------------------------------------------------------

class ImportanceMaker
{
   public:
    ImportanceMaker(){};

    ImportanceMaker( const std::map<std::string, Float>& _importances )
        : importances_( _importances ){};

    ~ImportanceMaker() = default;

   public:
    /// Adds the _value to the column signified by _name in the map.
    void add_to_importances( const std::string& _name, const Float _value );

    /// Adds all of the colnames with importance 0.0.
    void fill_zeros(
        const Placeholder& _pl,
        const std::string& _tname,
        const bool _is_population );

    /// Merges the map into the existing importances.
    void merge( const std::map<std::string, Float>& _importances );

    /// Multiplies all importances with the importance factor.
    void multiply( const Float _importance_factor );

    /// Makes sure that all importances add up to 1.
    void normalize();

    /// Transfers the value from _from to _to.
    void transfer( const std::string& _from, const std::string& _to );

   public:
    /// Returns the names of the columns.
    std::vector<std::string> colnames() const
    {
        auto names = std::vector<std::string>();
        for ( const auto& [name, _] : importances_ )
            {
                names.push_back( name );
            }
        return names;
    }

    /// Trivial (const) accessor.
    std::map<std::string, Float> importances() const { return importances_; }

    /// Marks a table as peripheral.
    std::string peripheral() const { return "[PERIPHERAL] "; }

    /// Marks a table as population.
    std::string population() const { return "[POPULATION] "; }

   private:
    /// Adds all of the elements from this column.
    void fill_zeros_from_columns(
        const std::string& _marker,
        const std::string& _tname,
        const std::vector<std::string>& _colnames );

   private:
    /// Maps the column names to the importance values.
    std::map<std::string, Float> importances_;
};

// ------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_IMPORTANCEMAKER_HPP_
