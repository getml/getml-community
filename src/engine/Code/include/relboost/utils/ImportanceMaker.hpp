#ifndef RELBOOST_UTILS_IMPORTANCEMAKER_HPP_
#define RELBOOST_UTILS_IMPORTANCEMAKER_HPP_

// ----------------------------------------------------------------------------

namespace relboost
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
        const size_t _column_input,
        const Float _value );

    /// Adds all of the colnames with importance 0.0.
    void fill_zeros(
        const containers::Placeholder& _pl, const bool _is_population );

    /// Merges the map into the existing importances.
    void merge( const std::map<std::string, Float>& _importances );

    /// Multiplies all importances with the importance factor.
    void multiply( const Float _importance_factor );

    /// Makes sure that all importances add up to 1.
    void normalize();

   public:
    /// Trivial (const) accessor.
    std::map<std::string, Float> importances() const { return importances_; }

   private:
    /// Adds the _value to the column signified by _name in the map.
    void add_to_importances( const std::string& _name, const Float _value );

    /// Adds all of the elements from this column.
    void fill_zeros_from_columns(
        const std::string& _marker,
        const std::string& _pname,
        const std::vector<std::string>& _colnames );

   private:
    /// Marks a table as peripheral.
    std::string peripheral() { return "[PERIPHERAL] "; }

    /// Marks a table as population.
    std::string population() { return "[POPULATION] "; }

   private:
    /// Maps the column names to the importance values.
    std::map<std::string, Float> importances_;
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

#endif  // RELBOOST_UTILS_IMPORTANCEMAKER_HPP_
