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
    ImportanceMaker( const size_t _num_subfeatures = 0 )
        : helper_( helpers::ImportanceMaker( _num_subfeatures ) ){};

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

   public:
    /// Adds all of the colnames with importance 0.0.
    void fill_zeros(
        const containers::Placeholder& _pl,
        const std::string& _tname,
        const bool _is_population )
    {
        helper_.fill_zeros( _pl, _tname, _is_population );
    }

    /// Trivial (const) accessor
    const std::vector<Float>& importance_factors_avg() const
    {
        return helper_.importance_factors_avg();
    }

    /// Trivial (const) accessor
    const std::vector<Float>& importance_factors_sum() const
    {
        return helper_.importance_factors_sum();
    }

    /// Merges the map into the existing importances.
    void merge(
        const std::map<helpers::ColumnDescription, Float>& _importances )
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
    std::map<helpers::ColumnDescription, Float> importances() const
    {
        return helper_.importances();
    }

   private:
    /// Adds the _value to the column signified by _name in the map.
    void add_to_importances(
        const helpers::ColumnDescription& _desc, const Float _value )
    {
        helper_.add_to_importances( _desc, _value );
    }

    /// Adds the _value to the column signified by _ix in the importance
    /// factors.
    void add_to_importance_factors( const size_t _ix, const Float _value )
    {
        helper_.add_to_importance_factors( _ix, _value );
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
}  // namespace relboost

#endif  // RELBOOST_UTILS_IMPORTANCEMAKER_HPP_
