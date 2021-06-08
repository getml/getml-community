#ifndef HELPERS_IMPORTANCEMAKER_HPP_
#define HELPERS_IMPORTANCEMAKER_HPP_

// ----------------------------------------------------------------------------

namespace helpers
{
// ------------------------------------------------------------------------

class ImportanceMaker
{
   public:
    ImportanceMaker( const size_t _num_subfeatures = 0 )
        : importance_factors_avg_( std::vector<Float>( _num_subfeatures ) ),
          importance_factors_sum_( std::vector<Float>( _num_subfeatures ) ){};

    ImportanceMaker(
        const std::map<ColumnDescription, Float>& _importances,
        const size_t _num_subfeatures = 0 )
        : ImportanceMaker( _num_subfeatures )
    {
        importances_ = _importances;
    };

    ~ImportanceMaker() = default;

   public:
    /// Adds the _value to the column signified by _desc in the map.
    void add_to_importances(
        const ColumnDescription& _desc, const Float _value );

    /// Adds the _value to the column signified by _ix in the importance
    /// factors.
    void add_to_importance_factors( const size_t _ix, const Float _value );

    /// Retrieves the fast prop importances and deletes the corresponding
    /// entries.
    std::vector<Float> retrieve_fast_prop(
        const std::vector<ColumnDescription>& _fast_prop_descs );

    /// Adds all of the colnames with importance 0.0.
    void fill_zeros(
        const Schema& _pl,
        const std::string& _tname,
        const bool _is_population );

    /// Merges the map into the existing importances.
    void merge( const std::map<ColumnDescription, Float>& _importances );

    /// Multiplies all importances with the importance factor.
    void multiply( const Float _importance_factor );

    /// Makes sure that all importances add up to 1.
    void normalize();

    /// Transfers the value from _from to _to.
    void transfer(
        const ColumnDescription& _from, const ColumnDescription& _to );

    /// Transfers all importance values marked population to an equivalent value
    /// marked peripheral.
    void transfer_population();

   public:
    /// Returns the names of the columns.
    std::vector<std::string> colnames() const
    {
        auto names = std::vector<std::string>();
        for ( const auto& [desc, _] : importances_ )
            {
                names.push_back( desc.name() );
            }
        return names;
    }

    /// Trivial (const) accessor.
    const std::map<ColumnDescription, Float>& importances() const
    {
        return importances_;
    }

    /// Trivial (const) accessor.
    const std::vector<Float>& importance_factors_avg() const
    {
        return importance_factors_avg_;
    }

    /// Trivial (const) accessor.
    const std::vector<Float>& importance_factors_sum() const
    {
        return importance_factors_sum_;
    }

    /// Marks a table as peripheral.
    std::string peripheral() const { return ColumnDescription::PERIPHERAL; }

    /// Marks a table as population.
    std::string population() const { return ColumnDescription::POPULATION; }

   private:
    /// Adds the _value to fast_prop importance factors.
    void add_to_fast_prop( const ColumnDescription& _desc, const Float _value );

    /// Adds all of the elements from this column.
    void fill_zeros_from_columns(
        const std::string& _marker,
        const std::string& _tname,
        const std::vector<std::string>& _colnames );

   private:
    /// The importance factors for the AVG subfeatures.
    std::vector<Float> importance_factors_avg_;

    /// The importance factors for the SUM subfeatures.
    std::vector<Float> importance_factors_sum_;

    /// Maps the column names to the importance values.
    std::map<ColumnDescription, Float> importances_;
};

// ------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_IMPORTANCEMAKER_HPP_
