#ifndef ENGINE_PREPROCESSORS_IMPUTATION_HPP_
#define ENGINE_PREPROCESSORS_IMPUTATION_HPP_

namespace engine
{
namespace preprocessors
{
// ----------------------------------------------------

class Imputation : public Preprocessor
{
   private:
    typedef std::map<helpers::ColumnDescription, std::pair<Float, bool>>
        ImputationMapType;

   public:
    Imputation()
        : add_dummies_( false ), cols_( std::make_shared<ImputationMapType>() )
    {
    }

    Imputation(
        const Poco::JSON::Object& _obj,
        const std::vector<Poco::JSON::Object::Ptr>& _dependencies )
        : add_dummies_( false ), cols_( std::make_shared<ImputationMapType>() )
    {
        *this = from_json_obj( _obj );
        dependencies_ = _dependencies;
    }

    ~Imputation() = default;

   public:
    /// Returns the fingerprint of the preprocessor (necessary to build
    /// the dependency graphs).
    Poco::JSON::Object::Ptr fingerprint() const final;

    /// Identifies which features should be extracted from which time stamps.
    std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
    fit_transform(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<containers::Encoding>& _categories,
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs,
        const helpers::Placeholder& _placeholder,
        const std::vector<std::string>& _peripheral_names ) final;

    /// Expresses the Seasonal preprocessor as a JSON object.
    Poco::JSON::Object::Ptr to_json_obj() const final;

    /// Transforms the data frames by adding the desired time series
    /// transformations.
    std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
    transform(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const containers::Encoding> _categories,
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs ) const final;

   public:
    /// Creates a deep copy.
    std::shared_ptr<Preprocessor> clone() const final
    {
        return std::make_shared<Imputation>( *this );
    }

    /// The preprocessor does not generate any SQL scripts.
    std::vector<std::string> to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories )
        const final
    {
        return {};
    }

    /// Returns the type of the preprocessor.
    std::string type() const final { return Preprocessor::IMPUTATION; }

   private:
    /// Adds a dummy column that assumes the value of 1, if and only if the
    /// original column is nan.
    void add_dummy(
        const containers::Column<Float>& _original_col,
        containers::DataFrame* _df ) const;

    /// Extracts an imputed column and adds it to the data frame.
    void extract_and_add(
        const std::string& _marker,
        const size_t _table,
        const containers::Column<Float>& _original_col,
        containers::DataFrame* _df );

    /// Fits and transforms an individual data frame.
    containers::DataFrame fit_transform_df(
        const containers::DataFrame& _df,
        const std::string& _marker,
        const size_t _table );

    /// Parses a JSON object.
    Imputation from_json_obj( const Poco::JSON::Object& _obj ) const;

    /// Replaces the original column with an imputed one. Returns a boolean
    /// indicating whether any value had to be imputed.
    bool impute(
        const containers::Column<Float>& _original_col,
        const Float _imputation_value,
        containers::DataFrame* _df ) const;

    /// Retrieves all pairs in cols_ matching _marker and _table.
    std::vector<std::pair<Float, bool>> retrieve_pairs(
        const std::string& _marker, const size_t _table ) const;

    /// Transforms a single data frame.
    containers::DataFrame transform_df(
        const containers::DataFrame& _df,
        const std::string& _marker,
        const size_t _table ) const;

   private:
    /// Trivial accessor
    ImputationMapType& cols()
    {
        assert_true( cols_ );
        return *cols_;
    }

    /// Trivial accessor
    const ImputationMapType& cols() const
    {
        assert_true( cols_ );
        return *cols_;
    }

    /// Retrieve the column description of all columns in cols_.
    std::vector<std::shared_ptr<helpers::ColumnDescription>> get_all_cols()
        const
    {
        std::vector<std::shared_ptr<helpers::ColumnDescription>> all_cols;
        for ( const auto& [key, _] : cols() )
            {
                all_cols.push_back(
                    std::make_shared<helpers::ColumnDescription>( key ) );
            }
        return all_cols;
    }

    /// Generates the colname for the newly created column.
    std::string make_dummy_name( const std::string& _colname ) const
    {
        return helpers::Macros::dummy_begin() + _colname +
               helpers::Macros::dummy_end();
    }

    /// Generates the colname for the newly created column.
    std::string make_name(
        const std::string& _colname, const Float _replacement ) const
    {
        return helpers::Macros::imputation_begin() + _colname +
               helpers::Macros::imputation_replacement() +
               std::to_string( _replacement ) +
               helpers::Macros::imputation_end();
    }

   private:
    /// Whether to create dummy columns.
    bool add_dummies_;

    /// Map of all columns to which the imputation transformation applies.
    /// Maps to the mean value and whether we need to build a dummy column.
    std::shared_ptr<ImputationMapType> cols_;

    /// The dependencies inserted into the the preprocessor.
    std::vector<Poco::JSON::Object::Ptr> dependencies_;
};

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_IMPUTATION_HPP_

