#ifndef ENGINE_PREPROCESSORS_SEASONAL_HPP_
#define ENGINE_PREPROCESSORS_SEASONAL_HPP_

namespace engine
{
namespace preprocessors
{
// ----------------------------------------------------

class Seasonal : public Preprocessor
{
   private:
    static constexpr bool ADD_ZERO = true;
    static constexpr bool DONT_ADD_ZERO = false;

   public:
    Seasonal() {}

    Seasonal(
        const Poco::JSON::Object& _obj,
        const std::vector<Poco::JSON::Object::Ptr>& _dependencies )
    {
        *this = from_json_obj( _obj );
        dependencies_ = _dependencies;
    }

    ~Seasonal() = default;

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
        const std::vector<containers::DataFrame>& _peripheral_dfs ) final;

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
        return std::make_shared<Seasonal>( *this );
    }

    /// Returns the type of the preprocessor.
    std::string type() const final { return Preprocessor::SEASONAL; }

   private:
    /// Extracts the hour of a time stamp. Returns std::nullopt, if the column
    /// generates warning.
    std::optional<containers::Column<Int>> extract_hour(
        const containers::Column<Float>& _col,
        containers::Encoding* _categories ) const;

    /// Extracts the hour of a time stamp.
    containers::Column<Int> extract_hour(
        const containers::Encoding& _categories,
        const containers::Column<Float>& _col ) const;

    /// Extracts the minute of a time stamp. Returns std::nullopt, if the column
    /// generates warning.
    std::optional<containers::Column<Int>> extract_minute(
        const containers::Column<Float>& _col,
        containers::Encoding* _categories ) const;

    /// Extracts the minute of a time stamp.
    containers::Column<Int> extract_minute(
        const containers::Encoding& _categories,
        const containers::Column<Float>& _col ) const;

    /// Extracts the month of a time stamp. Returns std::nullopt, if the column
    /// generates warning.
    std::optional<containers::Column<Int>> extract_month(
        const containers::Column<Float>& _col,
        containers::Encoding* _categories ) const;

    /// Extracts the month of a time stamp.
    containers::Column<Int> extract_month(
        const containers::Encoding& _categories,
        const containers::Column<Float>& _col ) const;

    /// Extracts the weekday of a time stamp. Returns std::nullopt, if the
    /// column generates warnings.
    std::optional<containers::Column<Int>> extract_weekday(
        const containers::Column<Float>& _col,
        containers::Encoding* _categories ) const;

    /// Extracts the weekday of a time stamp.
    containers::Column<Int> extract_weekday(
        const containers::Encoding& _categories,
        const containers::Column<Float>& _col ) const;

    /// Extracts the year of a time stamp. Returns std::nullopt, if the column
    /// generates warning.
    std::optional<containers::Column<Float>> extract_year(
        const containers::Column<Float>& _col );

    /// Extracts the year of a time stamp.
    containers::Column<Float> extract_year(
        const containers::Column<Float>& _col ) const;

    /// Fits and transforms an individual data frame.
    containers::DataFrame fit_transform_df(
        const containers::DataFrame& _df,
        const std::string& _marker,
        const size_t _table,
        containers::Encoding* _categories );

    /// Parses a JSON object.
    Seasonal from_json_obj( const Poco::JSON::Object& _obj ) const;

    // Transforms a float column to a categorical column.
    containers::Column<Int> to_int(
        const containers::Column<Float>& _col,
        const bool _add_zero,
        containers::Encoding* _categories ) const;

    /// Transforms a float column to a categorical column.
    containers::Column<Int> to_int(
        const containers::Encoding& _categories,
        const bool _add_zero,
        const containers::Column<Float>& _col ) const;

    /// Transforms a single data frame.
    containers::DataFrame transform_df(
        const containers::Encoding& _categories,
        const containers::DataFrame& _df,
        const std::string& _marker,
        const size_t _table ) const;

   private:
    /// Undertakes a transformation based on the template class
    /// Operator.
    template <class Operator>
    containers::Column<Int> to_categorical(
        const containers::Column<Float>& _col,
        const bool _add_zero,
        const Operator& _op,
        containers::Encoding* _categories ) const
    {
        auto result = containers::Column<Float>( _col.nrows() );

        std::transform( _col.begin(), _col.end(), result.begin(), _op );

        return to_int( result, _add_zero, _categories );
    }

    /// Undertakes a transformation based on the template class
    /// Operator.
    template <class Operator>
    containers::Column<Int> to_categorical(
        const containers::Encoding& _categories,
        const containers::Column<Float>& _col,
        const bool _add_zero,
        const Operator& _op ) const
    {
        auto result = containers::Column<Float>( _col.nrows() );

        std::transform( _col.begin(), _col.end(), result.begin(), _op );

        return to_int( _categories, _add_zero, result );
    }

    /// Undertakes a transformation based on the template class
    /// Operator.
    template <class Operator>
    containers::Column<Float> to_numerical(
        const containers::Column<Float>& _col, const Operator& _op ) const
    {
        auto result = containers::Column<Float>( _col.nrows() );

        std::transform( _col.begin(), _col.end(), result.begin(), _op );

        return result;
    }

   private:
    /// The dependencies inserted into the the preprocessor.
    std::vector<Poco::JSON::Object::Ptr> dependencies_;

    /// List of all columns to which the hour transformation applies.
    std::vector<std::shared_ptr<helpers::ColumnDescription>> hour_;

    /// List of all columns to which the minute transformation applies.
    std::vector<std::shared_ptr<helpers::ColumnDescription>> minute_;

    /// List of all columns to which the month transformation applies.
    std::vector<std::shared_ptr<helpers::ColumnDescription>> month_;

    /// List of all columns to which the weekday transformation applies.
    std::vector<std::shared_ptr<helpers::ColumnDescription>> weekday_;

    /// List of all columns to which the year transformation applies.
    std::vector<std::shared_ptr<helpers::ColumnDescription>> year_;
};

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_SEASONAL_HPP_

