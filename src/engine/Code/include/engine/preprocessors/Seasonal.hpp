#ifndef ENGINE_PREPROCESSORS_SEASONAL_HPP_
#define ENGINE_PREPROCESSORS_SEASONAL_HPP_

namespace engine
{
namespace preprocessors
{
// ----------------------------------------------------

class Seasonal : public Preprocessor
{
   public:
    Seasonal( const std::shared_ptr<containers::Encoding>& _categories )
        : categories_( _categories )
    {
    }

    Seasonal(
        const std::shared_ptr<containers::Encoding>& _categories,
        const Poco::JSON::Object& _obj )
        : categories_( _categories )
    {
        *this = from_json_obj( _obj );
    }

    ~Seasonal() = default;

   public:
    /// Expresses the Seasonal preprocessor as a JSON object.
    Poco::JSON::Object::Ptr to_json_obj() const final;

    /// Identifies which features should be extracted from which time stamps.
    void fit_transform(
        const Poco::JSON::Object& _cmd,
        std::map<std::string, containers::DataFrame>* _data_frames ) final;

    /// Transforms the data frames by adding the desired time series
    /// transformations.
    void transform(
        const Poco::JSON::Object& _cmd,
        std::map<std::string, containers::DataFrame>* _data_frames )
        const final;

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
        const containers::Column<Float>& _col );

    /// Extracts the hour of a time stamp.
    containers::Column<Int> extract_hour(
        const containers::Column<Float>& _col ) const;

    /// Extracts the minute of a time stamp. Returns std::nullopt, if the column
    /// generates warning.
    std::optional<containers::Column<Int>> extract_minute(
        const containers::Column<Float>& _col );

    /// Extracts the minute of a time stamp.
    containers::Column<Int> extract_minute(
        const containers::Column<Float>& _col ) const;

    /// Extracts the month of a time stamp. Returns std::nullopt, if the column
    /// generates warning.
    std::optional<containers::Column<Int>> extract_month(
        const containers::Column<Float>& _col );

    /// Extracts the month of a time stamp.
    containers::Column<Int> extract_month(
        const containers::Column<Float>& _col ) const;

    /// Extracts the weekday of a time stamp. Returns std::nullopt, if the
    /// column generates warnings.
    std::optional<containers::Column<Int>> extract_weekday(
        const containers::Column<Float>& _col );

    /// Extracts the weekday of a time stamp.
    containers::Column<Int> extract_weekday(
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
        const size_t _table );

    /// Parses a JSON object.
    Seasonal from_json_obj( const Poco::JSON::Object& _obj ) const;

    // Transforms a float column to a categorical column.
    containers::Column<Int> to_int( const containers::Column<Float>& _col );

    /// Transforms a float column to a categorical column.
    containers::Column<Int> to_int(
        const containers::Column<Float>& _col ) const;

    /// Transforms a single data frame.
    containers::DataFrame transform_df(
        const containers::DataFrame& _df,
        const std::string& _marker,
        const size_t _table ) const;

   private:
    /// Trivial (private) accessor.
    containers::Encoding& categories()
    {
        assert_true( categories_ );
        return *categories_;
    }

    /// Trivial (private) accessor.
    const containers::Encoding& categories() const
    {
        assert_true( categories_ );
        return *categories_;
    }

    /// Undertakes a transformation based on the template class
    /// Operator.
    template <class Operator>
    containers::Column<Int> to_categorical(
        const containers::Column<Float>& _col, const Operator& _op )
    {
        auto result = containers::Column<Float>( _col.nrows() );

        std::transform( _col.begin(), _col.end(), result.begin(), _op );

        return to_int( result );
    }

    /// Undertakes a transformation based on the template class
    /// Operator.
    template <class Operator>
    containers::Column<Int> to_categorical(
        const containers::Column<Float>& _col, const Operator& _op ) const
    {
        auto result = containers::Column<Float>( _col.nrows() );

        std::transform( _col.begin(), _col.end(), result.begin(), _op );

        return to_int( result );
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
    /// The encoding used for the categories.
    std::shared_ptr<containers::Encoding> categories_;

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

