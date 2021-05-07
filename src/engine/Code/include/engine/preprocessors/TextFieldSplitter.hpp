#ifndef ENGINE_PREPROCESSORS_TEXTFIELDSPLITTER_HPP_
#define ENGINE_PREPROCESSORS_TEXTFIELDSPLITTER_HPP_

namespace engine
{
namespace preprocessors
{
// ----------------------------------------------------

class TextFieldSplitter : public Preprocessor
{
   public:
    TextFieldSplitter() {}

    TextFieldSplitter(
        const Poco::JSON::Object& _obj,
        const std::vector<Poco::JSON::Object::Ptr>& _dependencies )
    {
        *this = from_json_obj( _obj );
        dependencies_ = _dependencies;
    }

    ~TextFieldSplitter() = default;

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

    /// Generates SQL code for the text field splitting.
    std::vector<std::string> to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories )
        const final;

    /// Transforms the data frames by adding the desired time series
    /// transformations.
    std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
    transform(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const containers::Encoding> _categories,
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs,
        const helpers::Placeholder& _placeholder,
        const std::vector<std::string>& _peripheral_names ) const final;

   public:
    /// Creates a deep copy.
    std::shared_ptr<Preprocessor> clone(
        const std::optional<std::vector<Poco::JSON::Object::Ptr>>&
            _dependencies = std::nullopt ) const final
    {
        const auto c = std::make_shared<TextFieldSplitter>( *this );
        if ( _dependencies )
            {
                c->dependencies_ = *_dependencies;
            }
        return c;
    }

    /// Returns the type of the preprocessor.
    std::string type() const final { return Preprocessor::TEXT_FIELD_SPLITTER; }

   private:
    /// Adds a rowid to the data frame.
    containers::DataFrame add_rowid( const containers::DataFrame& _df ) const;

    /// Fits and transforms an individual data frame.
    std::vector<std::shared_ptr<helpers::ColumnDescription>> fit_df(
        const containers::DataFrame& _df, const std::string& _marker ) const;

    /// Parses a JSON object.
    TextFieldSplitter from_json_obj( const Poco::JSON::Object& _obj ) const;

    /// Generates a new data frame.
    containers::DataFrame make_new_df(
        const std::string& _df_name,
        const containers::Column<strings::String>& _col ) const;

    /// Removes the text fields from the data frame.
    containers::DataFrame remove_text_fields(
        const containers::DataFrame& _df ) const;

    /// Splits the text fields on a particular column.
    std::pair<containers::Column<Int>, containers::Column<strings::String>>
    split_text_fields_on_col(
        const containers::Column<strings::String>& _col ) const;

    /// Transforms a single data frame.
    void transform_df(
        const std::string& _marker,
        const containers::DataFrame& _df,
        std::vector<containers::DataFrame>* _peripheral_dfs ) const;

   private:
    /// List of all columns to which the email domain transformation
    /// applies.
    std::vector<std::shared_ptr<helpers::ColumnDescription>> cols_;

    /// The dependencies inserted into the the preprocessor.
    std::vector<Poco::JSON::Object::Ptr> dependencies_;
};

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_TEXTFIELDSPLITTER_HPP_

