#ifndef ENGINE_PREPROCESSORS_EMAILDOMAIN_HPP_
#define ENGINE_PREPROCESSORS_EMAILDOMAIN_HPP_

namespace engine
{
namespace preprocessors
{
// ----------------------------------------------------

class EMailDomain : public Preprocessor
{
   public:
    EMailDomain() {}

    EMailDomain(
        const Poco::JSON::Object& _obj,
        const std::vector<Poco::JSON::Object::Ptr>& _dependencies )
    {
        *this = from_json_obj( _obj );
        dependencies_ = _dependencies;
    }

    ~EMailDomain() = default;

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
        const std::vector<containers::DataFrame>& _peripheral_dfs,
        const helpers::Placeholder& _placeholder,
        const std::vector<std::string>& _peripheral_names ) const final;

   public:
    /// Creates a deep copy.
    std::shared_ptr<Preprocessor> clone() const final
    {
        return std::make_shared<EMailDomain>( *this );
    }

    /// The preprocessor does not generate any SQL scripts.
    std::vector<std::string> to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories )
        const final
    {
        return {};
    }

    /// Returns the type of the preprocessor.
    std::string type() const final { return Preprocessor::EMAILDOMAIN; }

   private:
    /// Extracts the domain during fitting.
    std::optional<containers::Column<Int>> extract_domain(
        const containers::Column<strings::String>& _col,
        containers::Encoding* _categories ) const;

    /// Extracts the domain during transformation.
    containers::Column<Int> extract_domain(
        const containers::Encoding& _categories,
        const containers::Column<strings::String>& _col ) const;

    /// Extracts the domain as a string.
    containers::Column<strings::String> extract_domain_string(
        const containers::Column<strings::String>& _col ) const;

    /// Fits and transforms an individual data frame.
    containers::DataFrame fit_transform_df(
        const containers::DataFrame& _df,
        const std::string& _marker,
        const size_t _table,
        containers::Encoding* _categories );

    /// Parses a JSON object.
    EMailDomain from_json_obj( const Poco::JSON::Object& _obj ) const;

    /// Transforms a single data frame.
    containers::DataFrame transform_df(
        const containers::Encoding& _categories,
        const containers::DataFrame& _df,
        const std::string& _marker,
        const size_t _table ) const;

   private:
    /// Generates the name for the newly created column.
    std::string make_name( const std::string& _colname ) const
    {
        return helpers::Macros::email_domain_begin() + _colname +
               helpers::Macros::email_domain_end();
    }

   private:
    /// List of all columns to which the email domain transformation applies.
    std::vector<std::shared_ptr<helpers::ColumnDescription>> cols_;

    /// The dependencies inserted into the the preprocessor.
    std::vector<Poco::JSON::Object::Ptr> dependencies_;
};

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_EMAILDOMAIN_HPP_

