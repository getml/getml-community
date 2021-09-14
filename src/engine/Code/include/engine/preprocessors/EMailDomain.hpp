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
    fit_transform( const FitParams& _params ) final;

    /// Expresses the Seasonal preprocessor as a JSON object.
    Poco::JSON::Object::Ptr to_json_obj() const final;

    /// Transforms the data frames by adding the desired time series
    /// transformations.
    std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
    transform( const TransformParams& _params ) const final;

   public:
    /// Creates a deep copy.
    std::shared_ptr<Preprocessor> clone(
        const std::optional<std::vector<Poco::JSON::Object::Ptr>>&
            _dependencies = std::nullopt ) const final
    {
        auto c = std::make_shared<EMailDomain>( *this );
        if ( _dependencies )
            {
                c->dependencies_ = *_dependencies;
            }
        return c;
    }

    /// The preprocessor does not generate any SQL scripts.
    std::vector<std::string> to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories,
        const std::shared_ptr<const helpers::SQLDialectGenerator>&
            _sql_dialect_generator ) const final
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

