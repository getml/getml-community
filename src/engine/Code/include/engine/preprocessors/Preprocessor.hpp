#ifndef ENGINE_PREPROCESSORS_PREPROCESSOR_HPP_
#define ENGINE_PREPROCESSORS_PREPROCESSOR_HPP_

namespace engine
{
namespace preprocessors
{
// ----------------------------------------------------

class Preprocessor
{
   public:
    static constexpr const char* EMAILDOMAIN = "EmailDomain";
    static constexpr const char* IMPUTATION = "Imputation";
    static constexpr const char* MAPPING = "Mapping";
    static constexpr const char* SEASONAL = "Seasonal";
    static constexpr const char* SUBSTRING = "Substring";
    static constexpr const char* TEXT_FIELD_SPLITTER = "TextFieldSplitter";

   public:
    Preprocessor(){};

    virtual ~Preprocessor() = default;

   public:
    /// Returns a deep copy.
    virtual std::shared_ptr<Preprocessor> clone() const = 0;

    /// Returns the fingerprint of the feature learner (necessary to build
    /// the dependency graphs).
    virtual Poco::JSON::Object::Ptr fingerprint() const = 0;

    /// Fits the preprocessor. Returns the transformed data frames.
    virtual std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
    fit_transform(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<containers::Encoding>& _categories,
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs ) = 0;

    /// Generates the new column.
    virtual std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
    transform(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const containers::Encoding> _categories,
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs ) const = 0;

    /// Expresses the preprocessor as a JSON object.
    virtual Poco::JSON::Object::Ptr to_json_obj() const = 0;

    /// Expresses the preprocessor as SQL, if applicable.
    virtual std::vector<std::string> to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories )
        const = 0;

    /// Returns the type of the preprocessor.
    virtual std::string type() const = 0;
};

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_PREPROCESSOR_HPP_

