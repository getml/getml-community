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
    static constexpr const char* SEASONAL = "Seasonal";

   public:
    Preprocessor(){};

    virtual ~Preprocessor() = default;

   public:
    /// Returns a deep copy.
    virtual std::shared_ptr<Preprocessor> clone() const = 0;

    /// Fits the preprocessor. Returns the transformed data frames.
    virtual void fit_transform(
        const Poco::JSON::Object& _cmd,
        std::map<std::string, containers::DataFrame>* _data_frames ) = 0;

    /// Generates the new column.
    virtual void transform(
        const Poco::JSON::Object& _cmd,
        std::map<std::string, containers::DataFrame>* _data_frames ) const = 0;

    /// Expresses the preprocessor as a JSON object.
    virtual Poco::JSON::Object::Ptr to_json_obj() const = 0;

    /// Returns the type of the preprocessor.
    virtual std::string type() const = 0;
};

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_PREPROCESSOR_HPP_

