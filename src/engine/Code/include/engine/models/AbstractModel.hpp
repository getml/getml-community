#ifndef ENGINE_MODELS_ABSTRACTMODEL_HPP_
#define ENGINE_MODELS_ABSTRACTMODEL_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace models
{
// ----------------------------------------------------------------------------

class AbstractModel
{
    // --------------------------------------------------------

   public:
    AbstractModel() {}

    ~AbstractModel() = default;

    // --------------------------------------------------------

   public:
    /// Fits the model.
    virtual void fit(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        Poco::Net::StreamSocket* _socket ) = 0;

    /// Saves the model.
    virtual void save( const std::string& _fname ) const = 0;

    /// Score predictions.
    virtual Poco::JSON::Object score(
        const Poco::JSON::Object& _cmd, Poco::Net::StreamSocket* _socket ) = 0;

    /// Return model as JSON Object.
    virtual Poco::JSON::Object to_json_obj() const = 0;

    /// Returns model as JSON Object in a form that the monitor can understand.
    virtual Poco::JSON::Object to_monitor( const std::string& _name ) const = 0;

    /// Return feature engineerer as SQL code.
    virtual std::string to_sql() const = 0;

    /// Generate features.
    virtual containers::Matrix<Float> transform(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        Poco::Net::StreamSocket* _socket ) = 0;

    // --------------------------------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace models
}  // namespace engine

// ----------------------------------------------------------------------------

#endif  // ENGINE_MODELS_ABSTRACTMODEL_HPP_
