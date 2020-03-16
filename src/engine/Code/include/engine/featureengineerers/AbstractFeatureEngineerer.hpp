#ifndef ENGINE_FEATUREENGINEERERS_ABSTRACTFEATUREENGINEERER_HPP_
#define ENGINE_FEATUREENGINEERERS_ABSTRACTFEATUREENGINEERER_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace featureengineerers
{
// ----------------------------------------------------------------------------

class AbstractFeatureEngineerer
{
    // --------------------------------------------------------

   public:
    AbstractFeatureEngineerer() {}

    virtual ~AbstractFeatureEngineerer() = default;

    // --------------------------------------------------------

   public:
    /// Fits the model.
    virtual void fit(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        Poco::Net::StreamSocket* _socket ) = 0;

    /// Loads the feature engineerer from a file designated by _fname.
    virtual void load( const std::string& _fname ) = 0;

    /// Returns the number of features in the feature engineerer.
    virtual size_t num_features() const = 0;

    /// Whether the feature engineerer is for the premium version only.
    virtual bool premium_only() const = 0;

    /// Saves the Model in JSON format, if applicable
    virtual void save( const std::string& _fname ) const = 0;

    /// Selects the features according to the index given.
    virtual void select_features( const std::vector<size_t>& _index ) = 0;

    /// Whether the feature engineerer supports multiple targets.
    virtual bool supports_multiple_targets() const = 0;

    /// Return model as a JSON Object.
    virtual Poco::JSON::Object to_json_obj( const bool _schema_only ) const = 0;

    /// Returns model as a JSON Object in a form that the monitor can
    /// understand.
    virtual Poco::JSON::Object to_monitor( const std::string& _name ) const = 0;

    /// Return feature engineerer as SQL code.
    virtual std::string to_sql( const size_t _offset ) const = 0;

    /// Generate features.
    virtual containers::Features transform(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        Poco::Net::StreamSocket* _socket ) const = 0;

    // --------------------------------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace featureengineerers
}  // namespace engine

// ----------------------------------------------------------------------------

#endif  // ENGINE_FEATUREENGINEERERS_ABSTRACTFEATUREENGINEERER_HPP_

