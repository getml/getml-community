#ifndef ENGINE_FEATURELEARNERS_ABSTRACTFEATURELEARNER_HPP_
#define ENGINE_FEATURELEARNERS_ABSTRACTFEATURELEARNER_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace featurelearners
{
// ----------------------------------------------------------------------------

class AbstractFeatureLearner
{
    // --------------------------------------------------------

   public:
    static constexpr Int USE_ALL_TARGETS = -1;
    static constexpr Int IGNORE_TARGETS = -2;

    static constexpr const char* FASTPROP_MODEL = "FastPropModel";
    static constexpr const char* FASTPROP_TIME_SERIES = "FastPropTimeSeries";
    static constexpr const char* MULTIREL_MODEL = "MultirelModel";
    static constexpr const char* MULTIREL_TIME_SERIES = "MultirelTimeSeries";
    static constexpr const char* RELBOOST_MODEL = "RelboostModel";
    static constexpr const char* RELMT_MODEL = "RelMTModel";
    static constexpr const char* RELBOOST_TIME_SERIES = "RelboostTimeSeries";
    static constexpr const char* RELMT_TIME_SERIES = "RelMTTimeSeries";

    // --------------------------------------------------------

   public:
    AbstractFeatureLearner() {}

    virtual ~AbstractFeatureLearner() = default;

    // --------------------------------------------------------

   public:
    /// Calculates the column importances for this ensemble.
    virtual std::map<helpers::ColumnDescription, Float> column_importances(
        const std::vector<Float>& _importance_factors ) const = 0;

    /// Creates a deep copy.
    virtual std::shared_ptr<AbstractFeatureLearner> clone() const = 0;

    /// Returns the fingerprint of the feature learner (necessary to build
    /// the dependency graphs).
    virtual Poco::JSON::Object::Ptr fingerprint() const = 0;

    /// Fits the model.
    virtual void fit(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const communication::SocketLogger>& _logger,
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs ) = 0;

    /// Whether this is a classification problem.
    virtual bool is_classification() const = 0;

    /// Whether this is a time series model (based on a self-join).
    virtual bool is_time_series() const = 0;

    /// Loads the feature learner from a file designated by _fname.
    virtual void load( const std::string& _fname ) = 0;

    /// Returns the placeholder not as passed by the user, but as seen by the
    /// feature learner (the difference matters for time series).
    virtual helpers::Placeholder make_placeholder() const = 0;

    /// Data frames might have to be modified, such as adding upper time stamps
    /// or self joins.
    virtual std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
    modify_data_frames(
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_df ) const = 0;

    /// Returns the number of features in the feature learner.
    virtual size_t num_features() const = 0;

    /// Whether the feature learner is for the premium version only.
    virtual bool premium_only() const = 0;

    /// Saves the Model in JSON format, if applicable
    virtual void save( const std::string& _fname ) const = 0;

    /// Whether the feature learner is to be silent.
    virtual bool silent() const = 0;

    /// Whether the feature learner supports multiple targets.
    virtual bool supports_multiple_targets() const = 0;

    /// Return model as a JSON Object.
    virtual Poco::JSON::Object to_json_obj( const bool _schema_only ) const = 0;

    /// Return features as SQL code.
    virtual std::vector<std::string> to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories,
        const bool _targets,
        const bool _subfeatures,
        const std::string& _prefix ) const = 0;

    /// Generate features.
    virtual containers::Features transform(
        const Poco::JSON::Object& _cmd,
        const std::vector<size_t>& _index,
        const std::shared_ptr<const communication::SocketLogger>& _logger,
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs ) const = 0;

    /// Returns a string describing the type of the feature learner.
    virtual std::string type() const = 0;

    // --------------------------------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace featurelearners
}  // namespace engine

// ----------------------------------------------------------------------------

#endif  // ENGINE_FEATURELEARNERS_ABSTRACTFEATURELEARNER_HPP_

