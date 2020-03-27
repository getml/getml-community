#ifndef MULTIREL_TIMESERIES_TIMESERIESMODEL_HPP_
#define MULTIREL_TIMESERIES_TIMESERIESMODEL_HPP_

namespace multirel
{
namespace timeseries
{
// ----------------------------------------------------------------------------

class TimeSeriesModel
{
    // -----------------------------------------------------------------

   public:
    typedef multirel::containers::DataFrame DataFrameType;
    typedef multirel::containers::DataFrameView DataFrameViewType;

    constexpr static bool premium_only_ = false;
    constexpr static bool supports_multiple_targets_ = true;

    // -----------------------------------------------------------------

   public:
    TimeSeriesModel(
        const std::shared_ptr<const std::vector<strings::String>> &_categories,
        const std::shared_ptr<const descriptors::Hyperparameters>
            &_hyperparameters,
        const std::shared_ptr<const std::vector<std::string>> &_peripheral,
        const std::shared_ptr<const containers::Placeholder> &_placeholder,
        const std::shared_ptr<const std::vector<containers::Placeholder>>
            &_peripheral_schema = nullptr,
        const std::shared_ptr<const containers::Placeholder>
            &_population_schema = nullptr )
        : model_(
              _categories,
              _hyperparameters,
              _peripheral,
              _placeholder,
              _peripheral_schema,
              _population_schema )
    {
    }

    TimeSeriesModel(
        const std::shared_ptr<const std::vector<strings::String>> &_categories,
        const Poco::JSON::Object &_obj )
        : model_( _categories, _obj )
    {
    }

    ~TimeSeriesModel() = default;

    // -----------------------------------------------------------------

   public:
    /// Fits the time series model.
    void fit(
        const containers::DataFrame &_population,
        const std::vector<containers::DataFrame> &_peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger =
            std::shared_ptr<const logging::AbstractLogger>() );

    /// Transforms a set of raw data into extracted features.
    containers::Features transform(
        const containers::DataFrame &_population,
        const std::vector<containers::DataFrame> &_peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger =
            std::shared_ptr<const logging::AbstractLogger>() ) const
    {
        return model_.transform( _population, _peripheral, _logger );
    }

    // -----------------------------------------------------------------

   public:
    /// Calculates feature importances
    void feature_importances() { model_.feature_importances(); }

    /// Saves the Model in JSON format, if applicable
    void save( const std::string &_fname ) const { model_.save( _fname ); }

    /// Selects the features according to the index given.
    void select_features( const std::vector<size_t> &_index )
    {
        model_.select_features( _index );
    }

    /// Extracts the ensemble as a Poco::JSON object
    Poco::JSON::Object to_json_obj( const bool _schema_only = false ) const
    {
        return model_.to_json_obj( _schema_only );
    }

    /// Extracts the ensemble as a Boost property tree the monitor process can
    /// understand
    Poco::JSON::Object to_monitor( const std::string _name ) const
    {
        return model_.to_monitor( _name );
    }

    /// Expresses DecisionTreeEnsemble as SQL code.
    std::vector<std::string> to_sql(
        const std::string &_feature_prefix = "",
        const size_t _offset = 0,
        const bool _subfeatures = true ) const
    {
        return model_.to_sql( _feature_prefix, _offset, _subfeatures );
    }

    // -----------------------------------------------------------------

   private:
    /// This lags the time stamps, which is necessary to prevent easter eggs.
    std::pair<
        std::vector<containers::Column<Float>>,
        std::vector<std::shared_ptr<std::vector<Float>>>>
    create_modified_time_stamps(
        const std::string &_ts_name,
        const Float _lag,
        const Float _memory,
        const containers::DataFrame &_population ) const;

    /// Creates a new placeholder that contains the self joins.
    containers::Placeholder create_placeholder(
        const containers::Placeholder &_placeholder,
        const std::vector<std::string> &_self_join_keys,
        const std::string &_lower_time_stamp_used,
        const std::string &_upper_time_stamp_used ) const;

    // -----------------------------------------------------------------

   public:
    /// Trivial accessor
    bool &allow_http() { return model_.allow_http(); }

    /// Trivial accessor
    bool allow_http() const { return model_.allow_http(); }

    // -----------------------------------------------------------------

   private:
    /// The underlying model - TimeSeriesModel is just a thin
    /// layer over a MultirelModel.
    multirel::ensemble::DecisionTreeEnsemble model_;

    // -----------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace timeseries
}  // namespace multirel

#endif  // MULTIREL_TIMESERIES_TIMESERIESMODEL_HPP_
