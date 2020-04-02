#ifndef TS_TIMESERIESMODEL_HPP_
#define TS_TIMESERIESMODEL_HPP_

namespace ts
{
// ----------------------------------------------------------------------------

template <class FEType>
class TimeSeriesModel
{
    // -----------------------------------------------------------------

   public:
    typedef typename FEType::DataFrameType DataFrameType;
    typedef typename FEType::DataFrameViewType DataFrameViewType;
    typedef typename FEType::FeaturesType FeaturesType;
    typedef typename FEType::FloatColumnType FloatColumnType;
    typedef Hyperparameters<typename FEType::HypType> HypType;
    typedef typename FEType::PlaceholderType PlaceholderType;

    constexpr static bool premium_only_ = FEType::premium_only_;
    constexpr static bool supports_multiple_targets_ =
        FEType::supports_multiple_targets_;

    // -----------------------------------------------------------------

   public:
    TimeSeriesModel(
        const std::shared_ptr<const std::vector<strings::String>> &_categories,
        const std::shared_ptr<const HypType> &_hyperparameters,
        const std::shared_ptr<const std::vector<std::string>> &_peripheral,
        const std::shared_ptr<const PlaceholderType> &_placeholder,
        const std::shared_ptr<const std::vector<PlaceholderType>>
            &_peripheral_schema = nullptr,
        const std::shared_ptr<const PlaceholderType> &_population_schema =
            nullptr )
        : model_(
              _categories,
              std::make_shared<typename FEType::HypType>(
                  _hyperparameters->model_hyperparams_ ),
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
        const DataFrameType &_population,
        const std::vector<DataFrameType> &_peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger =
            std::shared_ptr<const logging::AbstractLogger>() );

    /// Transforms a set of raw data into extracted features.
    FeaturesType transform(
        const DataFrameType &_population,
        const std::vector<DataFrameType> &_peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger =
            std::shared_ptr<const logging::AbstractLogger>() ) const
    {
        return model_.transform( _population, _peripheral, _logger );
    }

    // -----------------------------------------------------------------

   public:
    /// Calculates feature importances
    void feature_importances() { model_.feature_importances(); }

    /// Returns the number of features.
    size_t num_features() const { return model_.num_features(); }

    /// Trivial (const) accessor
    const std::vector<PlaceholderType> &peripheral_schema() const
    {
        return model_.peripheral_schema();
    }

    /// Trivial (const) accessor
    const PlaceholderType &population_schema() const
    {
        return model_.population_schema();
    }

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
        std::vector<FloatColumnType>,
        std::vector<std::shared_ptr<std::vector<Float>>>>
    create_modified_time_stamps(
        const std::string &_ts_name,
        const Float _lag,
        const Float _memory,
        const DataFrameType &_population ) const;

    /// Creates a new placeholder that contains the self joins.
    PlaceholderType create_placeholder(
        const PlaceholderType &_placeholder,
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
    /// layer over the actual feature engineerer.
    FEType model_;

    // -----------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace ts

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

namespace ts
{
// ----------------------------------------------------------------------------

template <class FEType>
std::pair<
    std::vector<typename FEType::FloatColumnType>,
    std::vector<std::shared_ptr<std::vector<Float>>>>
TimeSeriesModel<FEType>::create_modified_time_stamps(
    const std::string &_ts_name,
    const Float _lag,
    const Float _memory,
    const DataFrameType &_population ) const
{
    // -----------------------------------------------------------------

    if ( _lag < 0.0 )
        {
            throw std::invalid_argument( "'lag' cannot be negative!" );
        }

    if ( _memory < 0.0 )
        {
            throw std::invalid_argument( "'memory' cannot be negative!" );
        }

    // -----------------------------------------------------------------

    const auto lag_op = [_lag]( const Float val ) { return val + _lag; };

    const auto mem_op = [_lag, _memory]( const Float val ) {
        return val + _lag + _memory;
    };

    // -----------------------------------------------------------------

    size_t ix = 0;

    for ( ; ix < _population.num_time_stamps(); ++ix )
        {
            if ( _population.time_stamp_col( ix ).name_ == _ts_name )
                {
                    break;
                }
        }

    if ( _ts_name == "" )
        {
            ix = 0;
        }

    if ( _population.num_time_stamps() == 0 )
        {
            throw std::invalid_argument(
                "DataFrame '" + _population.name() + "' has no time stamps!" );
        }

    if ( _population.num_time_stamps() > 1 && _ts_name == "" )
        {
            throw std::invalid_argument(
                "DataFrame '" + _population.name() +
                "' has more than one time stamp, but no identifying time stamp "
                "has been passed!" );
        }

    if ( ix == _population.num_time_stamps() &&
         _population.num_time_stamps() > 1 )
        {
            throw std::invalid_argument(
                "DataFrame '" + _population.name() +
                "' has no time stamps named '" + _ts_name + "'!" );
        }

    const auto &ts = _population.time_stamp_col( ix );

    // -----------------------------------------------------------------

    std::vector<FloatColumnType> cols;

    std::vector<std::shared_ptr<std::vector<Float>>> data;

    // -----------------------------------------------------------------

    data.emplace_back( std::make_shared<std::vector<Float>>( ts.nrows_ ) );

    std::transform(
        ts.data_, ts.data_ + ts.nrows_, data.back()->begin(), lag_op );

    cols.emplace_back( FloatColumnType(
        data.back()->data(), ts.name_ + "$GETML_LOWER_TS", ts.nrows_, "" ) );

    // -----------------------------------------------------------------

    if ( _memory > 0.0 )
        {
            data.emplace_back(
                std::make_shared<std::vector<Float>>( ts.nrows_ ) );

            std::transform(
                ts.data_, ts.data_ + ts.nrows_, data.back()->begin(), mem_op );

            cols.emplace_back( FloatColumnType(
                data.back()->data(),
                ts.name_ + "$GETML_UPPER_TS",
                ts.nrows_,
                "" ) );
        }

    // -----------------------------------------------------------------

    return std::make_pair( cols, data );

    // -----------------------------------------------------------------
}

// -----------------------------------------------------------------------------

template <class FEType>
typename TimeSeriesModel<FEType>::PlaceholderType
TimeSeriesModel<FEType>::create_placeholder(
    const PlaceholderType &_placeholder,
    const std::vector<std::string> &_self_join_keys,
    const std::string &_lower_time_stamp_used,
    const std::string &_upper_time_stamp_used ) const
{
    // ----------------------------------------------------------

    const auto joined_table = PlaceholderType(
        _placeholder.categoricals_,
        _placeholder.discretes_,
        _placeholder.join_keys_,
        _placeholder.name_,
        _placeholder.numericals_,
        _placeholder.targets_,
        _placeholder.time_stamps_ );

    // ----------------------------------------------------------

    auto joined_tables = _placeholder.joined_tables_;

    auto join_keys_used = _placeholder.join_keys_used_;

    auto other_join_keys_used = _placeholder.other_join_keys_used_;

    auto other_time_stamps_used = _placeholder.other_time_stamps_used_;

    auto time_stamps_used = _placeholder.time_stamps_used_;

    auto upper_time_stamps_used = _placeholder.upper_time_stamps_used_;

    // ----------------------------------------------------------

    for ( size_t i = 0; i < _self_join_keys.size(); ++i )
        {
            joined_tables.push_back( joined_table );

            join_keys_used.push_back( _self_join_keys[i] );

            other_join_keys_used.push_back( _self_join_keys[i] );

            other_time_stamps_used.push_back( _lower_time_stamp_used );

            time_stamps_used.push_back( _lower_time_stamp_used );

            upper_time_stamps_used.push_back( _upper_time_stamp_used );
        }

    // ----------------------------------------------------------

    return PlaceholderType(
        joined_tables,
        join_keys_used,
        _placeholder.name(),
        other_join_keys_used,
        other_time_stamps_used,
        time_stamps_used,
        upper_time_stamps_used );

    // ----------------------------------------------------------
}

// -----------------------------------------------------------------------------

template <class FEType>
void TimeSeriesModel<FEType>::fit(
    const DataFrameType &_population,
    const std::vector<DataFrameType> &_peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger )
{
    model_.fit( _population, _peripheral, _logger );
}

// -----------------------------------------------------------------------------
// ----------------------------------------------------------------------------
}  // namespace ts

#endif  // TS_TIMESERIESMODEL_HPP_

