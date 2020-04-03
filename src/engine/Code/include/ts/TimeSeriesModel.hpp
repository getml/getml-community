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
    typedef Hyperparameters<typename FEType::HypType> HypType;
    typedef typename FEType::PlaceholderType PlaceholderType;

    typedef typename FEType::FloatColumnType FloatColumnType;
    typedef typename FEType::IntColumnType IntColumnType;

    constexpr static bool is_time_series_ = true;
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
            nullptr );

    TimeSeriesModel(
        const std::shared_ptr<const std::vector<strings::String>> &_categories,
        const Poco::JSON::Object &_obj );

    ~TimeSeriesModel();

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
            std::shared_ptr<const logging::AbstractLogger>() ) const;

    // -----------------------------------------------------------------

   public:
    /// Returns the number of features.
    size_t num_features() const { return model().num_features(); }

    /// Trivial (const) accessor
    const std::vector<PlaceholderType> &peripheral_schema() const
    {
        return model().peripheral_schema();
    }

    /// Trivial (const) accessor
    const PlaceholderType &population_schema() const
    {
        return model().population_schema();
    }

    /// Saves the Model in JSON format, if applicable
    void save( const std::string &_fname ) const { model().save( _fname ); }

    /// Selects the features according to the index given.
    /// TODO: Remove.
    void select_features( const std::vector<size_t> &_index )
    {
        model().select_features( _index );
    }

    /// Extracts the ensemble as a Poco::JSON object
    Poco::JSON::Object to_json_obj( const bool _schema_only = false ) const
    {
        return model().to_json_obj( _schema_only );
    }

    /// Extracts the ensemble as a Boost property tree the monitor process can
    /// understand
    Poco::JSON::Object to_monitor( const std::string _name ) const
    {
        return model().to_monitor( _name );
    }

    /// Expresses DecisionTreeEnsemble as SQL code.
    std::vector<std::string> to_sql(
        const std::string &_feature_prefix = "",
        const size_t _offset = 0,
        const bool _subfeatures = true ) const
    {
        return model().to_sql( _feature_prefix, _offset, _subfeatures );
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

    /// Creates a modified version of the peripheral tables that includes a
    /// modified version of the population table.
    std::pair<
        std::vector<DataFrameType>,
        std::vector<std::shared_ptr<std::vector<Float>>>>
    create_peripheral(
        const DataFrameType &_population,
        const std::vector<DataFrameType> &_peripheral ) const;

    /// Creates a new placeholder that contains the self joins.
    std::shared_ptr<PlaceholderType> create_placeholder(
        const PlaceholderType &_placeholder,
        const std::vector<std::string> &_self_join_keys,
        const std::string &_ts_name,
        const std::string &_lower_time_stamp_used,
        const std::string &_upper_time_stamp_used ) const;

    /// Creates a modified version of the population table that contains an
    /// additional join key, if necessary. Otherwise, it just returns the
    /// original population table.
    std::pair<typename FEType::DataFrameType, std::shared_ptr<std::vector<Int>>>
    create_population( const DataFrameType &_population ) const;

    // -----------------------------------------------------------------

   public:
    /// Trivial accessor
    bool &allow_http() { return model().allow_http(); }

    /// Trivial accessor
    bool allow_http() const { return model().allow_http(); }

    // -----------------------------------------------------------------

   private:
    /// Trivial (const) accessor
    const HypType &hyperparameters() const
    {
        assert_true( hyperparameters_ );
        return *hyperparameters_;
    }

    /// Trivial accessor
    FEType &model()
    {
        assert_true( model_ );
        return *model_;
    }

    /// Trivial (const) accessor
    const FEType &model() const
    {
        assert_true( model_ );
        return *model_;
    }

    // -----------------------------------------------------------------

   private:
    /// The hyperparameters underlying thie model.
    std::shared_ptr<const HypType> hyperparameters_;

    /// The underlying model - TimeSeriesModel is just a thin
    /// layer over the actual feature engineerer.
    std::optional<FEType> model_;

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
TimeSeriesModel<FEType>::TimeSeriesModel(
    const std::shared_ptr<const std::vector<strings::String>> &_categories,
    const std::shared_ptr<const HypType> &_hyperparameters,
    const std::shared_ptr<const std::vector<std::string>> &_peripheral,
    const std::shared_ptr<const PlaceholderType> &_placeholder,
    const std::shared_ptr<const std::vector<PlaceholderType>>
        &_peripheral_schema,
    const std::shared_ptr<const PlaceholderType> &_population_schema )
    : hyperparameters_( _hyperparameters )
{
    // --------------------------------------------------------------------

    assert_true( hyperparameters().model_hyperparams_ );
    assert_true( _placeholder );
    assert_true( _peripheral );

    // --------------------------------------------------------------------

    auto self_join_keys = hyperparameters().self_join_keys_;

    if ( self_join_keys.size() == 0 )
        {
            self_join_keys.push_back( "$GETML_SELF_JOIN_KEY" );
        }

    // --------------------------------------------------------------------

    const auto ts_name = hyperparameters().ts_name_ == ""
                             ? "$GETML_TIME_STAMP_USED"
                             : hyperparameters().ts_name_;

    const auto lower_ts_name = ts_name + "$GETML_LOWER_TS";

    const auto upper_ts_name = hyperparameters().memory_ > 0.0
                                   ? ts_name + "$GETML_UPPER_TS"
                                   : std::string( "" );

    // --------------------------------------------------------------------

    const auto new_placeholder = create_placeholder(
        *_placeholder, self_join_keys, ts_name, lower_ts_name, upper_ts_name );

    const auto new_peripheral =
        std::make_shared<std::vector<std::string>>( *_peripheral );

    new_peripheral->push_back( _placeholder->name() + "$GETML_PERIPHERAL" );

    // --------------------------------------------------------------------

    model_ = std::make_optional<FEType>(
        _categories,
        hyperparameters().model_hyperparams_,
        new_peripheral,
        new_placeholder,
        _peripheral_schema,
        _population_schema );

    // --------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <class FEType>
TimeSeriesModel<FEType>::TimeSeriesModel(
    const std::shared_ptr<const std::vector<strings::String>> &_categories,
    const Poco::JSON::Object &_obj )
    : hyperparameters_( std::make_shared<const HypType>(
          *jsonutils::JSON::get_object( _obj, "hyperparameters_" ) ) ),
      model_( std::make_optional<FEType>( _categories, _obj ) )
{
    assert_true( hyperparameters().model_hyperparams_ );
}

// ----------------------------------------------------------------------------

template <class FEType>
TimeSeriesModel<FEType>::~TimeSeriesModel() = default;

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

    for ( ; ix < _population.time_stamps_.size(); ++ix )
        {
            if ( _population.time_stamps_[ix].name_ == _ts_name )
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

    assert_true( ix < _population.time_stamps_.size() );

    const auto &ts = _population.time_stamps_[ix];

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
std::pair<
    std::vector<typename FEType::DataFrameType>,
    std::vector<std::shared_ptr<std::vector<Float>>>>
TimeSeriesModel<FEType>::create_peripheral(
    const DataFrameType &_population,
    const std::vector<DataFrameType> &_peripheral ) const
{
    const auto [ts_cols, ts_data] = create_modified_time_stamps(
        hyperparameters().ts_name_,
        hyperparameters().lag_,
        hyperparameters().memory_,
        _population );

    auto new_ts = _population.time_stamps_;

    for ( const auto &col : ts_cols )
        {
            new_ts.push_back( col );
        }

    const auto new_table = DataFrameType(
        _population.categoricals_,
        _population.discretes_,
        _population.indices_,
        _population.join_keys_,
        _population.name_ + "$GETML_PERIPHERAL",
        _population.numericals_,
        {},
        new_ts );

    auto new_peripheral = _peripheral;

    new_peripheral.push_back( new_table );

    return std::make_pair( new_peripheral, ts_data );
}

// -----------------------------------------------------------------------------

template <class FEType>
std::pair<typename FEType::DataFrameType, std::shared_ptr<std::vector<Int>>>
TimeSeriesModel<FEType>::create_population(
    const DataFrameType &_population ) const
{
    // -----------------------------------------------------------------

    if ( hyperparameters().self_join_keys_.size() > 0 )
        {
            return std::make_pair(
                _population, std::shared_ptr<std::vector<Int>>() );
        }

    // -----------------------------------------------------------------

    auto join_keys = _population.join_keys_;

    auto indices = _population.indices_;

    // -----------------------------------------------------------------

    const auto data = std::make_shared<std::vector<Int>>( _population.nrows() );

    const auto new_join_key =
        IntColumnType( data->data(), "$GETML_SELF_JOIN_KEY", data->size(), "" );

    join_keys.push_back( new_join_key );

    indices.push_back( DataFrameType::create_index( new_join_key ) );

    // -----------------------------------------------------------------

    const auto new_table = DataFrameType(
        _population.categoricals_,
        _population.discretes_,
        indices,
        join_keys,
        _population.name_,
        _population.numericals_,
        _population.targets_,
        _population.time_stamps_ );

    // -----------------------------------------------------------------

    return std::make_pair( new_table, data );

    // -----------------------------------------------------------------
}

// -----------------------------------------------------------------------------

template <class FEType>
std::shared_ptr<typename TimeSeriesModel<FEType>::PlaceholderType>
TimeSeriesModel<FEType>::create_placeholder(
    const PlaceholderType &_placeholder,
    const std::vector<std::string> &_self_join_keys,
    const std::string &_ts_name,
    const std::string &_lower_time_stamp_used,
    const std::string &_upper_time_stamp_used ) const
{
    // ----------------------------------------------------------

    const auto joined_table = PlaceholderType(
        _placeholder.categoricals_,
        _placeholder.discretes_,
        _placeholder.join_keys_,
        _placeholder.name_ + "$GETML_PERIPHERAL",
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

            time_stamps_used.push_back( _ts_name );

            upper_time_stamps_used.push_back( _upper_time_stamp_used );
        }

    // ----------------------------------------------------------

    return std::make_shared<PlaceholderType>(
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
    const auto [new_population, jk_data] = create_population( _population );

    const auto [new_peripheral, ts_data] =
        create_peripheral( new_population, _peripheral );

    model().fit( new_population, new_peripheral, _logger );
}

// -----------------------------------------------------------------------------

template <class FEType>
typename FEType::FeaturesType TimeSeriesModel<FEType>::transform(
    const DataFrameType &_population,
    const std::vector<DataFrameType> &_peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger ) const
{
    const auto [new_population, jk_data] = create_population( _population );

    const auto [new_peripheral, ts_data] =
        create_peripheral( new_population, _peripheral );

    return model().transform( new_population, new_peripheral, _logger );
}

// ----------------------------------------------------------------------------
}  // namespace ts

#endif  // TS_TIMESERIESMODEL_HPP_

