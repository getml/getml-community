#ifndef ENGINE_TS_TIMESERIESMODEL_HPP_
#define ENGINE_TS_TIMESERIESMODEL_HPP_

namespace engine
{
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
    /// Creates a modified version of the population table and the peripheral
    /// tables.
    std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
    create_data_frames(
        const containers::DataFrame &_population,
        const std::vector<containers::DataFrame> &_peripheral ) const;

    /// Fits the time series model.
    void fit(
        const DataFrameType &_population,
        const std::vector<DataFrameType> &_peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger =
            std::shared_ptr<const logging::AbstractLogger>() );

    /// Saves the Model in JSON format, if applicable
    void save( const std::string &_fname ) const;

    /// Extracts the ensemble as a Poco::JSON object
    Poco::JSON::Object to_json_obj( const bool _schema_only = false ) const;

    /// Transforms a set of raw data into extracted features.
    FeaturesType transform(
        const DataFrameType &_population,
        const std::vector<DataFrameType> &_peripheral,
        const std::vector<size_t> &_index,
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
    const PlaceholderType &placeholder() const { return model().placeholder(); }

    /// Trivial (const) accessor
    const PlaceholderType &population_schema() const
    {
        return model().population_schema();
    }

    /// Selects the features according to the index given.
    /// TODO: Remove.
    void select_features( const std::vector<size_t> &_index )
    {
        model().select_features( _index );
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
        auto queries = model().to_sql( _feature_prefix, _offset, _subfeatures );
        for ( auto &query : queries )
            {
                query = replace_macros( query );
            }
        return queries;
    }

    // -----------------------------------------------------------------

   private:
    /// This lags the time stamps, which is necessary to prevent easter eggs.
    std::vector<containers::Column<Float>> create_modified_time_stamps(
        const std::string &_ts_name,
        const Float _horizon,
        const Float _memory,
        const containers::DataFrame &_population ) const;

    /// Creates a modified version of the peripheral tables that includes a
    /// modified version of the population table.
    std::vector<containers::DataFrame> create_peripheral(
        const containers::DataFrame &_population,
        const std::vector<containers::DataFrame> &_peripheral ) const;

    /// Creates a new placeholder that contains the self joins.
    std::shared_ptr<PlaceholderType> create_placeholder(
        const PlaceholderType &_placeholder ) const;

    /// Creates a time stamps difference in interpretable form.
    std::string make_time_stamp_diff( const Float _diff ) const;

    /// Little helper function. Replaces all occurences of _from with _to.
    std::string replace_all(
        const std::string &_str,
        const std::string &_from,
        const std::string &_to ) const;

    /// Replaces all of occurences of temporary names in an SQL query with
    /// something more meaningful.
    std::string replace_macros( const std::string &_query ) const;

    /// Creates a modified version of the population table that contains an
    /// additional join key and an additional time stamp, if necessary.
    /// Otherwise, it just returns the original population table.
    containers::DataFrame create_population(
        const containers::DataFrame &_population ) const;

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
    /// layer over the actual feature learner.
    std::optional<FEType> model_;

    // -----------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace ts
}  // namespace engine

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

namespace engine
{
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

    const auto new_placeholder = create_placeholder( *_placeholder );

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
std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
TimeSeriesModel<FEType>::create_data_frames(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral ) const
{
    const auto new_population = create_population( _population );

    const auto new_peripheral =
        create_peripheral( new_population, _peripheral );

    return std::make_pair( new_population, new_peripheral );
}

// ----------------------------------------------------------------------------

template <class FEType>
std::vector<containers::Column<Float>>
TimeSeriesModel<FEType>::create_modified_time_stamps(
    const std::string &_ts_name,
    const Float _horizon,
    const Float _memory,
    const containers::DataFrame &_population ) const
{
    // -----------------------------------------------------------------

    if ( _population.num_time_stamps() == 0 )
        {
            throw std::invalid_argument(
                "DataFrame '" + _population.name() + "' has no time stamps!" );
        }

    // -----------------------------------------------------------------

    const auto horizon_op = [_horizon]( const Float val ) {
        return val + _horizon;
    };

    const auto mem_op = [_horizon, _memory]( const Float val ) {
        return val + _horizon + _memory;
    };

    // -----------------------------------------------------------------

    const auto ts_name =
        _ts_name == "" ? std::string( "$GETML_ROWID" ) : _ts_name;

    size_t ix = 0;

    for ( ; ix < _population.num_time_stamps(); ++ix )
        {
            if ( _population.time_stamp( ix ).name() == ts_name )
                {
                    break;
                }
        }

    if ( ix == _population.num_time_stamps() )
        {
            throw std::invalid_argument(
                "DataFrame '" + _population.name() +
                "' has no time stamps named '" + _ts_name + "'!" );
        }

    const auto ts = _population.time_stamp( ix );

    // -----------------------------------------------------------------

    std::vector<containers::Column<Float>> cols;

    // -----------------------------------------------------------------

    cols.emplace_back( containers::Column<Float>( _population.nrows() ) );

    cols.back().set_name( ts.name() + "$GETML_LOWER_TS" );

    cols.back().set_unit( ts.unit() );

    std::transform( ts.begin(), ts.end(), cols.back().begin(), horizon_op );

    // -----------------------------------------------------------------

    if ( _memory > 0.0 )
        {
            cols.emplace_back(
                containers::Column<Float>( _population.nrows() ) );

            cols.back().set_name( ts.name() + "$GETML_UPPER_TS" );

            cols.back().set_unit( ts.unit() );

            std::transform( ts.begin(), ts.end(), cols.back().begin(), mem_op );
        }

    // -----------------------------------------------------------------

    return cols;

    // -----------------------------------------------------------------
}

// -----------------------------------------------------------------------------

template <class FEType>
std::vector<containers::DataFrame> TimeSeriesModel<FEType>::create_peripheral(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral ) const
{
    // ------------------------------------------------------------

    auto new_df = _population;

    new_df.set_name( new_df.name() + "$GETML_PERIPHERAL" );

    // ------------------------------------------------------------

    if ( hyperparameters().allow_lagged_targets_ )
        {
            for ( size_t i = 0; i < new_df.num_targets(); ++i )
                {
                    new_df.add_float_column( new_df.target( i ), "numerical" );
                }
        }

    // ------------------------------------------------------------

    const auto ts_cols = create_modified_time_stamps(
        hyperparameters().ts_name_,
        hyperparameters().horizon_,
        hyperparameters().memory_,
        _population );

    for ( const auto &col : ts_cols )
        {
            new_df.add_float_column( col, "time_stamp" );
        }

    // ------------------------------------------------------------

    auto new_peripheral = _peripheral;

    new_peripheral.push_back( new_df );

    // ------------------------------------------------------------

    return new_peripheral;

    // ------------------------------------------------------------
}

// -----------------------------------------------------------------------------

template <class FEType>
containers::DataFrame TimeSeriesModel<FEType>::create_population(
    const containers::DataFrame &_population ) const
{
    // -----------------------------------------------------------------

    auto new_df = _population;

    // -----------------------------------------------------------------

    if ( hyperparameters().self_join_keys_.size() == 0 )
        {
            auto new_jk = containers::Column<Int>( new_df.nrows() );

            new_jk.set_name( "$GETML_SELF_JOIN_KEY" );

            new_df.add_int_column( new_jk, "join_key" );
        }

    // -----------------------------------------------------------------

    if ( hyperparameters().ts_name_ == "" )
        {
            auto new_ts = containers::Column<Float>( new_df.nrows() );

            new_ts.set_name( "$GETML_ROWID" );

            new_ts.set_unit( "$GETML_ROWID, comparison only" );

            for ( size_t i = 0; i < new_ts.size(); ++i )
                {
                    new_ts[i] = static_cast<Float>( i );
                }

            new_df.add_float_column( new_ts, "time_stamp" );
        }

    // -----------------------------------------------------------------

    return new_df;

    // -----------------------------------------------------------------
}

// -----------------------------------------------------------------------------

template <class FEType>
std::shared_ptr<typename TimeSeriesModel<FEType>::PlaceholderType>
TimeSeriesModel<FEType>::create_placeholder(
    const PlaceholderType &_placeholder ) const
{
    // --------------------------------------------------------------------

    auto self_join_keys = hyperparameters().self_join_keys_;

    if ( self_join_keys.size() == 0 )
        {
            self_join_keys.push_back( "$GETML_SELF_JOIN_KEY" );
        }

    // --------------------------------------------------------------------

    const auto ts_name = hyperparameters().ts_name_ == ""
                             ? "$GETML_ROWID"
                             : hyperparameters().ts_name_;

    const auto lower_ts_name = ts_name + "$GETML_LOWER_TS";

    const auto upper_ts_name = hyperparameters().memory_ > 0.0
                                   ? ts_name + "$GETML_UPPER_TS"
                                   : std::string( "" );

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

    for ( size_t i = 0; i < self_join_keys.size(); ++i )
        {
            joined_tables.push_back( joined_table );

            join_keys_used.push_back( self_join_keys[i] );

            other_join_keys_used.push_back( self_join_keys[i] );

            other_time_stamps_used.push_back( lower_ts_name );

            time_stamps_used.push_back( ts_name );

            upper_time_stamps_used.push_back( upper_ts_name );
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
    model().fit( _population, _peripheral, _logger );
}

// ----------------------------------------------------------------------------

template <class FEType>
std::string TimeSeriesModel<FEType>::make_time_stamp_diff(
    const Float _diff ) const
{
    constexpr Float seconds_per_day = 24.0 * 60.0 * 60.0;
    constexpr Float seconds_per_hour = 60.0 * 60.0;
    constexpr Float seconds_per_minute = 60.0;

    auto diffstr = std::to_string( _diff ) + " seconds";

    if ( _diff >= seconds_per_day )
        {
            diffstr = std::to_string( _diff / seconds_per_day ) + " days";
        }
    else if ( _diff >= seconds_per_hour )
        {
            diffstr = std::to_string( _diff / seconds_per_hour ) + " hours";
        }
    else if ( _diff >= seconds_per_minute )
        {
            diffstr = std::to_string( _diff / seconds_per_minute ) + " minutes";
        }

    return ", '+" + diffstr + "'";
}

// -----------------------------------------------------------------------------

template <class FEType>
std::string TimeSeriesModel<FEType>::replace_all(
    const std::string &_str,
    const std::string &_from,
    const std::string &_to ) const
{
    if ( _from.empty() )
        {
            return _str;
        }

    auto modified = _str;

    size_t pos = 0;

    while ( ( pos = modified.find( _from, pos ) ) != std::string::npos )
        {
            modified.replace( pos, _from.length(), _to );
            pos += _to.length();
        }

    return modified;
}

// -----------------------------------------------------------------------------

template <class FEType>
std::string TimeSeriesModel<FEType>::replace_macros(
    const std::string &_query ) const
{
    // --------------------------------------------------------------

    const auto getml_lower_ts_rowid =
        hyperparameters().horizon_ > 0.0
            ? " + " + std::to_string( hyperparameters().horizon_ )
            : std::string( "" );

    const auto getml_upper_ts_rowid =
        " + " + std::to_string(
                    hyperparameters().horizon_ + hyperparameters().memory_ );

    const auto getml_lower_ts =
        hyperparameters().horizon_ > 0.0
            ? make_time_stamp_diff( hyperparameters().horizon_ )
            : std::string( "" );

    const auto getml_upper_ts = make_time_stamp_diff(
        hyperparameters().horizon_ + hyperparameters().memory_ );

    std::stringstream one;

    one << "       1," << std::endl;

    // --------------------------------------------------------------

    auto new_query = replace_all(
        _query,
        "datetime( t1.\"$GETML_ROWID$GETML_UPPER_TS\" )",
        "t1.rowid" + getml_upper_ts_rowid );

    new_query = replace_all(
        new_query,
        "datetime( t2.\"$GETML_ROWID$GETML_UPPER_TS\" )",
        "t2.rowid" + getml_upper_ts_rowid );

    new_query =
        replace_all( new_query, "$GETML_LOWER_TS\"", "\"" + getml_lower_ts );

    new_query =
        replace_all( new_query, "$GETML_UPPER_TS\"", "\"" + getml_upper_ts );

    new_query = replace_all( new_query, "$GETML_PERIPHERAL", "" );

    new_query = replace_all( new_query, "t1.\"$GETML_SELF_JOIN_KEY\"", "1" );

    new_query = replace_all( new_query, "t2.\"$GETML_SELF_JOIN_KEY\"", "1" );

    new_query = replace_all( new_query, "  " + one.str(), "" );

    new_query = replace_all( new_query, one.str(), "" );

    new_query = replace_all( new_query, "$GETML_SELF_JOIN_KEY, ", "" );

    new_query =
        replace_all( new_query, "datetime( t1.\"$GETML_ROWID\" )", "t1.rowid" );

    new_query =
        replace_all( new_query, "datetime( t2.\"$GETML_ROWID\" )", "t2.rowid" );

    new_query = replace_all( new_query, "\"$GETML_ROWID\"", "rowid" );

    // --------------------------------------------------------------

    return new_query;

    // --------------------------------------------------------------
}

// -----------------------------------------------------------------------------

template <class FEType>
void TimeSeriesModel<FEType>::save( const std::string &_fname ) const
{
    std::ofstream output( _fname );

    output << jsonutils::JSON::stringify( to_json_obj() );

    output.close();
}

// -----------------------------------------------------------------------------

template <class FEType>
Poco::JSON::Object TimeSeriesModel<FEType>::to_json_obj(
    const bool _schema_only ) const
{
    auto obj = model().to_json_obj( _schema_only );

    auto hyp_obj = jsonutils::JSON::get_object( obj, "hyperparameters_" );

    hyp_obj->set(
        "allow_lagged_targets_", hyperparameters().allow_lagged_targets_ );

    hyp_obj->set( "horizon_", hyperparameters().horizon_ );

    hyp_obj->set( "memory_", hyperparameters().memory_ );

    hyp_obj->set(
        "self_join_keys_",
        jsonutils::JSON::vector_to_array_ptr(
            hyperparameters().self_join_keys_ ) );

    hyp_obj->set( "ts_name_", hyperparameters().ts_name_ );

    return obj;
}

// -----------------------------------------------------------------------------

template <class FEType>
typename FEType::FeaturesType TimeSeriesModel<FEType>::transform(
    const DataFrameType &_population,
    const std::vector<DataFrameType> &_peripheral,
    const std::vector<size_t> &_index,
    const std::shared_ptr<const logging::AbstractLogger> _logger ) const
{
    return model().transform( _population, _peripheral, _index, _logger );
}

// ----------------------------------------------------------------------------
}  // namespace ts
}  // namespace engine
#endif  // ENGINE_TS_TIMESERIESMODEL_HPP_

