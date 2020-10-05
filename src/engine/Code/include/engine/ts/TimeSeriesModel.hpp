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
        const std::shared_ptr<const HypType> &_hyperparameters,
        const std::shared_ptr<const std::vector<std::string>> &_peripheral,
        const std::shared_ptr<const PlaceholderType> &_placeholder,
        const std::shared_ptr<const std::vector<PlaceholderType>>
            &_peripheral_schema = nullptr,
        const std::shared_ptr<const PlaceholderType> &_population_schema =
            nullptr );

    TimeSeriesModel( const Poco::JSON::Object &_obj );

    ~TimeSeriesModel();

    // -----------------------------------------------------------------

   public:
    /// Calculates the column importances for this ensemble.
    std::map<helpers::ColumnDescription, Float> column_importances(
        const std::vector<Float> &_importance_factors ) const;

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
    /// Trivial (const) accessor
    const HypType &hyperparameters() const
    {
        assert_true( hyperparameters_ );
        return *hyperparameters_;
    }

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

    /// Expresses DecisionTreeEnsemble as SQL code.
    std::vector<std::string> to_sql(
        const std::shared_ptr<const std::vector<strings::String>> &_categories,
        const std::string &_feature_prefix = "",
        const size_t _offset = 0,
        const bool _subfeatures = true ) const
    {
        auto queries = model().to_sql(
            _categories, _feature_prefix, _offset, _subfeatures );
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

    /// Creates a modified version of the population table that contains an
    /// additional join key and an additional time stamp, if necessary.
    /// Otherwise, it just returns the original population table.
    containers::DataFrame create_population(
        const containers::DataFrame &_population ) const;

    /// Replaces all of occurences of temporary names in an SQL query with
    /// something more meaningful.
    std::string replace_macros( const std::string &_query ) const;

    /// Transfers the importances values from colnames containing macros to
    /// actual columns.
    void transfer_importance_value(
        const helpers::ColumnDescription &_from,
        helpers::ImportanceMaker *_importance_maker ) const;

    // -----------------------------------------------------------------

   public:
    /// Trivial accessor
    bool &allow_http() { return model().allow_http(); }

    /// Trivial accessor
    bool allow_http() const { return model().allow_http(); }

    // -----------------------------------------------------------------

   private:
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

    new_peripheral->push_back(
        _placeholder->name() + containers::Macros::peripheral() );

    // --------------------------------------------------------------------

    model_ = std::make_optional<FEType>(
        hyperparameters().model_hyperparams_,
        new_peripheral,
        new_placeholder,
        _peripheral_schema,
        _population_schema );

    // --------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <class FEType>
TimeSeriesModel<FEType>::TimeSeriesModel( const Poco::JSON::Object &_obj )
    : hyperparameters_( std::make_shared<const HypType>(
          *jsonutils::JSON::get_object( _obj, "hyperparameters_" ) ) ),
      model_( std::make_optional<FEType>( _obj ) )
{
    assert_true( hyperparameters().model_hyperparams_ );
}

// ----------------------------------------------------------------------------

template <class FEType>
TimeSeriesModel<FEType>::~TimeSeriesModel() = default;

// ----------------------------------------------------------------------------

template <class FEType>
std::map<helpers::ColumnDescription, Float>
TimeSeriesModel<FEType>::column_importances(
    const std::vector<Float> &_importance_factors ) const
{
    const auto importances = model().column_importances( _importance_factors );

    auto importance_maker = helpers::ImportanceMaker( importances );

    for ( const auto &[desc, _] : importances )
        {
            transfer_importance_value( desc, &importance_maker );
        }

    return importance_maker.importances();
}

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
    const auto ts_name =
        _ts_name == "" ? containers::Macros::rowid() : _ts_name;

    auto cols = TimeStampMaker::make_time_stamps(
        ts_name, _horizon, _memory, _population );

    assert_true( cols.size() == 0 || cols.size() == 1 || cols.size() == 2 );

    assert_true( _horizon != 0.0 || _memory > 0.0 || cols.size() == 0 );

    assert_true( _horizon == 0.0 || _memory <= 0.0 || cols.size() == 2 );

    if ( _horizon != 0.0 )
        {
            assert_true( cols.size() > 0 );

            cols.at( 0 ).set_name( ts_name + containers::Macros::lower_ts() );
        }

    if ( _memory > 0.0 )
        {
            assert_true( cols.size() > 0 );

            cols.back().set_name( ts_name + containers::Macros::upper_ts() );
        }

    return cols;
}

// -----------------------------------------------------------------------------

template <class FEType>
std::vector<containers::DataFrame> TimeSeriesModel<FEType>::create_peripheral(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral ) const
{
    // ------------------------------------------------------------

    auto new_df = _population;

    new_df.set_name( new_df.name() + containers::Macros::peripheral() );

    // ------------------------------------------------------------

    const auto ts_cols = create_modified_time_stamps(
        hyperparameters().ts_name_,
        hyperparameters().horizon_,
        hyperparameters().memory_,
        _population );

    for ( const auto &col : ts_cols )
        {
            new_df.add_float_column(
                col, containers::DataFrame::ROLE_TIME_STAMP );
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

            new_jk.set_name( containers::Macros::self_join_key() );

            new_df.add_int_column(
                new_jk, containers::DataFrame::ROLE_JOIN_KEY );
        }

    // -----------------------------------------------------------------

    if ( hyperparameters().ts_name_ == "" )
        {
            auto new_ts = containers::Column<Float>( new_df.nrows() );

            new_ts.set_name( containers::Macros::rowid() );

            new_ts.set_unit( containers::Macros::rowid_comparison_only() );

            for ( size_t i = 0; i < new_ts.size(); ++i )
                {
                    new_ts[i] = static_cast<Float>( i );
                }

            new_df.add_float_column(
                new_ts, containers::DataFrame::ROLE_TIME_STAMP );
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
            self_join_keys.push_back( containers::Macros::self_join_key() );
        }

    // --------------------------------------------------------------------

    const auto ts_name = hyperparameters().ts_name_ == ""
                             ? containers::Macros::rowid()
                             : hyperparameters().ts_name_;

    const auto lower_ts_name = hyperparameters().horizon_ != 0.0
                                   ? ts_name + containers::Macros::lower_ts()
                                   : ts_name;

    const auto upper_ts_name = hyperparameters().memory_ > 0.0
                                   ? ts_name + containers::Macros::upper_ts()
                                   : std::string( "" );

    // ----------------------------------------------------------

    const auto joined_table = PlaceholderType(
        _placeholder.categoricals_,
        _placeholder.discretes_,
        _placeholder.join_keys_,
        _placeholder.name_ + containers::Macros::peripheral(),
        _placeholder.numericals_,
        _placeholder.targets_,
        _placeholder.time_stamps_ );

    // ----------------------------------------------------------

    auto allow_lagged_targets = _placeholder.allow_lagged_targets_;

    auto joined_tables = _placeholder.joined_tables_;

    auto join_keys_used = _placeholder.join_keys_used_;

    auto other_join_keys_used = _placeholder.other_join_keys_used_;

    auto other_time_stamps_used = _placeholder.other_time_stamps_used_;

    auto time_stamps_used = _placeholder.time_stamps_used_;

    auto upper_time_stamps_used = _placeholder.upper_time_stamps_used_;

    // ----------------------------------------------------------

    for ( size_t i = 0; i < self_join_keys.size(); ++i )
        {
            allow_lagged_targets.push_back(
                hyperparameters().allow_lagged_targets_ );

            joined_tables.push_back( joined_table );

            join_keys_used.push_back( self_join_keys.at( i ) );

            other_join_keys_used.push_back( self_join_keys.at( i ) );

            other_time_stamps_used.push_back( lower_ts_name );

            time_stamps_used.push_back( ts_name );

            upper_time_stamps_used.push_back( upper_ts_name );
        }

    // ----------------------------------------------------------

    return std::make_shared<PlaceholderType>(
        allow_lagged_targets,
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

    const auto getml_lower_ts = hyperparameters().horizon_ > 0.0
                                    ? utils::TSDiffMaker::make_time_stamp_diff(
                                          hyperparameters().horizon_ )
                                    : std::string( "" );

    const auto getml_upper_ts = utils::TSDiffMaker::make_time_stamp_diff(
        hyperparameters().horizon_ + hyperparameters().memory_ );

    std::stringstream one;

    one << "       1," << std::endl;

    // --------------------------------------------------------------

    auto new_query = utils::StringReplacer::replace_all(
        _query,
        "datetime( t1.\"" + containers::Macros::rowid() +
            containers::Macros::upper_ts() + "\" )",
        "t1.rowid" + getml_upper_ts_rowid );

    new_query = utils::StringReplacer::replace_all(
        new_query,
        "datetime( t2.\"" + containers::Macros::rowid() +
            containers::Macros::upper_ts() + "\" )",
        "t2.rowid" + getml_upper_ts_rowid );

    new_query = utils::StringReplacer::replace_all(
        new_query,
        containers::Macros::lower_ts() + "\"",
        "\"" + getml_lower_ts );

    new_query = utils::StringReplacer::replace_all(
        new_query,
        containers::Macros::upper_ts() + "\"",
        "\"" + getml_upper_ts );

    new_query = utils::StringReplacer::replace_all(
        new_query, containers::Macros::peripheral(), "" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t1.\"" + containers::Macros::self_join_key() + "\"", "1" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t2.\"" + containers::Macros::self_join_key() + "\"", "1" );

    new_query =
        utils::StringReplacer::replace_all( new_query, "  " + one.str(), "" );

    new_query = utils::StringReplacer::replace_all( new_query, one.str(), "" );

    new_query = utils::StringReplacer::replace_all(
        new_query, containers::Macros::self_join_key() + ", ", "" );

    new_query = utils::StringReplacer::replace_all(
        new_query,
        "datetime( t1.\"" + containers::Macros::rowid() + "\" )",
        "t1.rowid" );

    new_query = utils::StringReplacer::replace_all(
        new_query,
        "datetime( t2.\"" + containers::Macros::rowid() + "\" )",
        "t2.rowid" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "\"" + containers::Macros::rowid() + "\"", "rowid" );

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

// ----------------------------------------------------------------------------

template <class FEType>
void TimeSeriesModel<FEType>::transfer_importance_value(
    const helpers::ColumnDescription &_from,
    helpers::ImportanceMaker *_importance_maker ) const
{
    std::unique_ptr<helpers::ColumnDescription> from_desc =
        std::make_unique<helpers::ColumnDescription>( _from );

    if ( from_desc->table_.find( containers::Macros::peripheral() ) !=
         std::string::npos )
        {
            const auto to_table = utils::StringReplacer::replace_all(
                from_desc->table_, containers::Macros::peripheral(), "" );

            auto to_desc = std::make_unique<helpers::ColumnDescription>(
                _importance_maker->population(), to_table, from_desc->name_ );

            _importance_maker->transfer( *from_desc, *to_desc );

            from_desc = std::move( to_desc );
        }

    if ( from_desc->name_.find( containers::Macros::upper_ts() ) !=
         std::string::npos )
        {
            const auto to_name = utils::StringReplacer::replace_all(
                from_desc->name_, containers::Macros::upper_ts(), "" );

            auto to_desc = std::make_unique<helpers::ColumnDescription>(
                _importance_maker->population(), from_desc->table_, to_name );

            _importance_maker->transfer( *from_desc, *to_desc );

            from_desc = std::move( to_desc );
        }

    if ( from_desc->name_.find( containers::Macros::lower_ts() ) !=
         std::string::npos )
        {
            const auto to_name = utils::StringReplacer::replace_all(
                from_desc->name_, containers::Macros::lower_ts(), "" );

            auto to_desc = std::make_unique<helpers::ColumnDescription>(
                _importance_maker->population(), from_desc->table_, to_name );

            _importance_maker->transfer( *from_desc, *to_desc );

            from_desc = std::move( to_desc );
        }

    if ( from_desc->name_.find( containers::Macros::rowid() ) !=
         std::string::npos )
        {
            const auto to_name = utils::StringReplacer::replace_all(
                from_desc->name_, containers::Macros::rowid(), "rowid" );

            auto to_desc = std::make_unique<helpers::ColumnDescription>(
                _importance_maker->population(), from_desc->table_, to_name );

            _importance_maker->transfer( *from_desc, *to_desc );

            from_desc = std::move( to_desc );
        }
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

