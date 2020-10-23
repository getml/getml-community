#ifndef ENGINE_FEATURELEARNERS_FEATURELEARNER_HPP_
#define ENGINE_FEATURELEARNERS_FEATURELEARNER_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace featurelearners
{
// ----------------------------------------------------------------------------

template <class FeatureLearnerType>
class FeatureLearner : public AbstractFeatureLearner
{
    // --------------------------------------------------------

   private:
    typedef typename FeatureLearnerType::DataFrameType DataFrameType;
    typedef typename FeatureLearnerType::HypType HypType;

    // --------------------------------------------------------

   public:
    FeatureLearner(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const helpers::Placeholder>& _placeholder,
        const std::shared_ptr<const std::vector<std::string>>& _peripheral,
        const std::vector<Poco::JSON::Object::Ptr>& _dependencies )
        : cmd_( _cmd ),
          dependencies_( _dependencies ),
          placeholder_( _placeholder ),
          peripheral_( _peripheral )
    {
        assert_true( placeholder_ );
        assert_true( peripheral_ );
    }

    /// Destructor.
    ~FeatureLearner() = default;

    // --------------------------------------------------------

   public:
    /// Returns the fingerprint of the feature learner (necessary to build
    /// the dependency graphs).
    Poco::JSON::Object::Ptr fingerprint() const final;

    /// Fits the model.
    void fit(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const communication::SocketLogger>& _logger,
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs,
        const Int _target_num ) final;

    /// Data frames might have to be modified, such as adding upper time stamps
    /// or self joins.
    std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
    modify_data_frames(
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs ) const final;

    /// Generate features.
    containers::Features transform(
        const Poco::JSON::Object& _cmd,
        const std::vector<size_t>& _index,
        const std::shared_ptr<const communication::SocketLogger>& _logger,
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs ) const final;

    /// Returns a string describing the type of the feature learner.
    std::string type() const final;

    // --------------------------------------------------------

   public:
    /// Creates a deep copy.
    std::shared_ptr<AbstractFeatureLearner> clone() const final
    {
        return std::make_shared<FeatureLearner<FeatureLearnerType>>( *this );
    }

    /// Calculates the column importances for this ensemble.
    std::map<helpers::ColumnDescription, Float> column_importances(
        const std::vector<Float>& _importance_factors ) const final
    {
        return feature_learner().column_importances( _importance_factors );
    }

    /// Whether the feature learner is used for classification.
    bool is_classification() const final
    {
        const auto loss_function =
            JSON::get_value<std::string>( cmd_, "loss_function_" );
        return ( loss_function != "SquareLoss" );
    }

    /// Whether this is a time series model (based on a self-join).
    bool is_time_series() const final
    {
        return FeatureLearnerType::is_time_series_;
    }

    /// Loads the feature learner from a file designated by _fname.
    void load( const std::string& _fname ) final
    {
        const auto obj = load_json_obj( _fname );
        feature_learner_ = std::make_optional<FeatureLearnerType>( obj );
    }

    /// Returns the placeholder not as passed by the user, but as seen by the
    /// feature learner (the difference matters for time series).
    helpers::Placeholder make_placeholder() const final
    {
        return make_feature_learner()->placeholder();
    }

    /// Returns the number of features in the feature learner.
    size_t num_features() const final
    {
        return feature_learner().num_features();
    }

    /// Whether the feature learner is for the premium version only.
    bool premium_only() const final
    {
        return FeatureLearnerType::premium_only_;
    }

    /// Saves the feature learner in JSON format, if applicable
    void save( const std::string& _fname ) const final
    {
        feature_learner().save( _fname );
    }

    /// Whether the feature learner is to be silent.
    bool silent() const final
    {
        return make_feature_learner()->hyperparameters().silent_;
    }

    /// Whether the feature learner supports multiple targets.
    bool supports_multiple_targets() const final
    {
        return FeatureLearnerType::supports_multiple_targets_;
    }

    /// Return model as a JSON Object.
    Poco::JSON::Object to_json_obj( const bool _schema_only ) const final
    {
        return feature_learner().to_json_obj( _schema_only );
    }

    /// Return feature learner as SQL code.
    std::vector<std::string> to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories,
        const std::string& _prefix,
        const bool _subfeatures ) const final
    {
        return feature_learner().to_sql(
            _categories, _prefix, 0, _subfeatures );
    }

    // --------------------------------------------------------

   private:
    /// Extract a data frame of type FeatureLearnerType::DataFrameType from
    /// an engine::containers::DataFrame.
    DataFrameType extract_table(
        const containers::DataFrame& _df, const Int _target_num ) const;

    /// Extract a data frame of type FeatureLearnerType::DataFrameType from
    /// an engine::containers::DataFrame using the pre-stored schema.
    template <typename SchemaType>
    DataFrameType extract_table_by_colnames(
        const SchemaType& _schema,
        const containers::DataFrame& _df,
        const bool _needs_targets ) const;

    /// Extract a vector of FeatureLearnerType::DataFrameType from
    /// an engine::containers::DataFrame using the pre-stored schema.
    std::pair<DataFrameType, std::vector<DataFrameType>> extract_tables(
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs,
        const Int _target_num ) const;

    /// Extract a vector of FeatureLearnerType::DataFrameType from
    /// an engine::containers::DataFrame using the pre-stored schema.
    std::pair<DataFrameType, std::vector<DataFrameType>>
    extract_tables_by_colnames(
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs ) const;

    /// Infers whether we need the targets of a peripheral table.
    std::vector<bool> infer_needs_targets(
        const helpers::Placeholder& _placeholder,
        const size_t _num_peripheral,
        std::vector<bool>* _needs_targets = nullptr ) const;

    /// Initializes the feature learner.
    std::optional<FeatureLearnerType> make_feature_learner() const;

    /// Extracts the table and column name, if they are from a many-to-one join,
    /// needed for the column importances.
    std::pair<std::string, std::string> parse_table_colname(
        const std::string& _table, const std::string& _colname ) const;

    /// Removes the time difference marker from the colnames, needed for the
    /// column importances.
    std::string remove_time_diff( const std::string& _from_colname ) const;

    /// Helper function for loading a json object.
    Poco::JSON::Object load_json_obj( const std::string& _fname ) const;

    // --------------------------------------------------------

   private:
    /// Trivial accessor.
    FeatureLearnerType& feature_learner()
    {
        if ( !feature_learner_ )
            {
                throw std::invalid_argument(
                    "Feature learning algorithm has not been fitted!" );
            }

        return *feature_learner_;
    }

    /// Trivial accessor.
    const FeatureLearnerType& feature_learner() const
    {
        if ( !feature_learner_ )
            {
                throw std::invalid_argument(
                    "Feature learning algorithm has not been fitted!" );
            }

        return *feature_learner_;
    }

    /// Trivial accessor.
    const std::vector<std::string>& peripheral() const
    {
        assert_true( peripheral_ );
        return *peripheral_;
    }

    /// Trivial accessor.
    const helpers::Placeholder& placeholder() const
    {
        assert_true( placeholder_ );
        return *placeholder_;
    }

    /// Determines whether the population table needs targets during
    /// transform(...).
    const bool population_needs_targets() const
    {
        if constexpr ( FeatureLearnerType::is_time_series_ )
            {
                return feature_learner()
                    .hyperparameters()
                    .allow_lagged_targets_;
            }

        return false;
    }

    // --------------------------------------------------------

   private:
    /// The command used to create the feature learner.
    Poco::JSON::Object cmd_;

    /// The dependencies used to build the fingerprint.
    std::vector<Poco::JSON::Object::Ptr> dependencies_;

    /// The underlying feature learning algorithm.
    std::optional<FeatureLearnerType> feature_learner_;

    /// The placeholder describing the data schema.
    std::shared_ptr<const helpers::Placeholder> placeholder_;

    /// The names of the peripheral tables
    std::shared_ptr<const std::vector<std::string>> peripheral_;

    // --------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace featurelearners
}  // namespace engine

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

namespace engine
{
namespace featurelearners
{
// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
typename FeatureLearnerType::DataFrameType
FeatureLearner<FeatureLearnerType>::extract_table(
    const containers::DataFrame& _df, const Int _target_num ) const
{
    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::IntColumnType> categoricals;

    for ( size_t i = 0; i < _df.num_categoricals(); ++i )
        {
            const auto& mat = _df.categorical( i );

            categoricals.push_back( typename DataFrameType::IntColumnType(
                mat.data(), mat.name(), mat.nrows(), mat.unit() ) );
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::IntColumnType> join_keys;

    for ( size_t i = 0; i < _df.num_join_keys(); ++i )
        {
            const auto& mat = _df.join_key( i );

            join_keys.push_back( typename DataFrameType::IntColumnType(
                mat.data(), mat.name(), mat.nrows(), mat.unit() ) );
        }

    // ------------------------------------------------------------------------
    // The numerical/discrete binning strategy exists, but
    // the user does not have to think about it. Instead, this will make
    // the decision for him/her.

    std::vector<typename DataFrameType::FloatColumnType> discretes;

    std::vector<typename DataFrameType::FloatColumnType> numericals;

    const auto is_int = []( const Float val ) {
        return std::isnan( val ) || val == std::round( val );
    };

    for ( size_t i = 0; i < _df.num_numericals(); ++i )
        {
            const auto& mat = _df.numerical( i );

            const bool is_discrete =
                std::all_of( mat.begin(), mat.end(), is_int );

            if ( is_discrete )
                {
                    discretes.push_back(
                        typename DataFrameType::FloatColumnType(
                            mat.data(), mat.name(), mat.nrows(), mat.unit() ) );
                }
            else
                {
                    numericals.push_back(
                        typename DataFrameType::FloatColumnType(
                            mat.data(), mat.name(), mat.nrows(), mat.unit() ) );
                }
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::FloatColumnType> targets;

    switch ( _target_num )
        {
            case AbstractFeatureLearner::USE_ALL_TARGETS:
                for ( size_t i = 0; i < _df.num_targets(); ++i )
                    {
                        const auto& mat = _df.target( i );

                        targets.push_back(
                            typename DataFrameType::FloatColumnType(
                                mat.data(),
                                mat.name(),
                                mat.nrows(),
                                mat.unit() ) );
                    }
                break;

            default:
                assert_true( _target_num >= 0 );
                assert_true(
                    _target_num < static_cast<Int>( _df.num_targets() ) );

                const auto& mat = _df.target( _target_num );

                targets.push_back( typename DataFrameType::FloatColumnType(
                    mat.data(), mat.name(), mat.nrows(), mat.unit() ) );
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::FloatColumnType> time_stamps;

    for ( size_t i = 0; i < _df.num_time_stamps(); ++i )
        {
            const auto& mat = _df.time_stamp( i );

            time_stamps.push_back( typename DataFrameType::FloatColumnType(
                mat.data(), mat.name(), mat.nrows(), mat.unit() ) );
        }

    // ------------------------------------------------------------------------

    return DataFrameType(
        categoricals,
        discretes,
        _df.maps(),
        join_keys,
        _df.name(),
        numericals,
        targets,
        time_stamps );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
template <typename SchemaType>
typename FeatureLearnerType::DataFrameType
FeatureLearner<FeatureLearnerType>::extract_table_by_colnames(
    const SchemaType& _schema,
    const containers::DataFrame& _df,
    const bool _needs_targets ) const
{
    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::IntColumnType> categoricals;

    for ( size_t i = 0; i < _schema.num_categoricals(); ++i )
        {
            const auto& name = _schema.categorical_name( i );

            const auto& mat = _df.categorical( name );

            categoricals.push_back( typename DataFrameType::IntColumnType(
                mat.data(), name, mat.nrows(), mat.unit() ) );
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::FloatColumnType> discretes;

    for ( size_t i = 0; i < _schema.num_discretes(); ++i )
        {
            const auto& name = _schema.discrete_name( i );

            // Note that discrete columns actually do not exist
            // in the DataFrame - they are taken from numerical
            // instead.
            const auto& mat = _df.numerical( name );

            discretes.push_back( typename DataFrameType::FloatColumnType(
                mat.data(), name, mat.nrows(), mat.unit() ) );
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::IntColumnType> join_keys;

    std::vector<std::shared_ptr<typename containers::DataFrameIndex::MapType>>
        indices;

    for ( size_t i = 0; i < _schema.num_join_keys(); ++i )
        {
            const auto& name = _schema.join_keys_name( i );

            const auto& mat = _df.join_key( name );

            join_keys.push_back( typename DataFrameType::IntColumnType(
                mat.data(), name, mat.nrows(), mat.unit() ) );

            indices.push_back( _df.index( name ).map() );
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::FloatColumnType> numericals;

    for ( size_t i = 0; i < _schema.num_numericals(); ++i )
        {
            const auto& name = _schema.numerical_name( i );

            const auto& mat = _df.numerical( name );

            numericals.push_back( typename DataFrameType::FloatColumnType(
                mat.data(), name, mat.nrows(), mat.unit() ) );
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::FloatColumnType> targets;

    for ( size_t i = 0; i < _schema.num_targets(); ++i )
        {
            const auto& name = _schema.target_name( i );

            if ( _df.has_target( name ) )
                {
                    const auto& mat = _df.target( name );

                    targets.push_back( typename DataFrameType::FloatColumnType(
                        mat.data(), name, mat.nrows(), mat.unit() ) );
                }
            else if ( _needs_targets )
                {
                    throw std::invalid_argument(
                        "Target '" + name + "' not found in data frame '" +
                        _df.name() +
                        "', but is required to generate the "
                        "prediction. This is because you have set "
                        "allow_lagged_targets to True." );
                }
        }

    // ------------------------------------------------------------------------

    std::vector<typename DataFrameType::FloatColumnType> time_stamps;

    for ( size_t i = 0; i < _schema.num_time_stamps(); ++i )
        {
            const auto& name = _schema.time_stamps_name( i );

            const auto& mat = _df.time_stamp( name );

            time_stamps.push_back( typename DataFrameType::FloatColumnType(
                mat.data(), name, mat.nrows(), mat.unit() ) );
        }

    // ------------------------------------------------------------------------

    return DataFrameType(
        categoricals,
        discretes,
        indices,
        join_keys,
        _df.name(),
        numericals,
        targets,
        time_stamps );

    // ------------------------------------------------------------------------
}

// ------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::pair<
    typename FeatureLearnerType::DataFrameType,
    std::vector<typename FeatureLearnerType::DataFrameType>>
FeatureLearner<FeatureLearnerType>::extract_tables(
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const Int _target_num ) const
{
    // ------------------------------------------------

    const auto population_table = extract_table( _population_df, _target_num );

    // ------------------------------------------------

    std::vector<DataFrameType> peripheral_tables;

    for ( const auto& df : _peripheral_dfs )
        {
            const auto table =
                extract_table( df, AbstractFeatureLearner::USE_ALL_TARGETS );

            peripheral_tables.push_back( table );
        }

    // ------------------------------------------------

    return std::make_pair( population_table, peripheral_tables );

    // ------------------------------------------------
}

// ------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::pair<
    typename FeatureLearnerType::DataFrameType,
    std::vector<typename FeatureLearnerType::DataFrameType>>
FeatureLearner<FeatureLearnerType>::extract_tables_by_colnames(
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs ) const
{
    // -------------------------------------------------------------------------

    const auto population_schema = feature_learner().population_schema();

    const auto population_table = extract_table_by_colnames(
        population_schema, _population_df, population_needs_targets() );

    // ------------------------------------------------

    const auto peripheral_schema = feature_learner().peripheral_schema();

    if ( peripheral_schema.size() != _peripheral_dfs.size() )
        {
            const auto expected = FeatureLearnerType::is_time_series_
                                      ? peripheral_schema.size() - 1
                                      : peripheral_schema.size();

            const auto got = FeatureLearnerType::is_time_series_
                                 ? _peripheral_dfs.size() - 1
                                 : _peripheral_dfs.size();

            throw std::invalid_argument(
                "Expected " + std::to_string( expected ) +
                " peripheral tables, got " + std::to_string( got ) + "." );
        }

    // ------------------------------------------------

    const auto needs_targets =
        infer_needs_targets( placeholder(), peripheral_schema.size() );

    assert_true( needs_targets.size() == peripheral_schema.size() );

    // ------------------------------------------------

    std::vector<DataFrameType> peripheral_tables;

    for ( size_t i = 0; i < peripheral_schema.size(); ++i )
        {
            const auto table = extract_table_by_colnames(
                peripheral_schema.at( i ),
                _peripheral_dfs.at( i ),
                needs_targets.at( i ) );

            peripheral_tables.push_back( table );
        }

    // ------------------------------------------------

    return std::make_pair( population_table, peripheral_tables );

    // ------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
Poco::JSON::Object::Ptr FeatureLearner<FeatureLearnerType>::fingerprint() const
{
    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    obj->set( "cmd_", cmd_ );

    obj->set( "dependencies_", JSON::vector_to_array_ptr( dependencies_ ) );

    obj->set( "peripheral_", JSON::vector_to_array_ptr( peripheral() ) );

    obj->set( "placeholder_", placeholder().to_json_obj() );

    return obj;
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
void FeatureLearner<FeatureLearnerType>::fit(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const communication::SocketLogger>& _logger,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const Int _target_num )
{
    // ------------------------------------------------

    const auto [population_df, peripheral_dfs] =
        modify_data_frames( _population_df, _peripheral_dfs );

    // ------------------------------------------------

    const auto [population_table, peripheral_tables] =
        extract_tables( population_df, peripheral_dfs, _target_num );

    // ------------------------------------------------

    auto new_feature_learner = make_feature_learner();

    new_feature_learner->fit( population_table, peripheral_tables, _logger );

    // ------------------------------------------------

    feature_learner_ = std::move( new_feature_learner );

    // ------------------------------------------------
}

// ------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::vector<bool> FeatureLearner<FeatureLearnerType>::infer_needs_targets(
    const helpers::Placeholder& _placeholder,
    const size_t _num_peripheral,
    std::vector<bool>* _needs_targets ) const
{
    // ------------------------------------------------------------------------

    std::vector<bool> needs_targets;

    // ------------------------------------------------------------------------

    if ( _needs_targets != nullptr )
        {
            needs_targets = *_needs_targets;
            assert_true( needs_targets.size() == _num_peripheral );
        }
    else
        {
            needs_targets = std::vector<bool>( _num_peripheral, false );
        }

    // ------------------------------------------------------------------------

    const auto& allow_lagged_targets = _placeholder.allow_lagged_targets_;

    const auto& joined_tables = _placeholder.joined_tables_;

    assert_true( allow_lagged_targets.size() == joined_tables.size() );

    // ------------------------------------------------------------------------

    for ( size_t i = 0; i < joined_tables.size(); ++i )
        {
            const auto& joined_table = joined_tables.at( i );

            if ( allow_lagged_targets.at( i ) )
                {
                    const auto name = joined_table.name_;

                    const auto it = std::find(
                        peripheral().begin(), peripheral().end(), name );

                    if ( it == peripheral().end() )
                        {
                            throw std::invalid_argument(
                                "Peripheral placeholder named '" + name +
                                "' not found!" );
                        }

                    const auto dist = std::distance( peripheral().begin(), it );

                    needs_targets.at( dist ) = true;
                }

            needs_targets = infer_needs_targets(
                joined_table, _num_peripheral, &needs_targets );
        }

    // ------------------------------------------------------------------------

    return needs_targets;

    // ------------------------------------------------------------------------
}

// ------------------------------------------------------------------------

template <typename FeatureLearnerType>
Poco::JSON::Object FeatureLearner<FeatureLearnerType>::load_json_obj(
    const std::string& _fname ) const
{
    std::ifstream input( _fname );

    std::stringstream json;

    std::string line;

    if ( input.is_open() )
        {
            while ( std::getline( input, line ) )
                {
                    json << line;
                }

            input.close();
        }
    else
        {
            throw std::invalid_argument( "File '" + _fname + "' not found!" );
        }

    const auto ptr = Poco::JSON::Parser()
                         .parse( json.str() )
                         .extract<Poco::JSON::Object::Ptr>();

    if ( !ptr )
        {
            throw std::runtime_error( "JSON file did not contain an object!" );
        }

    return *ptr;
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::optional<FeatureLearnerType>
FeatureLearner<FeatureLearnerType>::make_feature_learner() const
{
    const auto hyperparameters =
        std::make_shared<typename FeatureLearnerType::HypType>( cmd_ );

    const auto population_schema =
        std::shared_ptr<const helpers::Placeholder>();

    const auto peripheral_schema =
        std::shared_ptr<const std::vector<helpers::Placeholder>>();

    return std::make_optional<FeatureLearnerType>(
        hyperparameters,
        peripheral_,
        placeholder_,
        peripheral_schema,
        population_schema );
}

// -----------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
FeatureLearner<FeatureLearnerType>::modify_data_frames(
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs ) const
{
    if constexpr ( FeatureLearnerType::is_time_series_ )
        {
            return make_feature_learner()->create_data_frames(
                _population_df, _peripheral_dfs );
        }

    return std::make_pair( _population_df, _peripheral_dfs );
}

// -----------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::pair<std::string, std::string>
FeatureLearner<FeatureLearnerType>::parse_table_colname(
    const std::string& _table, const std::string& _colname ) const
{
    if ( _colname.find( helpers::Macros::table() ) == std::string::npos )
        {
            if ( _table.find( helpers::Macros::name() ) == std::string::npos )
                {
                    return std::make_pair( _table, _colname );
                }

            const auto table_end = _colname.find( helpers::Macros::name() );

            const auto table = _colname.substr( 0, table_end );

            return std::make_pair( table, _colname );
        }

    const auto table_begin = _colname.rfind( helpers::Macros::table() ) +
                             helpers::Macros::table().length() + 1;

    const auto table_end = _colname.rfind( helpers::Macros::column() );

    assert_true( table_end >= table_begin );

    const auto table_len = table_end - table_begin;

    const auto table = _colname.substr( table_begin, table_len );

    const auto colname_begin =
        table_end + helpers::Macros::column().length() + 1;

    const auto colname = _colname.substr( colname_begin );

    return std::make_pair( table, colname );
}

// -----------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::string FeatureLearner<FeatureLearnerType>::remove_time_diff(
    const std::string& _from_colname ) const
{
    // --------------------------------------------------------------

    if ( _from_colname.find( helpers::Macros::generated_ts() ) ==
         std::string::npos )
        {
            return _from_colname;
        }

    // --------------------------------------------------------------

    const auto pos = _from_colname.find( "\", '" );

    if ( pos == std::string::npos )
        {
            return _from_colname;
        }

    // --------------------------------------------------------------

    return _from_colname.substr( 0, pos );

    // --------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
containers::Features FeatureLearner<FeatureLearnerType>::transform(
    const Poco::JSON::Object& _cmd,
    const std::vector<size_t>& _index,
    const std::shared_ptr<const communication::SocketLogger>& _logger,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs ) const
{
    const auto [population_df, peripheral_dfs] =
        modify_data_frames( _population_df, _peripheral_dfs );

    const auto [population_table, peripheral_tables] =
        extract_tables_by_colnames( population_df, peripheral_dfs );

    return feature_learner().transform(
        population_table, peripheral_tables, _index, _logger );
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::string FeatureLearner<FeatureLearnerType>::type() const
{
    // ----------------------------------------------------------------------

    constexpr bool is_multirel = std::
        is_same<FeatureLearnerType, multirel::ensemble::DecisionTreeEnsemble>();

    constexpr bool is_multirel_ts =
        std::is_same<FeatureLearnerType, ts::MultirelTimeSeries>();

    constexpr bool is_relboost = std::
        is_same<FeatureLearnerType, relboost::ensemble::DecisionTreeEnsemble>();

    constexpr bool is_relmt = std::
        is_same<FeatureLearnerType, relmt::ensemble::DecisionTreeEnsemble>();

    constexpr bool is_relboost_ts =
        std::is_same<FeatureLearnerType, ts::RelboostTimeSeries>();

    constexpr bool is_relmt_ts =
        std::is_same<FeatureLearnerType, ts::RelMTTimeSeries>();

    // ----------------------------------------------------------------------

    if constexpr ( is_multirel )
        {
            return AbstractFeatureLearner::MULTIREL_MODEL;
        }

    if constexpr ( is_multirel_ts )
        {
            return AbstractFeatureLearner::MULTIREL_TIME_SERIES;
        }

    if constexpr ( is_relboost )
        {
            return AbstractFeatureLearner::RELBOOST_MODEL;
        }

    if constexpr ( is_relmt )
        {
            return AbstractFeatureLearner::RELMT_MODEL;
        }

    if constexpr ( is_relboost_ts )
        {
            return AbstractFeatureLearner::RELBOOST_TIME_SERIES;
        }

    if constexpr ( is_relmt_ts )
        {
            return AbstractFeatureLearner::RELMT_TIME_SERIES;
        }

    // ----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------
}  // namespace featurelearners
}  // namespace engine

// ----------------------------------------------------------------------------

#endif  // ENGINE_FEATURELEARNERS_FEATURELEARNER_HPP_

