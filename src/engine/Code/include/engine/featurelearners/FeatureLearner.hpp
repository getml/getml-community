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
    typedef typename FeatureLearnerType::PlaceholderType PlaceholderType;

    // --------------------------------------------------------

   public:
    FeatureLearner(
        const std::shared_ptr<const std::vector<strings::String>>& _categories,
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const Poco::JSON::Object>& _placeholder,
        const std::shared_ptr<const std::vector<std::string>>& _peripheral,
        const std::vector<Poco::JSON::Object::Ptr>& _dependencies )
        : categories_( _categories ),
          cmd_( _cmd ),
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
    /// Calculates the column importances for this ensemble.
    std::map<std::string, Float> column_importances(
        const std::vector<Float>& _importance_factors ) const final;

    /// Returns the fingerprint of the feature learner (necessary to build
    /// the dependency graphs).
    Poco::JSON::Object::Ptr fingerprint() const final;

    /// Fits the model.
    void fit(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const communication::SocketLogger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
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
        const std::map<std::string, containers::DataFrame>& _data_frames )
        const final;

    /// Returns a string describing the type of the feature learner.
    std::string type() const final;

    // --------------------------------------------------------

   public:
    /// Creates a deep copy.
    std::shared_ptr<AbstractFeatureLearner> clone() const final
    {
        return std::make_shared<FeatureLearner<FeatureLearnerType>>( *this );
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
        feature_learner_ =
            std::make_optional<FeatureLearnerType>( categories_, obj );
    }

    /// Returns the placeholder not as passed by the user, but as seen by the
    /// feature learner (the difference matters for time series).
    Poco::JSON::Object make_placeholder() const final
    {
        const auto ptr = make_feature_learner()->placeholder().to_json_obj();
        assert_true( ptr );
        return *ptr;
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

    /// Selects the features according to the index given.
    void select_features( const std::vector<size_t>& _index ) final
    {
        feature_learner().select_features( _index );
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

    /// Returns model as a JSON Object in a form that the monitor can
    /// understand.
    Poco::JSON::Object to_monitor( const std::string& _name ) const final
    {
        return feature_learner().to_monitor( _name );
    }

    /// Return feature learner as SQL code.
    std::vector<std::string> to_sql(
        const std::string& _prefix, const bool _subfeatures ) const final
    {
        auto queries = feature_learner().to_sql( _prefix, 0, _subfeatures );
        for ( auto& query : queries )
            {
                query = replace_macros( query );
            }
        return queries;
    }

    // --------------------------------------------------------

   private:
    /// If this is a time series model, this will add the data necessary for the
    /// self joins. Otherwise, it will just return the original data
    std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
    add_self_joins(
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral ) const;

    /// Extracts upper time stamps from the memory parameter. (Memory is just
    /// syntactic sugar for upper time stamps. The feature learners don't know
    /// about this concept).
    std::vector<containers::DataFrame> add_time_stamps(
        const Poco::JSON::Object _population_placeholder,
        const std::vector<containers::DataFrame>& _peripheral_dfs ) const;

    /// Adds lower and upper time stamps to the data frame.
    void add_ts(
        const Poco::JSON::Object& _joined_table,
        const std::string& _ts_used,
        const std::string& _upper_ts_used,
        const Float _horizon,
        const Float _memory,
        std::vector<containers::DataFrame>* _peripheral_dfs ) const;

    /// Creates the placeholder, including transforming memory into upper time
    /// stamps.
    PlaceholderType create_placeholder( const Poco::JSON::Object& _obj ) const;

    /// Extracts a vector named _name of size _expected_size from the
    /// _population_placeholder
    template <typename T>
    std::vector<T> extract_vector(
        const Poco::JSON::Object& _population_placeholder,
        const std::string& _name,
        const size_t _expected_size ) const;

    /// Extracts the necessary containers::DataFrames.
    std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
    extract_data_frames(
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames )
        const;

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
        const Poco::JSON::Object& _placeholder,
        const size_t _num_peripheral,
        std::vector<bool>* _needs_targets = nullptr ) const;

    /// Initializes the feature learner.
    std::optional<FeatureLearnerType> make_feature_learner() const;

    /// Replaces macros inside the SQL queries with actual code.
    std::string replace_macros( const std::string& _query ) const;

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
    const Poco::JSON::Object& placeholder() const
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

    /// Generates the name for the upper time stamp that is produced using
    /// memory.
    std::string make_ts_name(
        const std::string& _ts_used, const Float _diff ) const
    {
        return "$GETML_GENERATED_TS" + _ts_used + "\"" +
               utils::TSDiffMaker::make_time_stamp_diff( _diff ) +
               "$GETML_REMOVE_CHAR";
    }

    // --------------------------------------------------------

   private:
    /// The categories used for the mapping.
    std::shared_ptr<const std::vector<strings::String>> categories_;

    /// The command used to create the feature learner.
    Poco::JSON::Object cmd_;

    /// The dependencies used to build the fingerprint.
    std::vector<Poco::JSON::Object::Ptr> dependencies_;

    /// The underlying feature learning algorithm.
    std::optional<FeatureLearnerType> feature_learner_;

    /// The placeholder describing the data schema.
    std::shared_ptr<const Poco::JSON::Object> placeholder_;

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
// ------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
FeatureLearner<FeatureLearnerType>::add_self_joins(
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral ) const
{
    return std::make_pair( _population, _peripheral );
}

// ------------------------------------------------------------------------

template <typename FeatureLearnerType>
void FeatureLearner<FeatureLearnerType>::add_ts(
    const Poco::JSON::Object& _joined_table,
    const std::string& _ts_used,
    const std::string& _upper_ts_used,
    const Float _horizon,
    const Float _memory,
    std::vector<containers::DataFrame>* _peripheral_dfs ) const
{
    // ------------------------------------------------------------------------

    if ( _memory > 0.0 && _upper_ts_used != "" )
        {
            throw std::invalid_argument(
                "You can either set an upper time stamp or memory, but not "
                "both!" );
        }

    // ------------------------------------------------------------------------

    assert_true( peripheral().size() == _peripheral_dfs->size() );

    const auto name = JSON::get_value<std::string>( _joined_table, "name_" );

    const auto it = std::find( peripheral().begin(), peripheral().end(), name );

    if ( it == peripheral().end() )
        {
            throw std::invalid_argument(
                "Placeholder named '" + name +
                "' not among the peripheral tables." );
        }

    const auto dist = std::distance( peripheral().begin(), it );

    // ------------------------------------------------------------------------

    auto& df = _peripheral_dfs->at( dist );

    auto cols =
        ts::TimeStampMaker::make_time_stamps( _ts_used, _horizon, _memory, df );

    // ------------------------------------------------------------------------

    assert_true( cols.size() == 0 || cols.size() == 1 || cols.size() == 2 );

    assert_true( _horizon != 0.0 || _memory > 0.0 || cols.size() == 0 );

    assert_true( _horizon == 0.0 || _memory <= 0.0 || cols.size() == 2 );

    if ( _horizon != 0.0 )
        {
            assert_true( cols.size() > 0 );

            cols.at( 0 ).set_name( make_ts_name( _ts_used, _horizon ) );
        }

    if ( _memory > 0.0 )
        {
            assert_true( cols.size() > 0 );

            cols.back().set_name(
                make_ts_name( _ts_used, _horizon + _memory ) );
        }

    // ------------------------------------------------------------------------

    for ( auto& col : cols )
        {
            df.add_float_column( col, "time_stamp" );
        }

    // ------------------------------------------------------------------------
}

// ------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::vector<containers::DataFrame>
FeatureLearner<FeatureLearnerType>::add_time_stamps(
    const Poco::JSON::Object _population_placeholder,
    const std::vector<containers::DataFrame>& _peripheral_dfs ) const
{
    // ------------------------------------------------------------------------

    auto peripheral_dfs = _peripheral_dfs;

    // ------------------------------------------------------------------------

    if ( peripheral().size() != peripheral_dfs.size() )
        {
            throw std::invalid_argument(
                "There must be one peripheral table for every peripheral "
                "placeholder (" +
                std::to_string( peripheral_dfs.size() ) + " vs. " +
                std::to_string( peripheral().size() ) + ")." );
        }

    // ------------------------------------------------------------------------

    const auto joined_tables_arr =
        _population_placeholder.getArray( "joined_tables_" );

    if ( !joined_tables_arr )
        {
            throw std::invalid_argument(
                "The placeholder has no array named 'joined_tables_'!" );
        }

    const auto expected_size = joined_tables_arr->size();

    // ------------------------------------------------------------------------

    const auto other_time_stamps_used = extract_vector<std::string>(
        _population_placeholder, "other_time_stamps_used_", expected_size );

    const auto upper_time_stamps_used = extract_vector<std::string>(
        _population_placeholder, "upper_time_stamps_used_", expected_size );

    const auto horizon = extract_vector<Float>(
        _population_placeholder, "horizon_", expected_size );

    const auto memory = extract_vector<Float>(
        _population_placeholder, "memory_", expected_size );

    // ------------------------------------------------------------------------

    for ( unsigned int i = 0; i < static_cast<unsigned int>( memory.size() );
          ++i )
        {
            const auto joined_table = joined_tables_arr->getObject( i );

            if ( !joined_table )
                {
                    throw std::invalid_argument(
                        "Element " + std::to_string( i ) +
                        " in 'joined_tables_' is not a proper JSON object!" );
                }

            add_ts(
                *joined_table,
                other_time_stamps_used.at( i ),
                upper_time_stamps_used.at( i ),
                horizon.at( i ),
                memory.at( i ),
                &peripheral_dfs );

            peripheral_dfs = add_time_stamps( *joined_table, peripheral_dfs );
        }

    // ------------------------------------------------------------------------

    return peripheral_dfs;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::map<std::string, Float>
FeatureLearner<FeatureLearnerType>::column_importances(
    const std::vector<Float>& _importance_factors ) const
{
    const auto importances =
        feature_learner().column_importances( _importance_factors );

    auto importance_maker = helpers::ImportanceMaker( importances );

    const auto colnames = importance_maker.colnames();

    for ( const auto& from_colname : colnames )
        {
            const auto to_colname = replace_macros( from_colname );

            if ( from_colname != to_colname )
                {
                    importance_maker.transfer( from_colname, to_colname );
                }
        }

    return importance_maker.importances();
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
typename FeatureLearnerType::PlaceholderType
FeatureLearner<FeatureLearnerType>::create_placeholder(
    const Poco::JSON::Object& _obj ) const
{
    // ----------------------------------------------------------

    const auto placeholder = PlaceholderType( _obj );

    // ------------------------------------------------------------------------

    auto other_time_stamps_used = placeholder.other_time_stamps_used_;

    auto upper_time_stamps_used = placeholder.upper_time_stamps_used_;

    // ------------------------------------------------------------------------

    const auto joined_tables_arr = _obj.getArray( "joined_tables_" );

    if ( !joined_tables_arr )
        {
            throw std::invalid_argument(
                "The placeholder has no array named 'joined_tables_'!" );
        }

    const auto expected_size = joined_tables_arr->size();

    // ------------------------------------------------------------------------

    const auto horizon =
        extract_vector<Float>( _obj, "horizon_", expected_size );

    const auto memory = extract_vector<Float>( _obj, "memory_", expected_size );

    // ------------------------------------------------------------------------

    assert_true( placeholder.other_time_stamps_used_.size() == expected_size );

    assert_true( horizon.size() == expected_size );

    assert_true( memory.size() == expected_size );

    // ------------------------------------------------------------------------

    for ( size_t i = 0; i < horizon.size(); ++i )
        {
            if ( horizon.at( i ) == 0.0 )
                {
                    continue;
                }

            other_time_stamps_used.at( i ) = make_ts_name(
                placeholder.other_time_stamps_used_.at( i ), horizon.at( i ) );
        }

    // ------------------------------------------------------------------------

    for ( size_t i = 0; i < memory.size(); ++i )
        {
            if ( memory.at( i ) <= 0.0 )
                {
                    continue;
                }

            if ( upper_time_stamps_used.at( i ) != "" )
                {
                    throw std::invalid_argument(
                        "You can either set an upper time stamp or "
                        "memory, but not both!" );
                }

            upper_time_stamps_used.at( i ) = make_ts_name(
                placeholder.other_time_stamps_used_.at( i ),
                horizon.at( i ) + memory.at( i ) );
        }

    // ------------------------------------------------------------------------

    std::vector<PlaceholderType> joined_tables;

    for ( size_t i = 0; i < expected_size; ++i )
        {
            const auto joined_table_obj =
                joined_tables_arr->getObject( static_cast<unsigned int>( i ) );

            if ( !joined_table_obj )
                {
                    throw std::invalid_argument(
                        "Element " + std::to_string( i ) +
                        " in 'joined_tables_' is not a proper JSON object!" );
                }

            joined_tables.push_back( create_placeholder( *joined_table_obj ) );
        }

    // ------------------------------------------------------------------------

    return PlaceholderType(
        placeholder.allow_lagged_targets_,
        joined_tables,
        placeholder.join_keys_used_,
        placeholder.name(),
        placeholder.other_join_keys_used_,
        other_time_stamps_used,
        placeholder.time_stamps_used_,
        upper_time_stamps_used );

    // ------------------------------------------------------------------------
}

// ------------------------------------------------------------------------

template <typename FeatureLearnerType>
template <typename T>
std::vector<T> FeatureLearner<FeatureLearnerType>::extract_vector(
    const Poco::JSON::Object& _population_placeholder,
    const std::string& _name,
    const size_t _expected_size ) const
{
    // ------------------------------------------------------------------------

    if ( !_population_placeholder.getArray( _name ) )
        {
            throw std::invalid_argument(
                "The placeholder has no array named '" + _name + "'!" );
        }

    const auto vec =
        JSON::array_to_vector<T>( _population_placeholder.getArray( _name ) );

    // ------------------------------------------------------------------------

    if ( vec.size() != _expected_size )
        {
            throw std::invalid_argument(
                "Size of '" + _name + "' unexpected. Expected " +
                std::to_string( _expected_size ) + ", got " +
                std::to_string( vec.size() ) + "." );
        }

    // ------------------------------------------------------------------------

    return vec;

    // ------------------------------------------------------------------------
}

// ------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
FeatureLearner<FeatureLearnerType>::extract_data_frames(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames ) const
{
    // ------------------------------------------------

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    // ------------------------------------------------

    const auto peripheral_names = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "peripheral_names_" ) );

    std::vector<containers::DataFrame> peripheral_dfs;

    for ( auto& name : peripheral_names )
        {
            peripheral_dfs.emplace_back(
                utils::Getter::get( name, _data_frames ) );
        }

    // ------------------------------------------------

    return modify_data_frames( population_df, peripheral_dfs );

    // ------------------------------------------------
}

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

    const auto placeholder_ptr =
        Poco::JSON::Object::Ptr( new Poco::JSON::Object( placeholder() ) );

    obj->set( "cmd_", cmd_ );

    obj->set( "dependencies_", JSON::vector_to_array_ptr( dependencies_ ) );

    obj->set( "peripheral_", JSON::vector_to_array_ptr( peripheral() ) );

    obj->set( "placeholder_", placeholder_ptr );

    return obj;
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
void FeatureLearner<FeatureLearnerType>::fit(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const communication::SocketLogger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const Int _target_num )
{
    // ------------------------------------------------

    const auto [population_df, peripheral_dfs] =
        extract_data_frames( _cmd, _data_frames );

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
    const Poco::JSON::Object& _placeholder,
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

    const auto joined_tables_arr = _placeholder.getArray( "joined_tables_" );

    if ( !joined_tables_arr )
        {
            throw std::invalid_argument(
                "The placeholder has no array named 'joined_tables_'!" );
        }

    const auto expected_size = joined_tables_arr->size();

    // ------------------------------------------------------------------------

    const auto allow_lagged_targets = extract_vector<bool>(
        _placeholder, "allow_lagged_targets_", expected_size );

    // ------------------------------------------------------------------------

    for ( size_t i = 0; i < expected_size; ++i )
        {
            const auto joined_table = joined_tables_arr->getObject( i );

            if ( !joined_table )
                {
                    throw std::invalid_argument(
                        "Element " + std::to_string( i ) +
                        " in 'joined_tables_' is not a proper JSON object!" );
                }

            if ( allow_lagged_targets.at( i ) )
                {
                    const auto name =
                        JSON::get_value<std::string>( *joined_table, "name_" );

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
                *joined_table, _num_peripheral, &needs_targets );
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

    const auto placeholder_struct = std::make_shared<PlaceholderType>(
        create_placeholder( placeholder() ) );

    const auto population_schema = std::shared_ptr<const PlaceholderType>();

    const auto peripheral_schema =
        std::shared_ptr<const std::vector<PlaceholderType>>();

    return std::make_optional<FeatureLearnerType>(
        categories_,
        hyperparameters,
        peripheral_,
        placeholder_struct,
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
    // ------------------------------------------------

    const auto peripheral_dfs =
        add_time_stamps( placeholder(), _peripheral_dfs );

    // ------------------------------------------------

    if constexpr ( FeatureLearnerType::is_time_series_ )
        {
            return make_feature_learner()->create_data_frames(
                _population_df, peripheral_dfs );
        }

    // ------------------------------------------------

    return std::make_pair( _population_df, peripheral_dfs );

    // ------------------------------------------------
}

// -----------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::string FeatureLearner<FeatureLearnerType>::replace_macros(
    const std::string& _query ) const
{
    // --------------------------------------------------------------

    auto new_query =
        utils::StringReplacer::replace_all( _query, "$GETML_GENERATED_TS", "" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "$GETML_REMOVE_CHAR\"", "" );

    // --------------------------------------------------------------

    return new_query;

    // --------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
containers::Features FeatureLearner<FeatureLearnerType>::transform(
    const Poco::JSON::Object& _cmd,
    const std::vector<size_t>& _index,
    const std::shared_ptr<const communication::SocketLogger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames ) const
{
    const auto [population_df, peripheral_dfs] =
        extract_data_frames( _cmd, _data_frames );

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

    constexpr bool is_relboost_ts =
        std::is_same<FeatureLearnerType, ts::RelboostTimeSeries>();

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

    if constexpr ( is_relboost_ts )
        {
            return AbstractFeatureLearner::RELBOOST_TIME_SERIES;
        }

    // ----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------
}  // namespace featurelearners
}  // namespace engine

// ----------------------------------------------------------------------------

// ----------------------------------------------------------------------------

#endif  // ENGINE_FEATURELEARNERS_FEATURELEARNER_HPP_

