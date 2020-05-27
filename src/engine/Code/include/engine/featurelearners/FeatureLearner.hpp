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
    /// Returns the fingerprint of the feature learner (necessary to build
    /// the dependency graphs).
    Poco::JSON::Object::Ptr fingerprint() const final;

    /// Fits the model.
    void fit(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        const Int _target_num,
        Poco::Net::StreamSocket* _socket ) final;

    /// Generate features.
    containers::Features transform(
        const Poco::JSON::Object& _cmd,
        const std::vector<size_t>& _index,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        Poco::Net::StreamSocket* _socket ) const final;

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

    /// Loads the feature learner from a file designated by _fname.
    void load( const std::string& _fname ) final
    {
        const auto obj = load_json_obj( _fname );
        feature_learner_ =
            std::make_optional<FeatureLearnerType>( categories_, obj );
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
        return feature_learner().to_sql( _prefix, 0, _subfeatures );
    }

    // --------------------------------------------------------

   private:
    /// Extracts the necessary containers::DataFrames.
    std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
    extract_data_frames(
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames )
        const;

    /// Extract a data frame of type FeatureLearnerType::DataFrameType from
    /// an engine::containers::DataFrame.
    template <typename DataFrameType>
    DataFrameType extract_table(
        const containers::DataFrame& _df, const Int _target_num ) const;

    /// Extract a data frame of type FeatureLearnerType::DataFrameType from
    /// an engine::containers::DataFrame using the pre-stored schema.
    template <typename DataFrameType, typename SchemaType>
    DataFrameType extract_table_by_colnames(
        const SchemaType& _schema, const containers::DataFrame& _df ) const;

    /// Extract a vector of FeatureLearnerType::DataFrameType from
    /// an engine::containers::DataFrame using the pre-stored schema.
    std::pair<
        typename FeatureLearnerType::DataFrameType,
        std::vector<typename FeatureLearnerType::DataFrameType>>
    extract_tables(
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        const Int _target_num ) const;

    /// Extract a vector of FeatureLearnerType::DataFrameType from
    /// an engine::containers::DataFrame using the pre-stored schema.
    std::pair<
        typename FeatureLearnerType::DataFrameType,
        std::vector<typename FeatureLearnerType::DataFrameType>>
    extract_tables_by_colnames(
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames )
        const;

    /// Initializes the feature learner.
    std::optional<FeatureLearnerType> make_feature_learner(
        const Poco::JSON::Object& _cmd ) const;

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

    if constexpr ( FeatureLearnerType::is_time_series_ )
        {
            return feature_learner().create_data_frames(
                population_df, peripheral_dfs );
        }

    // ------------------------------------------------

    return std::make_pair( population_df, peripheral_dfs );

    // ------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
template <typename DataFrameType>
DataFrameType FeatureLearner<FeatureLearnerType>::extract_table(
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
            case AbstractFeatureLearner::IGNORE_TARGETS:
                break;

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
template <typename DataFrameType, typename SchemaType>
DataFrameType FeatureLearner<FeatureLearnerType>::extract_table_by_colnames(
    const SchemaType& _schema, const containers::DataFrame& _df ) const
{
    // ------------------------------------------------------------------------

    try
        {
            // ------------------------------------------------------------------------

            std::vector<typename DataFrameType::IntColumnType> categoricals;

            for ( size_t i = 0; i < _schema.num_categoricals(); ++i )
                {
                    const auto& name = _schema.categorical_name( i );

                    const auto& mat = _df.categorical( name );

                    categoricals.push_back(
                        typename DataFrameType::IntColumnType(
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

                    discretes.push_back(
                        typename DataFrameType::FloatColumnType(
                            mat.data(), name, mat.nrows(), mat.unit() ) );
                }

            // ------------------------------------------------------------------------

            std::vector<typename DataFrameType::IntColumnType> join_keys;

            for ( size_t i = 0; i < _schema.num_join_keys(); ++i )
                {
                    const auto& name = _schema.join_keys_name( i );

                    if ( FeatureLearnerType::is_time_series_ &&
                         name == "$GETML_SELF_JOIN_KEY" )
                        {
                            continue;
                        }

                    const auto& mat = _df.join_key( name );

                    join_keys.push_back( typename DataFrameType::IntColumnType(
                        mat.data(), name, mat.nrows(), mat.unit() ) );
                }

            // ------------------------------------------------------------------------

            std::vector<typename DataFrameType::FloatColumnType> numericals;

            for ( size_t i = 0; i < _schema.num_numericals(); ++i )
                {
                    const auto& name = _schema.numerical_name( i );

                    const auto& mat = _df.numerical( name );

                    numericals.push_back(
                        typename DataFrameType::FloatColumnType(
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

                            targets.push_back(
                                typename DataFrameType::FloatColumnType(
                                    mat.data(),
                                    name,
                                    mat.nrows(),
                                    mat.unit() ) );
                        }
                }

            // ------------------------------------------------------------------------

            std::vector<typename DataFrameType::FloatColumnType> time_stamps;

            for ( size_t i = 0; i < _schema.num_time_stamps(); ++i )
                {
                    const auto& name = _schema.time_stamps_name( i );

                    if ( FeatureLearnerType::is_time_series_ &&
                         name == "$GETML_ROWID" )
                        {
                            continue;
                        }

                    const auto& mat = _df.time_stamp( name );

                    time_stamps.push_back(
                        typename DataFrameType::FloatColumnType(
                            mat.data(), name, mat.nrows(), mat.unit() ) );
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
    catch ( std::exception& e )
        {
            throw std::invalid_argument(
                std::string( e.what() ) +
                " Is it possible that your peripheral tables are in the "
                "wrong "
                "order?" );
        }

    // ------------------------------------------------------------------------
}
// ------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::pair<
    typename FeatureLearnerType::DataFrameType,
    std::vector<typename FeatureLearnerType::DataFrameType>>
FeatureLearner<FeatureLearnerType>::extract_tables(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const Int _target_num ) const
{
    // ------------------------------------------------

    const auto [population_df, peripheral_dfs] =
        extract_data_frames( _cmd, _data_frames );

    // ------------------------------------------------

    const auto population_table =
        extract_table<typename FeatureLearnerType::DataFrameType>(
            population_df, _target_num );

    // ------------------------------------------------

    std::vector<typename FeatureLearnerType::DataFrameType> peripheral_tables;

    for ( const auto& df : peripheral_dfs )
        {
            const auto table =
                extract_table<typename FeatureLearnerType::DataFrameType>(
                    df, AbstractFeatureLearner::IGNORE_TARGETS );

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
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames ) const
{
    // ------------------------------------------------

    const auto [population_df, peripheral_dfs] =
        extract_data_frames( _cmd, _data_frames );

    // -------------------------------------------------------------------------

    const auto population_schema = feature_learner().population_schema();

    const auto population_table =
        extract_table_by_colnames<typename FeatureLearnerType::DataFrameType>(
            population_schema, population_df );

    // ------------------------------------------------

    const auto peripheral_schema = feature_learner().peripheral_schema();

    if ( peripheral_schema.size() != peripheral_dfs.size() )
        {
            const auto expected = FeatureLearnerType::is_time_series_
                                      ? peripheral_schema.size() - 1
                                      : peripheral_schema.size();

            const auto got = FeatureLearnerType::is_time_series_
                                 ? peripheral_dfs.size() - 1
                                 : peripheral_dfs.size();

            throw std::invalid_argument(
                "Expected " + std::to_string( expected ) +
                " peripheral tables, got " + std::to_string( got ) + "." );
        }

    // ------------------------------------------------

    std::vector<typename FeatureLearnerType::DataFrameType> peripheral_tables;

    for ( size_t i = 0; i < peripheral_schema.size(); ++i )
        {
            const auto df = extract_table_by_colnames<
                typename FeatureLearnerType::DataFrameType>(
                peripheral_schema.at( i ), peripheral_dfs.at( i ) );

            peripheral_tables.push_back( df );
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
    const std::shared_ptr<const monitoring::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const Int _target_num,
    Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------

    const auto [population_table, peripheral_tables] =
        extract_tables( _cmd, _data_frames, _target_num );

    // ------------------------------------------------

    auto new_feature_learner = make_feature_learner( cmd_ );

    new_feature_learner->fit( population_table, peripheral_tables, _logger );

    // ------------------------------------------------

    feature_learner_ = std::move( new_feature_learner );

    // ------------------------------------------------
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
FeatureLearner<FeatureLearnerType>::make_feature_learner(
    const Poco::JSON::Object& _cmd ) const
{
    const auto hyperparameters =
        std::make_shared<typename FeatureLearnerType::HypType>( _cmd );

    const auto placeholder =
        std::make_shared<const typename FeatureLearnerType::PlaceholderType>(
            *placeholder_ );

    auto population_schema =
        std::shared_ptr<const typename FeatureLearnerType::PlaceholderType>();

    auto peripheral_schema = std::shared_ptr<
        const std::vector<typename FeatureLearnerType::PlaceholderType>>();

    return std::make_optional<FeatureLearnerType>(
        categories_,
        hyperparameters,
        peripheral_,
        placeholder,
        peripheral_schema,
        population_schema );
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
containers::Features FeatureLearner<FeatureLearnerType>::transform(
    const Poco::JSON::Object& _cmd,
    const std::vector<size_t>& _index,
    const std::shared_ptr<const monitoring::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    Poco::Net::StreamSocket* _socket ) const
{
    const auto [population_table, peripheral_tables] =
        extract_tables_by_colnames( _cmd, _data_frames );

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

