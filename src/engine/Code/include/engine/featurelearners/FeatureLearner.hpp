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
   private:
    /// Whether this is a FastProp algorithm
    static constexpr bool is_fastprop_ =
        std::is_same<FeatureLearnerType, fastprop::algorithm::FastProp>();

    /// Whether this is multirel.
    static constexpr bool is_multirel_ = std::
        is_same<FeatureLearnerType, multirel::ensemble::DecisionTreeEnsemble>();

    /// Whether this is relboost.
    static constexpr bool is_relboost_ = std::
        is_same<FeatureLearnerType, relboost::ensemble::DecisionTreeEnsemble>();

    /// Whether this is relmt.
    static constexpr bool is_relmt_ = std::
        is_same<FeatureLearnerType, relmt::ensemble::DecisionTreeEnsemble>();

    /// Because FastProp are propositionalization
    /// approaches themselves, they do not have a propositionalization
    /// subfeature.
    static constexpr bool has_propositionalization_ = !is_fastprop_;

    // --------------------------------------------------------

   private:
    typedef typename FeatureLearnerType::DataFrameType DataFrameType;
    typedef typename FeatureLearnerType::HypType HypType;

    typedef typename std::conditional<
        has_propositionalization_,
        std::shared_ptr<const fastprop::Hyperparameters>,
        int>::type PropType;

    // --------------------------------------------------------

   public:
    FeatureLearner( const FeatureLearnerParams& _params )
        : cmd_( _params.cmd_ ),
          dependencies_( _params.dependencies_ ),
          peripheral_( _params.peripheral_ ),
          peripheral_schema_( _params.peripheral_schema_ ),
          placeholder_( _params.placeholder_ ),
          population_schema_( _params.population_schema_ ),
          target_num_( _params.target_num_ )
    {
        assert_true( placeholder_ );
        assert_true( peripheral_ );
    }

    ~FeatureLearner() = default;

    // --------------------------------------------------------

   public:
    /// Calculates the column importances for this ensemble.
    std::map<helpers::ColumnDescription, Float> column_importances(
        const std::vector<Float>& _importance_factors ) const final;

    /// Returns the fingerprint of the feature learner (necessary to build
    /// the dependency graphs).
    Poco::JSON::Object::Ptr fingerprint() const final;

    /// Fits the model.
    void fit(
        const Poco::JSON::Object& _cmd,
        const std::string& _prefix,
        const std::shared_ptr<const communication::SocketLogger>& _logger,
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs ) final;

    /// Loads the feature learner from a file designated by _fname.
    void load( const std::string& _fname ) final;

    /// Saves the feature learner in JSON format, if applicable
    void save( const std::string& _fname ) const final;

    /// Return model as a JSON Object.
    Poco::JSON::Object to_json_obj( const bool _schema_only ) const final;

    /// Return feature learner as SQL code.
    std::vector<std::string> to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories,
        const bool _targets,
        const bool _subfeatures,
        const std::shared_ptr<const helpers::SQLDialectGenerator>&
            _sql_dialect_generator,
        const std::string& _prefix ) const final;

    /// Generate features.
    containers::Features transform(
        const Poco::JSON::Object& _cmd,
        const std::vector<size_t>& _index,
        const std::string& _prefix,
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

    /// Whether the feature learner is used for classification.
    bool is_classification() const final
    {
        const auto loss_function =
            JSON::get_value<std::string>( cmd_, "loss_function_" );
        return ( loss_function != "SquareLoss" );
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

    /// Determines whether the population table needs targets during
    /// transform (only for time series that include autoregression).
    bool population_needs_targets() const final { return false; }

    /// Whether the feature learner is for the premium version only.
    bool premium_only() const final
    {
        return FeatureLearnerType::premium_only_;
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

    // --------------------------------------------------------

   private:
    /// Extract a data frame of type FeatureLearnerType::DataFrameType from
    /// an engine::containers::DataFrame using the pre-stored schema.
    template <typename SchemaType>
    DataFrameType extract_table_by_colnames(
        const SchemaType& _schema,
        const containers::DataFrame& _df,
        const Int _target_num,
        const bool _apply_subroles ) const;

    /// Extract a vector of FeatureLearnerType::DataFrameType from
    /// an engine::containers::DataFrame using the pre-stored schema.
    std::pair<DataFrameType, std::vector<DataFrameType>>
    extract_tables_by_colnames(
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs,
        const helpers::Schema& _population_schema,
        const std::vector<helpers::Schema>& _peripheral_schema,
        const bool _apply_subroles,
        const bool _population_needs_targets ) const;

    /// Fits the propositionalization, if applicable.
    std::optional<std::pair<
        std::shared_ptr<const fastprop::subfeatures::FastPropContainer>,
        helpers::FeatureContainer>>
    fit_propositionalization(
        const DataFrameType& _population,
        const std::vector<DataFrameType>& _peripheral,
        const helpers::RowIndexContainer& _row_indices,
        const helpers::WordIndexContainer& _word_indices,
        const std::string& _prefix,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const FeatureLearnerType& _feature_learner ) const;

    /// Splits the text fields, if necessary and trains the RowIndexContainer
    /// and WordIndexContainer.
    std::tuple<
        typename FeatureLearnerType::DataFrameType,
        std::vector<typename FeatureLearnerType::DataFrameType>,
        std::shared_ptr<const helpers::VocabularyContainer>,
        helpers::RowIndexContainer,
        helpers::WordIndexContainer>
    handle_text_fields(
        const DataFrameType& _population,
        const std::vector<DataFrameType>& _peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger ) const;

    /// Initializes the feature learner.
    std::optional<FeatureLearnerType> make_feature_learner() const;

    /// Interprets the subroles and indicates whether the column they belong to
    /// should be included.
    bool parse_subroles( const std::vector<std::string>& _subroles ) const;

    /// Extracts the table and column name, if they are from a many-to-one
    /// join, needed for the column importances.
    std::pair<std::string, std::string> parse_table_colname(
        const std::string& _table, const std::string& _colname ) const;

    /// Transforms the propositionalization features to SQL.
    void propositionalization_to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories,
        const helpers::VocabularyTree& _vocabulary,
        const std::shared_ptr<const helpers::SQLDialectGenerator>&
            _sql_dialect_generator,
        const std::string& _prefix,
        const bool _subfeatures,
        std::vector<std::string>* _sql ) const;

    /// Removes the time difference marker from the colnames, needed for the
    /// column importances.
    std::string remove_time_diff( const std::string& _from_colname ) const;

    /// Helper function for loading a json object.
    Poco::JSON::Object load_json_obj( const std::string& _fname ) const;

    /// Transforms the proppsitionalization.
    std::optional<const helpers::FeatureContainer>
    transform_propositionalization(
        const DataFrameType& _population,
        const std::vector<DataFrameType>& _peripheral,
        const helpers::WordIndexContainer& _word_indices,
        const std::string& _prefix,
        const std::shared_ptr<const logging::AbstractLogger> _logger ) const;

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

    /// Infers whether we need the targets of a peripheral table.
    std::vector<bool> infer_needs_targets() const
    {
        auto needs_targets = placeholder().infer_needs_targets( peripheral() );

        if ( peripheral_schema().size() > needs_targets.size() )
            {
                needs_targets.insert(
                    needs_targets.end(),
                    peripheral_schema().size() - needs_targets.size(),
                    population_needs_targets() );
            }

        return needs_targets;
    }

    /// The minimum document frequency used for the vocabulary.
    size_t min_df( const HypType& _hyp ) const { return _hyp.min_df_; }

    /// Trivial accessor.
    const std::vector<std::string>& peripheral() const
    {
        assert_true( peripheral_ );
        return *peripheral_;
    }

    /// Trivial accessor.
    std::vector<helpers::Schema> peripheral_schema() const
    {
        assert_true( peripheral_schema_ );

        return *peripheral_schema_;
    }

    /// Trivial accessor.
    const helpers::Placeholder& placeholder() const
    {
        assert_true( placeholder_ );
        return *placeholder_;
    }

    /// Trivial accessor.
    helpers::Schema population_schema() const
    {
        assert_true( population_schema_ );

        return *population_schema_;
    }

    /// Extracts the propositionalization from the hyperparameters, if they
    /// exist.
    PropType propositionalization( const HypType& _hyp ) const
    {
        if constexpr ( has_propositionalization_ )
            {
                return _hyp.propositionalization_;
            }

        if constexpr ( !has_propositionalization_ )
            {
                return 0;
            }
    }

    /// Extracts the propositionalization features.
    PropType propositionalization() const
    {
        return propositionalization( feature_learner().hyperparameters() );
    }

    /// The size of the vocabulary.
    size_t vocab_size( const HypType& _hyp ) const { return _hyp.vocab_size_; }

    // --------------------------------------------------------

   private:
    /// The command used to create the feature learner.
    Poco::JSON::Object cmd_;

    /// The dependencies used to build the fingerprint.
    std::vector<Poco::JSON::Object::Ptr> dependencies_;

    /// The containers for the propositionalization.
    std::shared_ptr<const fastprop::subfeatures::FastPropContainer>
        fast_prop_container_;

    /// The underlying feature learning algorithm.
    std::optional<FeatureLearnerType> feature_learner_;

    /// The names of the peripheral tables
    std::shared_ptr<const std::vector<std::string>> peripheral_;

    /// The schema of the peripheral tables.
    std::shared_ptr<const std::vector<helpers::Schema>> peripheral_schema_;

    /// The placeholder describing the data schema.
    std::shared_ptr<const helpers::Placeholder> placeholder_;

    /// The schema of the population table.
    std::shared_ptr<const helpers::Schema> population_schema_;

    /// Indicates which target to use
    Int target_num_;

    /// The vocabulary used for the text fields.
    std::shared_ptr<const helpers::VocabularyContainer> vocabulary_;

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
std::map<helpers::ColumnDescription, Float>
FeatureLearner<FeatureLearnerType>::column_importances(
    const std::vector<Float>& _importance_factors ) const
{
    const auto is_non_zero = []( const auto& p ) -> bool {
        return p.second > 0.0;
    };

    const auto filter_non_zeros = [is_non_zero]( const auto& _importances ) {
        return stl::collect::map<helpers::ColumnDescription, Float>(
            _importances | VIEWS::filter( is_non_zero ) );
    };

    if constexpr ( !has_propositionalization_ )
        {
            return filter_non_zeros( feature_learner().column_importances(
                _importance_factors, false ) );
        }

    if constexpr ( has_propositionalization_ )
        {
            assert_true( fast_prop_container_ );
            return filter_non_zeros( feature_learner().column_importances(
                _importance_factors, *fast_prop_container_, false ) );
        }
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
template <typename SchemaType>
typename FeatureLearnerType::DataFrameType
FeatureLearner<FeatureLearnerType>::extract_table_by_colnames(
    const SchemaType& _schema,
    const containers::DataFrame& _df,
    const Int _target_num,
    const bool _apply_subroles ) const
{
    // ------------------------------------------------------------------------

    assert_true(
        _target_num < 0 ||
        static_cast<size_t>( _target_num ) < _schema.targets_.size() );

    const auto include_target =
        [&_df, _target_num, &_schema]( const std::string& _name ) -> bool {
        if ( _target_num == AbstractFeatureLearner::IGNORE_TARGETS )
            {
                return false;
            }

        if ( _target_num >= 0 && _name != _schema.targets_.at( _target_num ) )
            {
                return false;
            }

        const bool exists = _df.has_target( _name );

        if ( exists )
            {
                return true;
            }

        throw std::invalid_argument(
            "Target '" + _name + "' not found in data frame '" + _df.name() +
            "', but is required to generate the "
            "prediction. This is because you have set "
            "allow_lagged_targets to True." );

        return false;
    };

    const auto targets = stl::collect::vector<std::string>(
        _schema.targets_ | VIEWS::filter( include_target ) );

    // ------------------------------------------------------------------------

    const auto include = [this, &_df]( const std::string& _colname ) -> bool {
        return parse_subroles( _df.subroles( _colname ) );
    };

    // ------------------------------------------------------------------------

    const auto categoricals =
        _apply_subroles ? stl::collect::vector<std::string>(
                              _schema.categoricals_ | VIEWS::filter( include ) )
                        : _schema.categoricals_;

    const auto discretes =
        _apply_subroles ? stl::collect::vector<std::string>(
                              _schema.discretes_ | VIEWS::filter( include ) )
                        : _schema.discretes_;

    const auto numericals =
        _apply_subroles ? stl::collect::vector<std::string>(
                              _schema.numericals_ | VIEWS::filter( include ) )
                        : _schema.numericals_;

    const auto text = _apply_subroles
                          ? stl::collect::vector<std::string>(
                                _schema.text_ | VIEWS::filter( include ) )
                          : _schema.text_;

    // ------------------------------------------------------------------------

    const auto schema = containers::Schema{
        .categoricals_ = categoricals,
        .discretes_ = discretes,
        .join_keys_ = _schema.join_keys_,
        .numericals_ = numericals,
        .targets_ = targets,
        .text_ = text,
        .time_stamps_ = _schema.time_stamps_,
        .unused_floats_ = _schema.unused_floats_,
        .unused_strings_ = _schema.unused_strings_ };

    // ------------------------------------------------------------------------

    return _df.to_immutable<typename FeatureLearnerType::DataFrameType>(
        schema );

    // ------------------------------------------------------------------------
}

// ------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::pair<
    typename FeatureLearnerType::DataFrameType,
    std::vector<typename FeatureLearnerType::DataFrameType>>
FeatureLearner<FeatureLearnerType>::extract_tables_by_colnames(
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const helpers::Schema& _population_schema,
    const std::vector<helpers::Schema>& _peripheral_schema,
    const bool _apply_subroles,
    const bool _population_needs_targets ) const
{
    // -------------------------------------------------------------------------

    const auto population_table = extract_table_by_colnames(
        _population_schema,
        _population_df,
        _population_needs_targets ? target_num_
                                  : AbstractFeatureLearner::IGNORE_TARGETS,
        _apply_subroles );

    // ------------------------------------------------

    if ( _peripheral_schema.size() != _peripheral_dfs.size() )
        {
            throw std::invalid_argument(
                "Expected " + std::to_string( _peripheral_schema.size() ) +
                " peripheral tables, got " +
                std::to_string( _peripheral_dfs.size() ) + "." );
        }

    // ------------------------------------------------

    const auto peripheral_needs_targets = infer_needs_targets();

    assert_true( peripheral_needs_targets.size() == _peripheral_schema.size() );

    // ------------------------------------------------

    const auto to_peripheral =
        [this,
         &peripheral_needs_targets,
         &_peripheral_schema,
         &_peripheral_dfs,
         _apply_subroles]( const size_t _i ) -> DataFrameType {
        const auto t =
            ( peripheral_needs_targets.at( _i )
                  ? AbstractFeatureLearner::USE_ALL_TARGETS
                  : AbstractFeatureLearner::IGNORE_TARGETS );

        return extract_table_by_colnames(
            _peripheral_schema.at( _i ),
            _peripheral_dfs.at( _i ),
            t,
            _apply_subroles );
    };

    const auto iota = stl::iota<size_t>( 0, _peripheral_schema.size() );

    const auto peripheral_tables = stl::collect::vector<DataFrameType>(
        iota | VIEWS::transform( to_peripheral ) );

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

    obj->set( "target_num_", target_num_ );

    return obj;
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
void FeatureLearner<FeatureLearnerType>::fit(
    const Poco::JSON::Object& _cmd,
    const std::string& _prefix,
    const std::shared_ptr<const communication::SocketLogger>& _logger,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs )
{
    // ------------------------------------------------

    feature_learner_ = make_feature_learner();

    // ------------------------------------------------

    const auto [population_table, peripheral_tables] =
        extract_tables_by_colnames(
            _population_df,
            _peripheral_dfs,
            population_schema(),
            peripheral_schema(),
            true,
            true );

    const auto [population, peripheral, vocabulary, row_indices, word_indices] =
        handle_text_fields( population_table, peripheral_tables, _logger );

    // ------------------------------------------------

    assert_true( feature_learner_ );

    const auto prop_pair = fit_propositionalization(
        population,
        peripheral,
        row_indices,
        word_indices,
        _prefix,
        _logger,
        feature_learner_.value() );

    // ------------------------------------------------

    const auto params = typename FeatureLearnerType::FitParamsType{
        .feature_container_ =
            prop_pair ? prop_pair->second
                      : std::optional<const helpers::FeatureContainer>(),
        .logger_ = _logger,
        .peripheral_ = peripheral,
        .population_ = population,
        .row_indices_ = row_indices,
        .word_indices_ = word_indices };

    feature_learner_->fit( params );

    // ------------------------------------------------

    fast_prop_container_ =
        prop_pair
            ? prop_pair->first
            : std::shared_ptr<const fastprop::subfeatures::FastPropContainer>();

    vocabulary_ = vocabulary;

    // ------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::optional<std::pair<
    std::shared_ptr<const fastprop::subfeatures::FastPropContainer>,
    helpers::FeatureContainer>>
FeatureLearner<FeatureLearnerType>::fit_propositionalization(
    const DataFrameType& _population,
    const std::vector<DataFrameType>& _peripheral,
    const helpers::RowIndexContainer& _row_indices,
    const helpers::WordIndexContainer& _word_indices,
    const std::string& _prefix,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const FeatureLearnerType& _feature_learner ) const
{
    if constexpr ( has_propositionalization_ )
        {
            const auto is_true = []( const bool _val ) { return _val; };

            const bool all_propositionalization =
                ( _feature_learner.placeholder().propositionalization().size() >
                  0 ) &&
                std::all_of(
                    _feature_learner.placeholder()
                        .propositionalization()
                        .begin(),
                    _feature_learner.placeholder().propositionalization().end(),
                    is_true );

            if ( all_propositionalization )
                {
                    throw std::invalid_argument(
                        "All joins in the data model have been set to "
                        "propositionalization. You should use FastProp "
                        "instead." );
                }

            const auto hyp =
                propositionalization( _feature_learner.hyperparameters() );

            if ( !hyp )
                {
                    return std::nullopt;
                }

            const auto peripheral_names =
                std::make_shared<const std::vector<std::string>>(
                    _feature_learner.peripheral() );

            using MakerParams = fastprop::subfeatures::MakerParams;

            const auto params = MakerParams{
                .hyperparameters_ = hyp,
                .logger_ = _logger,
                .peripheral_ = _peripheral,
                .peripheral_names_ = peripheral_names,
                .placeholder_ = _feature_learner.placeholder(),
                .population_ = _population,
                .prefix_ = _prefix,
                .row_index_container_ = _row_indices,
                .word_index_container_ = _word_indices };

            return fastprop::subfeatures::Maker::fit( params );
        }

    return std::nullopt;
}

// ------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::tuple<
    typename FeatureLearnerType::DataFrameType,
    std::vector<typename FeatureLearnerType::DataFrameType>,
    std::shared_ptr<const helpers::VocabularyContainer>,
    helpers::RowIndexContainer,
    helpers::WordIndexContainer>
FeatureLearner<FeatureLearnerType>::handle_text_fields(
    const DataFrameType& _population,
    const std::vector<DataFrameType>& _peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger ) const
{
    assert_true( _logger );

    const auto hyperparameters =
        std::make_shared<typename FeatureLearnerType::HypType>( cmd_ );

    const auto has_text_fields = []( const helpers::DataFrame& _df ) -> bool {
        return _df.num_text() > 0;
    };

    const bool any_text_fields =
        has_text_fields( _population ) ||
        std::any_of( _peripheral.begin(), _peripheral.end(), has_text_fields );

    if ( any_text_fields ) _logger->log( "Indexing text fields..." );

    const auto vocabulary =
        std::make_shared<const helpers::VocabularyContainer>(
            min_df( *hyperparameters ),
            vocab_size( *hyperparameters ),
            _population,
            _peripheral );

#ifndef NDEBUG
    assert_true( _population.num_text() == vocabulary->population().size() );

    assert_true( _peripheral.size() == vocabulary->peripheral().size() );

    for ( size_t i = 0; i < _peripheral.size(); ++i )
        {
            assert_true(
                _peripheral.at( i ).num_text() ==
                vocabulary->peripheral().at( i ).size() );
        }
#endif

    if ( any_text_fields ) _logger->log( "Progress: 33%." );

    const auto word_indices =
        helpers::WordIndexContainer( _population, _peripheral, *vocabulary );

    if ( any_text_fields ) _logger->log( "Progress: 66%." );

    const auto row_indices = helpers::RowIndexContainer( word_indices );

    if ( any_text_fields ) _logger->log( "Progress: 100%." );

    return std::make_tuple(
        _population, _peripheral, vocabulary, row_indices, word_indices );
}

// ------------------------------------------------------------------------

template <typename FeatureLearnerType>
void FeatureLearner<FeatureLearnerType>::load( const std::string& _fname )
{
    const auto obj = load_json_obj( _fname );

    feature_learner_ = std::make_optional<FeatureLearnerType>( obj );

    if ( obj.has( "fast_prop_container_" ) )
        {
            fast_prop_container_ = std::make_shared<
                const fastprop::subfeatures::FastPropContainer>(
                *JSON::get_object( obj, "fast_prop_container_" ) );
        }

    if ( obj.has( "vocabulary_" ) )
        {
            vocabulary_ = std::make_shared<const helpers::VocabularyContainer>(
                *JSON::get_object( obj, "vocabulary_" ) );
        }

    target_num_ = JSON::get_value<Int>( obj, "target_num_" );
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

    return std::make_optional<FeatureLearnerType>(
        hyperparameters, peripheral_, placeholder_ );
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
bool FeatureLearner<FeatureLearnerType>::parse_subroles(
    const std::vector<std::string>& _subroles ) const
{
    auto blacklist = std::vector<helpers::Subrole>(
        { helpers::Subrole::exclude_feature_learners,
          helpers::Subrole::email_only,
          helpers::Subrole::substring_only } );

    if constexpr ( is_fastprop_ )
        {
            blacklist.push_back( helpers::Subrole::exclude_fastprop );
            return !helpers::SubroleParser::contains_any(
                _subroles, blacklist );
        }

    if constexpr ( is_multirel_ )
        {
            blacklist.push_back( helpers::Subrole::exclude_multirel );
            return !helpers::SubroleParser::contains_any(
                _subroles, blacklist );
        }

    if constexpr ( is_relboost_ )
        {
            blacklist.push_back( helpers::Subrole::exclude_relboost );
            return !helpers::SubroleParser::contains_any(
                _subroles, blacklist );
        }

    if constexpr ( is_relmt_ )
        {
            blacklist.push_back( helpers::Subrole::exclude_relmt );
            return !helpers::SubroleParser::contains_any(
                _subroles, blacklist );
        }

    assert_true( false );

    return true;
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

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
void FeatureLearner<FeatureLearnerType>::propositionalization_to_sql(
    const std::shared_ptr<const std::vector<strings::String>>& _categories,
    const helpers::VocabularyTree& _vocabulary,
    const std::shared_ptr<const helpers::SQLDialectGenerator>&
        _sql_dialect_generator,
    const std::string& _prefix,
    const bool _subfeatures,
    std::vector<std::string>* _sql ) const
{
    if constexpr ( has_propositionalization_ )
        {
            if ( !fast_prop_container_ )
                {
                    return;
                }

            fast_prop_container_->to_sql(
                _categories,
                _vocabulary,
                _sql_dialect_generator,
                _prefix,
                _subfeatures,
                _sql );
        }
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
void FeatureLearner<FeatureLearnerType>::save( const std::string& _fname ) const
{
    std::ofstream fs( _fname, std::ofstream::out );
    Poco::JSON::Stringifier::stringify( to_json_obj( false ), fs );
    fs.close();
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
Poco::JSON::Object FeatureLearner<FeatureLearnerType>::to_json_obj(
    const bool _schema_only ) const
{
    auto obj = feature_learner().to_json_obj( _schema_only );

    if ( _schema_only )
        {
            return obj;
        }

    if ( fast_prop_container_ )
        {
            obj.set(
                "fast_prop_container_", fast_prop_container_->to_json_obj() );
        }

    if ( vocabulary_ )
        {
            obj.set( "vocabulary_", vocabulary_->to_json_obj() );
        }

    obj.set( "target_num_", target_num_ );

    return obj;
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::vector<std::string> FeatureLearner<FeatureLearnerType>::to_sql(
    const std::shared_ptr<const std::vector<strings::String>>& _categories,
    const bool _targets,
    const bool _subfeatures,
    const std::shared_ptr<const helpers::SQLDialectGenerator>&
        _sql_dialect_generator,
    const std::string& _prefix ) const
{
    throw_unless( peripheral_schema_, "Pipeline has not been fitted." );

    std::vector<std::string> sql;

    throw_unless( vocabulary_, "Pipeline has not been fitted." );

    const auto vocabulary_tree = helpers::VocabularyTree(
        vocabulary_->population(),
        vocabulary_->peripheral(),
        feature_learner().placeholder(),
        feature_learner().peripheral() );

    propositionalization_to_sql(
        _categories,
        vocabulary_tree,
        _sql_dialect_generator,
        _prefix,
        _subfeatures,
        &sql );

    const auto features = feature_learner().to_sql(
        _categories,
        vocabulary_tree,
        _sql_dialect_generator,
        _prefix,
        0,
        _subfeatures );

    sql.insert( sql.end(), features.begin(), features.end() );

    return sql;
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
containers::Features FeatureLearner<FeatureLearnerType>::transform(
    const Poco::JSON::Object& _cmd,
    const std::vector<size_t>& _index,
    const std::string& _prefix,
    const std::shared_ptr<const communication::SocketLogger>& _logger,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs ) const
{
    // -------------------------------------------------------

    const auto [population, peripheral] = extract_tables_by_colnames(
        _population_df,
        _peripheral_dfs,
        feature_learner().population_schema(),
        feature_learner().peripheral_schema(),
        false,
        population_needs_targets() );

    assert_true( vocabulary_ );

    const auto word_indices =
        helpers::WordIndexContainer( population, peripheral, *vocabulary_ );

    const auto feature_container = transform_propositionalization(
        population, peripheral, word_indices, _prefix, _logger );

    // -------------------------------------------------------

    using TransformParams = typename FeatureLearnerType::TransformParamsType;

    const auto params = TransformParams{
        .feature_container_ = feature_container,
        .index_ = _index,
        .logger_ = _logger,
        .peripheral_ = peripheral,
        .population_ = population,
        .word_indices_ = word_indices };

    return feature_learner().transform( params );
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::optional<const helpers::FeatureContainer>
FeatureLearner<FeatureLearnerType>::transform_propositionalization(
    const DataFrameType& _population,
    const std::vector<DataFrameType>& _peripheral,
    const helpers::WordIndexContainer& _word_indices,
    const std::string& _prefix,
    const std::shared_ptr<const logging::AbstractLogger> _logger ) const
{
    if constexpr ( has_propositionalization_ )
        {
            if ( !propositionalization() )
                {
                    return std::nullopt;
                }

            assert_true( fast_prop_container_ );

            const auto peripheral_names =
                std::make_shared<const std::vector<std::string>>(
                    feature_learner().peripheral() );

            using MakerParams = fastprop::subfeatures::MakerParams;

            assert_true( _prefix != "" );

            const auto params = MakerParams{
                .fast_prop_container_ = fast_prop_container_,
                .hyperparameters_ = propositionalization(),
                .logger_ = _logger,
                .peripheral_ = _peripheral,
                .peripheral_names_ = peripheral_names,
                .placeholder_ = feature_learner().placeholder(),
                .population_ = _population,
                .prefix_ = _prefix,
                .word_index_container_ = _word_indices };

            const auto feature_container =
                fastprop::subfeatures::Maker::transform( params );

            return feature_container;
        }

    return std::nullopt;
}

// ----------------------------------------------------------------------------

template <typename FeatureLearnerType>
std::string FeatureLearner<FeatureLearnerType>::type() const
{
    // ----------------------------------------------------------------------

    constexpr bool unknown_feature_learner =
        !is_fastprop_ && !is_multirel_ && !is_relboost_ && !is_relmt_;

    static_assert( !unknown_feature_learner, "Unknown feature learner!" );

    // ----------------------------------------------------------------------

    if constexpr ( is_fastprop_ )
        {
            return AbstractFeatureLearner::FASTPROP;
        }

    if constexpr ( is_multirel_ )
        {
            return AbstractFeatureLearner::MULTIREL;
        }

    if constexpr ( is_relboost_ )
        {
            return AbstractFeatureLearner::RELBOOST;
        }

    if constexpr ( is_relmt_ )
        {
            return AbstractFeatureLearner::RELMT;
        }

    // ----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace featurelearners
}  // namespace engine

// ----------------------------------------------------------------------------

#endif  // ENGINE_FEATURELEARNERS_FEATURELEARNER_HPP_

