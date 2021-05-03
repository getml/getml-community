#ifndef MULTIREL_ENSEMBLE_DECISIONTREEENSEMBLE_HPP_
#define MULTIREL_ENSEMBLE_DECISIONTREEENSEMBLE_HPP_

namespace multirel
{
namespace ensemble
{
// ----------------------------------------------------------------------------

class DecisionTreeEnsemble
{
    // -----------------------------------------------------------------

   public:
    typedef FitParams FitParamsType;
    typedef TransformParams TransformParamsType;

    typedef multirel::containers::DataFrame DataFrameType;
    typedef multirel::containers::DataFrameView DataFrameViewType;
    typedef multirel::containers::Features FeaturesType;
    typedef multirel::descriptors::Hyperparameters HypType;
    typedef multirel::containers::Placeholder PlaceholderType;

    typedef DataFrameType::FloatColumnType FloatColumnType;
    typedef DataFrameType::IntColumnType IntColumnType;

    constexpr static bool is_time_series_ = false;
    constexpr static bool premium_only_ = false;
    constexpr static bool supports_multiple_targets_ = true;

    // -----------------------------------------------------------------

   public:
    DecisionTreeEnsemble(
        const std::shared_ptr<const descriptors::Hyperparameters>
            &_hyperparameters,
        const std::shared_ptr<const std::vector<std::string>> &_peripheral,
        const std::shared_ptr<const containers::Placeholder> &_placeholder,
        const std::shared_ptr<const std::vector<containers::Placeholder>>
            &_peripheral_schema = nullptr,
        const std::shared_ptr<const containers::Placeholder>
            &_population_schema = nullptr );

    DecisionTreeEnsemble( const Poco::JSON::Object &_obj );

    ~DecisionTreeEnsemble() = default;

    // -----------------------------------------------------------------

   public:
    /// Calculates the column importances for this ensemble.
    std::map<helpers::ColumnDescription, Float> column_importances(
        const std::vector<Float> &_importance_factors,
        const bool _is_subfeatures ) const;

    /// Calculates the column importance for a particular tree.
    std::map<helpers::ColumnDescription, Float> column_importance_for_tree(
        const Float _importance_factors,
        const decisiontrees::DecisionTree &_tree ) const;

    /// Calculates feature importances
    void feature_importances();

    /// Fits the decision tree ensemble - spawns the threads.
    void fit( const FitParams &_params );

    /// Fits the decision tree ensemble - called by the spawned threads.
    void fit(
        const std::shared_ptr<const decisiontrees::TableHolder> &_table_holder,
        const helpers::WordIndexContainer &_word_indices,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const size_t _num_features,
        const std::shared_ptr<optimizationcriteria::OptimizationCriterion>
            &_opt,
        multithreading::Communicator *_comm );

    /// Selects the features according to the index given.
    void select_features( const std::vector<size_t> &_index );

    /// Extracts the ensemble as a Poco::JSON object
    Poco::JSON::Object to_json_obj( const bool _schema_only = false ) const;

    /// Expresses DecisionTreeEnsemble as SQL code.
    std::vector<std::string> to_sql(
        const std::shared_ptr<const std::vector<strings::String>> &_categories,
        const std::string &_feature_prefix = "",
        const size_t _offset = 0,
        const bool _subfeatures = true ) const;

    /// Transforms a set of raw data into extracted features.
    /// Only the features signified by _index will be used, if such an index
    /// is passed.
    containers::Features transform( const TransformParams &_params ) const;

    /// Transforms table holders into predictions, used for subtree
    /// predictions.
    containers::Predictions transform(
        const decisiontrees::TableHolder &_table_holder,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        multithreading::Communicator *_comm,
        containers::Optional<aggregations::AggregationImpl> *_impl ) const;

    /// Transforms a specific feature.
    std::shared_ptr<const std::vector<Float>> transform(
        const decisiontrees::TableHolder &_table_holder,
        const std::vector<containers::Subfeatures> &_subfeatures,
        const size_t _num_feature,
        containers::Optional<aggregations::AggregationImpl> *_impl ) const;

    // -----------------------------------------------------------------

   public:
    /// Trivial accessor
    bool &allow_http() { return impl().allow_http_; }

    /// Trivial accessor
    bool allow_http() const { return impl().allow_http_; }

    /// Trivial getter
    inline multithreading::Communicator *comm() { return impl().comm_; }

    /// Trivial accessor
    const descriptors::Hyperparameters &hyperparameters() const
    {
        throw_unless(
            impl().hyperparameters_, "Model has no hyperparameters." );
        return *impl().hyperparameters_;
    }

    /// Whether this is a classification problem
    const bool is_classification() const
    {
        return hyperparameters().loss_function_ != "SquareLoss";
    }

    /// Whether the ensemble is a subensemble
    const bool is_subensemble() const { return !impl().population_schema_; }

    /// Generates the optimizer
    std::shared_ptr<optimizationcriteria::RSquaredCriterion> make_r_squared(
        const containers::DataFrameView &_population,
        multithreading::Communicator *_comm ) const
    {
        return std::make_shared<optimizationcriteria::RSquaredCriterion>(
            impl().hyperparameters_, _population, _comm );
    }

    /// Trivial accessor
    const size_t num_features() const { return trees().size(); }

    /// Trivial accessor
    inline const std::vector<std::string> &peripheral() const
    {
        assert_true( impl().peripheral_ );
        return *impl().peripheral_;
    }

    /// Trivial (const) accessor
    const std::vector<containers::Placeholder> &peripheral_schema() const
    {
        throw_unless(
            impl().peripheral_schema_,
            "Model has no peripheral schema - did you maybe forget to fit "
            "it?" );
        return *impl().peripheral_schema_;
    }

    /// Trivial accessor
    inline const containers::Placeholder &placeholder() const
    {
        throw_unless( impl().placeholder_, "Model has no placeholder." );
        return *impl().placeholder_;
    }

    /// Trivial (const) accessor
    const containers::Placeholder &population_schema() const
    {
        throw_unless(
            impl().population_schema_,
            "Model has no population schema - did you may be forget to fit "
            "it?" );
        return *impl().population_schema_;
    }

    /// Trivial setter
    void set_comm( multithreading::Communicator *_comm )
    {
        impl().comm_ = _comm;
    }

    /// Trivial getter.
    const std::vector<containers::Optional<DecisionTreeEnsemble>>
        &subensembles_avg() const
    {
        return subensembles_avg_;
    }

    /// Trivial getter.
    const std::vector<containers::Optional<DecisionTreeEnsemble>>
        &subensembles_sum() const
    {
        return subensembles_sum_;
    }

    /// Extracts the ensemble as a JSON
    inline std::string to_json() const
    {
        return JSON::stringify( to_json_obj() );
    }

    /// Trivial accessor
    inline const std::vector<decisiontrees::DecisionTree> &trees() const
    {
        return impl().trees_;
    }

    // -----------------------------------------------------------------

   private:
    /// Builds the candidates during fit(...)
    std::list<decisiontrees::DecisionTree> build_candidates(
        const size_t _ix_feature,
        const std::vector<descriptors::SameUnits> &_same_units,
        const decisiontrees::TableHolder &_table_holder );

    /// Calculates the thread numbers scattering the population table over
    /// several threads.
    std::pair<Int, std::vector<size_t>> calc_thread_nums(
        const containers::DataFrame &_population ) const;

    /// Makes sure that the input provided by the user is plausible
    /// and throws an exception if it isn't. Only the fit(...) member
    /// function needs to call this, not transform(...).
    void check_plausibility_of_targets(
        const containers::DataFrame &_population_table );

    /// Extracts the schemas from the data frames, for future referece.
    void extract_schemas(
        const containers::DataFrame &_population,
        const std::vector<containers::DataFrame> &_peripheral );

    /// Spawns the threads for fitting.
    void fit_spawn_threads(
        const containers::DataFrame &_population,
        const std::vector<containers::DataFrame> &_peripheral,
        const helpers::RowIndexContainer &_row_indices,
        const helpers::WordIndexContainer &_word_indices,
        const std::optional<const helpers::FeatureContainer>
            &_feature_container,
        const std::shared_ptr<const logging::AbstractLogger> _logger );

    /// Extracts a DecisionTreeEnsemble from a JSON object.
    DecisionTreeEnsemble from_json_obj(
        const Poco::JSON::Object &_json_obj ) const;

    /// Spawns the threads for transforming the features.
    void transform_spawn_threads(
        const containers::DataFrame &_population,
        const std::vector<containers::DataFrame> &_peripheral,
        const std::vector<size_t> &_index,
        const std::optional<helpers::WordIndexContainer> &_word_indices,
        const std::optional<const helpers::FeatureContainer>
            &_feature_container,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        containers::Features *_features ) const;

    // -----------------------------------------------------------------

   private:
    /// Whether the ensemble has a population schema
    inline bool has_population_schema() const
    {
        return ( impl().population_schema_ && true );
    }

    /// Trivial accessor
    inline DecisionTreeEnsembleImpl &impl() { return impl_; }

    /// Trivial accessor
    inline const DecisionTreeEnsembleImpl &impl() const { return impl_; }

    /// Abstraction that returns the last tree that has actually been added
    /// to the ensemble
    inline decisiontrees::DecisionTree *last_tree()
    {
        assert_true( trees().size() > 0 );
        return &( trees().back() );
    }

    /// Trivial accessor
    inline containers::Optional<std::mt19937> &random_number_generator()
    {
        return impl().random_number_generator_;
    }

    /// Trivial accessor
    inline decisiontrees::DecisionTree *tree( const Int _i )
    {
        assert_true( trees().size() > 0 );
        assert_true( static_cast<Int>( trees().size() ) > _i );

        return trees().data() + _i;
    }

    /// Trivial accessor
    inline std::vector<std::string> &targets() { return impl().targets_; }

    /// Trivial accessor
    inline const std::vector<std::string> &targets() const
    {
        return impl().targets_;
    }

    /// Trivial accessor
    inline std::vector<decisiontrees::DecisionTree> &trees()
    {
        return impl().trees_;
    }

    // -----------------------------------------------------------------

   private:
    /// Contains all variables other than the subensembles.
    DecisionTreeEnsembleImpl impl_;

    /// Contains the ensembles for the subfeatures trained with the
    /// intermediate aggregation AVG.
    std::vector<containers::Optional<DecisionTreeEnsemble>> subensembles_avg_;

    /// Contains the ensembles for the subfeatures trained with the
    /// intermediate aggregation SUM.
    std::vector<containers::Optional<DecisionTreeEnsemble>> subensembles_sum_;

    // -----------------------------------------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace ensemble
}  // namespace multirel

#endif  // MULTIREL_ENSEMBLE_DECISIONTREEENSEMBLE_HPP_
