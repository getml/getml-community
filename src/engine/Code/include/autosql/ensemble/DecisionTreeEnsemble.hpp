#ifndef AUTOSQL_ENSEMBLE_DECISIONTREEENSEMBLE_HPP_
#define AUTOSQL_ENSEMBLE_DECISIONTREEENSEMBLE_HPP_

namespace autosql
{
namespace ensemble
{
// ----------------------------------------------------------------------------

class DecisionTreeEnsemble
{
    // -----------------------------------------------------------------

   public:
    typedef autosql::containers::DataFrame DataFrameType;
    typedef autosql::containers::DataFrameView DataFrameViewType;

    // -----------------------------------------------------------------

   public:
    DecisionTreeEnsemble(
        const std::shared_ptr<const std::vector<std::string>> &_categories,
        const std::shared_ptr<const descriptors::Hyperparameters>
            &_hyperparameters,
        const std::shared_ptr<const std::vector<std::string>> &_peripheral,
        const std::shared_ptr<const decisiontrees::Placeholder> &_placeholder );

    DecisionTreeEnsemble(
        const std::shared_ptr<const std::vector<std::string>> &_categories,
        const Poco::JSON::Object &_obj );

    ~DecisionTreeEnsemble() = default;

    // -----------------------------------------------------------------

   public:
    /// Calculates feature importances
    void feature_importances();

    /// Fits the decision tree ensemble - spawns the threads.
    void fit(
        const containers::DataFrame &_population,
        const std::vector<containers::DataFrame> &_peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger =
            std::shared_ptr<const logging::AbstractLogger>() );

    /// Fits the decision tree ensemble - called by the spawned threads.
    void fit(
        const std::shared_ptr<const decisiontrees::TableHolder> &_table_holder,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const size_t _num_features,
        optimizationcriteria::OptimizationCriterion *_opt,
        multithreading::Communicator *_comm );

    /// Saves the Model in JSON format, if applicable
    void save( const std::string &_fname ) const;

    /// Selects the features according to the index given.
    void select_features( const std::vector<size_t> &_index );

    /// Extracts the ensemble as a Poco::JSON object
    Poco::JSON::Object to_json_obj( const bool _schema_only = false ) const;

    /// Extracts the ensemble as a Boost property tree the monitor process can
    /// understand
    Poco::JSON::Object to_monitor( const std::string _name ) const;

    /// Extracts the SQL statements underlying these features as a string
    std::string to_sql() const;

    /// Transforms a set of raw data into extracted features.
    containers::Features transform(
        const containers::DataFrame &_population,
        const std::vector<containers::DataFrame> &_peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger =
            std::shared_ptr<const logging::AbstractLogger>() ) const;

    /// Transforms table holders into a predictions. This is used for
    /// subfeatures, so no logging.
    containers::Predictions transform(
        const decisiontrees::TableHolder &_table_holder,
        containers::Optional<aggregations::AggregationImpl> *_impl ) const;

    /// Transforms a specific feature.
    std::vector<Float> transform(
        const decisiontrees::TableHolder &_table_holder,
        const std::vector<containers::Subfeatures> &_subfeatures,
        const size_t _num_feature,
        containers::Optional<aggregations::AggregationImpl> *_impl ) const;

    // -----------------------------------------------------------------

   public:
    /// Trivial accessor
    inline const std::shared_ptr<const std::vector<std::string>> &categories()
        const
    {
        return impl().categories_;
    }

    /// Trivial getter
    inline multithreading::Communicator *comm() { return impl().comm_; }

    /// Whether the ensemble has been fitted
    inline bool has_been_fitted() const { return trees().size() > 0; }

    /// Trivial accessor
    const descriptors::Hyperparameters &hyperparameters() const
    {
        return *impl().hyperparameters_;
    }

    /// Whether this is a classification problem
    const bool is_classification() const
    {
        return hyperparameters().loss_function_ != "SquareLoss";
    }

    /// Trivial accessor
    const size_t num_features() const { return trees().size(); }

    /// Trivial accessor
    inline const std::vector<std::string> &peripheral_names() const
    {
        return impl().placeholder_peripheral_;
    }

    /// Trivial (const) accessor
    const std::vector<containers::Schema> &peripheral_schema() const
    {
        assert_true( impl().peripheral_schema_ );
        return *impl().peripheral_schema_;
    }

    /// Trivial accessor
    inline const decisiontrees::Placeholder &placeholder() const
    {
        assert_true( impl().placeholder_population_ );
        return *impl().placeholder_population_;
    }

    /// Trivial (const) accessor
    const containers::Schema &population_schema() const
    {
        assert_true( impl().population_schema_ );
        return *impl().population_schema_;
    }

    /// Trivial (const) accessor
    const std::string &session_name() const
    {
        return hyperparameters().session_name_;
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

    /// Makes sure that the input provided by the user is plausible
    /// and throws an exception if it isn't. Only the fit(...) member
    /// function needs to call this, not transform(...).
    void check_plausibility_of_targets(
        const containers::DataFrame &_population_table );

    /// Extracts the schemas from the data frames, for future referece.
    void extract_schemas(
        const containers::DataFrame &_population,
        const std::vector<containers::DataFrame> &_peripheral );

    /// Extracts a DecisionTreeEnsemble from a JSON object.
    DecisionTreeEnsemble from_json_obj(
        const Poco::JSON::Object &_json_obj ) const;

    // -----------------------------------------------------------------

   private:
    /// Trivial accessor
    inline containers::Optional<aggregations::AggregationImpl>
        &aggregation_impl()
    {
        return impl().aggregation_impl_;
    }

    /// Trivial accessor
    inline DecisionTreeEnsembleImpl &impl() { return impl_; }

    /// Trivial accessor
    inline const DecisionTreeEnsembleImpl &impl() const { return impl_; }

    /// Abstraction that returns the last tree that has actually been added to
    /// the ensemble
    inline decisiontrees::DecisionTree *last_tree()
    {
        assert_true( trees().size() > 0 );
        return &( trees().back() );
    }

    /// Trivial accessor
    inline std::vector<std::string> &peripheral_names()
    {
        return impl().placeholder_peripheral_;
    }

    /// Trivial accessor
    inline decisiontrees::Placeholder &placeholder()
    {
        assert_true( impl().placeholder_population_ );
        return *impl().placeholder_population_;
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
    /// Contains all variables other than the DecisionTreeEnsemble.
    DecisionTreeEnsembleImpl impl_;

    /// Contains the ensembles for the subfeatures trained with the intermediate
    /// aggregation AVG.
    std::vector<containers::Optional<DecisionTreeEnsemble>> subensembles_avg_;

    /// Contains the ensembles for the subfeatures trained with the intermediate
    /// aggregation SUM.
    std::vector<containers::Optional<DecisionTreeEnsemble>> subensembles_sum_;

    // -----------------------------------------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace ensemble
}  // namespace autosql

#endif  // AUTOSQL_ENSEMBLE_DECISIONTREEENSEMBLE_HPP_
