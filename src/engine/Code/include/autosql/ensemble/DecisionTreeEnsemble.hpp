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

    /// Makes sure that the input provided by the user is plausible
    /// and throws an exception if it isn't
    void check_plausibility(
        const std::vector<containers::DataFrame> &_peripheral_tables,
        const containers::DataFrameView &_population_table );

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
    Poco::JSON::Object to_json_obj() const;

    /// Extracts the ensemble as a Boost property tree the monitor process can
    /// understand
    Poco::JSON::Object to_monitor( const std::string _name ) const;

    /// Extracts the SQL statements underlying these features as a string
    std::string to_sql() const;

    /// Transforms a set of raw data into extracted features
    std::shared_ptr<std::vector<AUTOSQL_FLOAT>> transform(
        const containers::DataFrame &_population,
        const std::vector<containers::DataFrame> &_peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger =
            std::shared_ptr<const logging::AbstractLogger>() ) const;

    /// Transforms a specific feature.
    std::vector<AUTOSQL_FLOAT> transform(
        const decisiontrees::TableHolder &_table_holder,
        const size_t _num_feature,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        containers::Optional<aggregations::AggregationImpl> *_impl ) const;

    // -----------------------------------------------------------------

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

    /// Trivial accessor
    inline const decisiontrees::Placeholder &placeholder() const
    {
        assert( impl().placeholder_population_ );
        return *impl().placeholder_population_;
    }

    /// Trivial setter
    inline void set_comm( multithreading::Communicator *_comm )
    {
        impl().comm_ = _comm;
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
        const AUTOSQL_INT _ix_feature,
        const std::vector<descriptors::SameUnits> &_same_units,
        const decisiontrees::TableHolder &_table_holder );

    /// Makes sure that the input provided by the user is plausible
    /// and throws an exception if it isn't. Only the fit(...) member
    /// function needs to call this, not transform(...).
    void check_plausibility_of_targets(
        const containers::DataFrameView &_population_table );

    /// Extracts a DecisionTreeEnsemble from a JSON object.
    DecisionTreeEnsemble from_json_obj(
        const Poco::JSON::Object &_json_obj ) const;

    /// Fits subfeatures for a single peripheral table, for a single
    /// IntermediateAggregation.
    template <typename AggType>
    void fit_subfeatures(
        const std::shared_ptr<const decisiontrees::TableHolder> &_table_holder,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const std::shared_ptr<const std::map<AUTOSQL_INT, AUTOSQL_INT>>
            &_output_map,
        const size_t _ix_perip_used,
        optimizationcriteria::OptimizationCriterion *_opt,
        multithreading::Communicator *_comm,
        DecisionTreeEnsemble *_subfeature ) const;

    /// Fits all of the subfeatures.
    void fit_subfeatures(
        const std::shared_ptr<const decisiontrees::TableHolder> &_table_holder,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        optimizationcriteria::OptimizationCriterion *_opt,
        multithreading::Communicator *_comm );

    // -----------------------------------------------------------------

   private:
    /// Trivial accessor
    inline containers::Optional<aggregations::AggregationImpl>
        &aggregation_impl()
    {
        return impl().aggregation_impl_;
    }

    /// Trivial accessor
    inline std::shared_ptr<const std::vector<std::string>> &categories()
    {
        return impl().categories_;
    }

    /// Trivial accessor
    inline DecisionTreeEnsembleImpl &impl() { return impl_; }

    /// Trivial accessor
    inline const DecisionTreeEnsembleImpl &impl() const { return impl_; }

    /// Abstraction that returns the last tree that has actually been added to
    /// the ensemble
    inline decisiontrees::DecisionTree *last_tree()
    {
        assert( trees().size() > 0 );
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
        assert( impl().placeholder_population_ );
        return *impl().placeholder_population_;
    }

    /// Trivial accessor
    inline containers::Optional<std::mt19937> &random_number_generator()
    {
        return impl().random_number_generator_;
    }

    /// Trivial accessor
    inline decisiontrees::DecisionTree *tree( const AUTOSQL_INT _i )
    {
        assert( trees().size() > 0 );
        assert( static_cast<AUTOSQL_INT>( trees().size() ) > _i );

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

    /// Contains the ensemble for the subfeatures trained with the intermediate
    /// aggregation AVG.
    std::vector<containers::Optional<DecisionTreeEnsemble>> subfeatures_avg_;

    /// Contains the ensemble for the subfeatures trained with the intermediate
    /// aggregation SUM.
    std::vector<containers::Optional<DecisionTreeEnsemble>> subfeatures_sum_;

    // -----------------------------------------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace ensemble
}  // namespace autosql

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

namespace autosql
{
namespace ensemble
{
// ----------------------------------------------------------------------------

template <typename AggType>
void DecisionTreeEnsemble::fit_subfeatures(
    const std::shared_ptr<const decisiontrees::TableHolder> &_table_holder,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::shared_ptr<const std::map<AUTOSQL_INT, AUTOSQL_INT>>
        &_output_map,
    const size_t _ix_perip_used,
    optimizationcriteria::OptimizationCriterion *_opt,
    multithreading::Communicator *_comm,
    DecisionTreeEnsemble *_subfeature ) const
{
    const auto subtable_holder =
        std::make_shared<const decisiontrees::TableHolder>(
            *_table_holder->subtables_[_ix_perip_used] );

    assert( subtable_holder->main_tables_.size() > 0 );

    const auto input_table = containers::DataFrameView(
        _table_holder->peripheral_tables_[_ix_perip_used],
        subtable_holder->main_tables_[0].rows_ptr() );

    // The input map is needed for propagating sampling.
    const auto input_map =
        utils::Mapper::create_rows_map( input_table.rows_ptr() );

    const auto aggregation_index = aggregations::AggregationIndex(
        input_table,
        _table_holder->main_tables_[_ix_perip_used],
        input_map,
        _output_map,
        hyperparameters().use_timestamps_ );

    const auto opt_impl =
        std::make_shared<aggregations::IntermediateAggregationImpl>(
            _table_holder->main_tables_[0].nrows(), aggregation_index, _opt );

    const auto intermediate_agg =
        std::unique_ptr<optimizationcriteria::OptimizationCriterion>(
            new aggregations::IntermediateAggregation<AggType>( opt_impl ) );

    _subfeature->fit(
        subtable_holder,
        _logger,
        hyperparameters().num_subfeatures_,
        intermediate_agg.get(),
        _comm );

    _opt->reset_yhat_old();
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace autosql

#endif  // AUTOSQL_ENSEMBLE_DECISIONTREEENSEMBLE_HPP_
