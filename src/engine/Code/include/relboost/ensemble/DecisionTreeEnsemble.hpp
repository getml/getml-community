#ifndef RELBOOST_ENSEMBLE_DECISIONTREEENSEMBLE_HPP_
#define RELBOOST_ENSEMBLE_DECISIONTREEENSEMBLE_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace ensemble
{
// ------------------------------------------------------------------------

class DecisionTreeEnsemble
{
    // ------------------------------------------------------------------------

   public:
    typedef relboost::containers::DataFrame DataFrameType;
    typedef relboost::containers::DataFrameView DataFrameViewType;

    // ------------------------------------------------------------------------

   public:
    DecisionTreeEnsemble(
        const std::shared_ptr<const std::vector<std::string>>& _encoding,
        const std::shared_ptr<const Hyperparameters>& _hyperparameters,
        const std::shared_ptr<const std::vector<std::string>>& _peripheral,
        const std::shared_ptr<const Placeholder>& _placeholder );

    DecisionTreeEnsemble(
        const std::shared_ptr<const std::vector<std::string>>& _encoding,
        const Poco::JSON::Object& _obj );

    DecisionTreeEnsemble( const DecisionTreeEnsemble& _other );

    DecisionTreeEnsemble( DecisionTreeEnsemble&& _other ) noexcept;

    ~DecisionTreeEnsemble() = default;

    // -----------------------------------------------------------------

   public:
    /// Deletes ressources that are no longer needed.
    void clean_up();

    /// Fits the DecisionTreeEnsemble.
    void fit(
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger =
            std::shared_ptr<const logging::AbstractLogger>() );

    /// Fits one more feature.
    void fit_new_feature(
        const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
        const std::shared_ptr<const TableHolder>& _table_holder );

    /// Fits the subensembles.
    void fit_subensembles(
        const std::shared_ptr<const TableHolder>& _table_holder,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const std::shared_ptr<lossfunctions::LossFunction>& _loss_function );

    /// Initializes the fitting process.
    std::pair<
        const std::shared_ptr<lossfunctions::LossFunction>,
        const std::shared_ptr<const TableHolder>>
    init(
        const containers::DataFrameView& _population,
        const std::vector<containers::DataFrame>& _peripheral );

    /// Copy constructor
    DecisionTreeEnsemble& operator=( const DecisionTreeEnsemble& _other );

    /// Copy assignment constructor
    DecisionTreeEnsemble& operator=( DecisionTreeEnsemble&& _other ) noexcept;

    /// Generates predictions.
    std::vector<Float> predict(
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral ) const;

    /// Prepares the subfeatures for this prediction (if any).
    std::pair<
        const std::vector<containers::Predictions>,
        const std::vector<containers::Subfeatures>>
    prepare_subfeatures(
        const std::shared_ptr<const TableHolder>& _table_holder,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const std::shared_ptr<lossfunctions::LossFunction>& _loss_function )
        const;

    /// Saves the DecisionTreeEnsemble into a JSON file.
    void save( const std::string& _fname ) const;

    /// Selects the features according to the index given.
    void select_features( const std::vector<size_t>& _index );

    /// Expresses the model in a format that the monitor can understand.
    Poco::JSON::Object to_monitor( const std::string _name ) const;

    /// Returns the features underlying the model (the predictions of the
    /// individual trees as opposed to the entire prediction)
    containers::Features transform(
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger =
            std::shared_ptr<const logging::AbstractLogger>() ) const;

    /// Returns one feature.
    std::vector<Float> transform(
        const TableHolder& _table_holder, size_t _n_feature ) const;

    /// Expresses DecisionTreeEnsemble as Poco::JSON::Object.
    Poco::JSON::Object to_json_obj( const bool _schema_only = false ) const;

    /// Expresses DecisionTreeEnsemble as SQL code.
    std::string to_sql() const;

    // -----------------------------------------------------------------

   public:
    /// Trivial (const) accessor
    const std::shared_ptr<const std::vector<std::string>>& encoding() const
    {
        return impl().encoding_;
    }

    /// Trivial (const) accessor
    const Hyperparameters& hyperparameters() const
    {
        throw_unless(
            impl().hyperparameters_, "Model has no hyperparameters." );
        return *impl().hyperparameters_;
    }

    /// Initializes the fitting process with this being a
    /// a subensemble.
    void init_as_subensemble( multithreading::Communicator* _comm )
    {
        set_comm( _comm );
    }

    /// Whether this is a classification problem
    const bool is_classification() const
    {
        return loss_function().type() != "SquareLoss";
    }

    /// Trivial accessor.
    size_t num_features() const { return trees().size(); }

    /// Trivial  accessor.
    const std::vector<std::string>& peripheral_names() const
    {
        throw_unless(
            impl().peripheral_names_,
            "Model has no peripheral names - did you maybe forget to fit "
            "it?" );
        return *impl().peripheral_names_;
    }

    /// Trivial (const) accessor
    const std::vector<containers::Schema>& peripheral_schema() const
    {
        throw_unless(
            impl().peripheral_schema_,
            "Model has no peripheral schema - did you maybe forget to fit "
            "it?" );
        return *impl().peripheral_schema_;
    }

    /// Trivial accessor.
    const Placeholder& placeholder() const
    {
        throw_unless( impl().placeholder_, "Model has no placeholder." );
        return *impl().placeholder_;
    }

    /// Trivial (const) accessor
    const containers::Schema& population_schema() const
    {
        throw_unless(
            impl().population_schema_,
            "Model has no population schema - did you may be forget to fit "
            "it?" );
        return *impl().population_schema_;
    }

    // -----------------------------------------------------------------

   private:
    /// Calculates the initial prediction.
    void calc_initial_prediction();

    // Calculates the loss reduction of the predictions generated by a
    // candidate.
    Float calc_loss_reduction(
        const decisiontrees::DecisionTree& _decision_tree,
        const std::vector<Float>& _predictions ) const;

    // Makes sure that the target values are well-behaved.
    void check_plausibility_of_targets(
        const containers::DataFrame& _population_table );

    /// Extracts the schemas of the population table and the peripheral tables.
    void extract_schemas(
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral );

    /// Generates a new slate of predictions.
    std::vector<Float> generate_predictions(
        const decisiontrees::DecisionTree& _decision_tree,
        const TableHolder& _table_holder ) const;

    /// Returns the number of matches for each element of the population table.
    std::shared_ptr<std::vector<Float>> make_counts(
        const size_t _nrows,
        const std::vector<const containers::Match*>& _matches_ptr );

    // -----------------------------------------------------------------

   private:
    /// Trivial (private) accessor
    multithreading::Communicator& comm() const
    {
        assert_true( impl().comm_ != nullptr );
        return *impl().comm_;
    }

    /// Trivial (private) accessor
    DecisionTreeEnsembleImpl& impl() { return impl_; }

    /// Trivial (private) accessor
    const DecisionTreeEnsembleImpl& impl() const { return impl_; }

    /// Trivial (private) accessor
    Float& initial_prediction() { return impl().initial_prediction_; }

    /// Trivial (private) accessor
    const Float initial_prediction() const
    {
        return impl().initial_prediction_;
    }

    /// Trivial (private) accessor
    lossfunctions::LossFunction& loss_function()
    {
        assert_true( loss_function_ );
        return *loss_function_;
    }

    /// Trivial (private) accessor
    const lossfunctions::LossFunction& loss_function() const
    {
        assert_true( loss_function_ );
        return *loss_function_;
    }

    /// Trivial (const) accessor
    const std::string& session_name() const
    {
        return hyperparameters().session_name_;
    }

    /// Trivial (private) setter.
    void set_comm( multithreading::Communicator* _comm )
    {
        impl().comm_ = _comm;
        loss_function().set_comm( _comm );
        for ( auto& tree : trees() ) tree.set_comm( _comm );
        for ( auto& subensemble : subensembles_avg_ )
            if ( subensemble ) subensemble->set_comm( _comm );
        for ( auto& subensemble : subensembles_sum_ )
            if ( subensemble ) subensemble->set_comm( _comm );
    }

    /// Trivial (private) accessor
    std::vector<Float>& targets()
    {
        assert_true( targets_ );
        return *targets_;
    }

    /// Trivial (private) accessor
    const std::vector<Float>& targets() const
    {
        assert_true( targets_ );
        return *targets_;
    }

    /// Trivial (private) accessor
    std::vector<decisiontrees::DecisionTree>& trees() { return impl().trees_; }

    /// Trivial (private) accessor
    const std::vector<decisiontrees::DecisionTree>& trees() const
    {
        return impl().trees_;
    }

    /// Updates the prediction.
    void update_predictions(
        const Float _update_rate,
        const std::vector<Float>& _predictions,
        std::vector<Float>* _yhat_old ) const
    {
        assert_true( _predictions.size() == _yhat_old->size() );
        std::transform(
            _yhat_old->begin(),
            _yhat_old->end(),
            _predictions.begin(),
            _yhat_old->begin(),
            [this, _update_rate]( const Float yhat, const Float pred ) {
                return yhat + pred * hyperparameters().eta_ * _update_rate;
            } );
    }

    // -----------------------------------------------------------------

   private:
    /// The implementation (variables we can copy without problems).
    DecisionTreeEnsembleImpl impl_;

    /// The loss function to be minimized.
    std::shared_ptr<lossfunctions::LossFunction> loss_function_;

    /// Contains the ensembles for the subfeatures trained with the intermediate
    /// aggregation AVG.
    std::vector<std::optional<DecisionTreeEnsemble>> subensembles_avg_;

    /// Contains the ensembles for the subfeatures trained with the intermediate
    /// aggregation SUM.
    std::vector<std::optional<DecisionTreeEnsemble>> subensembles_sum_;

    /// Target variables (previous trees already substracted).
    std::shared_ptr<std::vector<Float>> targets_;

    // -----------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_ENSEMBLE_DECISIONTREEENSEMBLE_HPP_
