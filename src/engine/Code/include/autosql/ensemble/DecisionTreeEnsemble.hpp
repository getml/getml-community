#ifndef AUTOSQL_ENSEMBLE_DECISIONTREEENSEMBLE_HPP_
#define AUTOSQL_ENSEMBLE_DECISIONTREEENSEMBLE_HPP_

namespace autosql
{
namespace ensemble
{
// ----------------------------------------------------------------------------

class DecisionTreeEnsemble
{
   public:
    DecisionTreeEnsemble();

    DecisionTreeEnsemble(
        const std::shared_ptr<std::vector<std::string>> &_categories );

    DecisionTreeEnsemble(
        const std::shared_ptr<std::vector<std::string>> &_categories,
        const std::vector<std::string> &_placeholder_peripheral,
        const decisiontrees::Placeholder &_placeholder_population );

    DecisionTreeEnsemble( const DecisionTreeEnsemble &_other );

    DecisionTreeEnsemble( DecisionTreeEnsemble &&_other ) noexcept;

    ~DecisionTreeEnsemble() = default;

    // --------------------------------------

    /// Makes sure that the input provided by the user is plausible
    /// and throws an exception if it isn't
    void check_plausibility(
        const std::vector<containers::DataFrame> &_peripheral_tables,
        const containers::DataFrameView &_population_table );

    /// Calculates feature importances
    void feature_importances();

    /// Fits the decision tree ensemble
    std::string fit(
        const containers::DataFrameView &_population,
        const std::vector<containers::DataFrame> &_peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger );

    /// Builds the decision tree ensemble from a Poco::JSON::Object
    void from_json_obj( const Poco::JSON::Object &_json_obj );

    /// Loads the Model in JSON format, if applicable
    void load( const std::string &_path );

    /// Copy constructor
    DecisionTreeEnsemble &operator=( const DecisionTreeEnsemble &_other );

    /// Copy assignment constructor
    DecisionTreeEnsemble &operator=( DecisionTreeEnsemble &&_other ) noexcept;

    /// Saves the Model in JSON format, if applicable
    void save( const std::string &_path );

    /// Extracts the ensemble as a Poco::JSON object
    Poco::JSON::Object to_json_obj();

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

    // --------------------------------------

    /// Trivial getter
    inline multithreading::Communicator *comm() { return impl().comm_; }

    /// Whether the ensemble has been fitted
    inline bool has_been_fitted() const { return trees().size() > 0; }

    /// Trivial accessor
    const descriptors::Hyperparameters &hyperparameters() const
    {
        return *impl().hyperparameters_;
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
    inline std::string to_json() { return JSON::stringify( to_json_obj() ); }

    /// Trivial accessor
    inline const std::vector<decisiontrees::DecisionTree> &trees() const
    {
        return impl().trees_;
    }

    // --------------------------------------

   private:
    /// Builds the candidates during fit(...)
    std::list<decisiontrees::DecisionTree> build_candidates(
        const AUTOSQL_INT _ix_feature,
        const std::vector<descriptors::SameUnits> &_same_units,
        const decisiontrees::TableHolder &_table_holder );

    /// Calculates the sampling rate based on the number of rows
    /// in the population table and the sampling_factor
    void calculate_sampling_rate( const AUTOSQL_INT _population_nrows );

    /// Makes sure that the input provided by the user is plausible
    /// and throws an exception if it isn't. Only the fit(...) member
    /// function needs to call this, not transform(...).
    void check_plausibility_of_targets(
        const containers::DataFrameView &_population_table );

    /// Fits the linear regression and then recalculates the residuals.
    /// This is not needed when the shrinkage is 0.0.
    void fit_linear_regressions_and_recalculate_residuals(
        const decisiontrees::TableHolder &_table_holder,
        const AUTOSQL_FLOAT _shrinkage,
        const std::vector<AUTOSQL_FLOAT> &_sample_weights,
        std::vector<std::vector<AUTOSQL_FLOAT>> *_yhat_old,
        std::vector<std::vector<AUTOSQL_FLOAT>> *_residuals );

    /// Parses the linear regression from a Poco::JSON::Object
    void parse_linear_regressions( const Poco::JSON::Object &_json_obj );

    /// Turns a string describing the loss function into a proper loss function
    lossfunctions::LossFunction *parse_loss_function(
        std::string _loss_function );

    /// This member functions stores the number of columns
    /// so we can compare them later on.
    void set_num_columns(
        const std::vector<containers::DataFrame> &_peripheral_tables,
        const containers::DataFrameView &_population_table );

    // --------------------------------------

   private:
    /// Trivial accessor
    inline containers::Optional<aggregations::AggregationImpl>
        &aggregation_impl()
    {
        return impl().aggregation_impl_;
    }

    /// Trivial accessor
    inline std::shared_ptr<std::vector<std::string>> &categories()
    {
        return impl().categories_;
    }

    /// Trivial accessor
    inline descriptors::Hyperparameters &hyperparameters()
    {
        return *impl().hyperparameters_;
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

    /// Abstraction that returns the last linear regression in the ensemble
    inline utils::LinearRegression *last_linear_regression()
    {
        assert( linear_regressions().size() > 0 );
        return &( linear_regressions().back() );
    }

    /// Abstraction that returns the last linear regression in the ensemble
    inline std::vector<utils::LinearRegression> &linear_regressions()
    {
        return impl().linear_regressions_;
    }

    /// Trivial accessor
    inline lossfunctions::LossFunction *loss_function()
    {
        assert( loss_function_ );
        return loss_function_.get();
    }

    /// Trivial setter
    inline void loss_function( lossfunctions::LossFunction *_loss_function )
    {
        loss_function_.reset( _loss_function );
    }

    /// Trivial accessor
    inline std::vector<AUTOSQL_INT> &num_columns_peripheral_categorical()
    {
        return impl().num_columns_peripheral_categorical_;
    }

    /// Trivial accessor
    inline std::vector<AUTOSQL_INT> &num_columns_peripheral_discrete()
    {
        return impl().num_columns_peripheral_discrete_;
    }

    /// Trivial accessor
    inline std::vector<AUTOSQL_INT> &num_columns_peripheral_numerical()
    {
        return impl().num_columns_peripheral_numerical_;
    }

    /// Trivial accessor
    inline AUTOSQL_INT &num_columns_population_categorical()
    {
        return impl().num_columns_population_categorical_;
    }

    /// Trivial accessor
    inline AUTOSQL_INT &num_columns_population_discrete()
    {
        return impl().num_columns_population_discrete_;
    }

    /// Trivial accessor
    inline AUTOSQL_INT &num_columns_population_numerical()
    {
        return impl().num_columns_population_numerical_;
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

    // --------------------------------------

   private:
    /// All variables other than loss_function_
    DecisionTreeEnsembleImpl impl_;

    /// The loss function for this ensemble.
    std::unique_ptr<lossfunctions::LossFunction> loss_function_;
};

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace autosql

#endif  // AUTOSQL_ENSEMBLE_DECISIONTREEENSEMBLE_HPP_
