#ifndef FASTPROP_ALGORITHM_FASTPROP_HPP_
#define FASTPROP_ALGORITHM_FASTPROP_HPP_

// ----------------------------------------------------------------------------

namespace fastprop
{
namespace algorithm
{
// ------------------------------------------------------------------------

class FastProp
{
   public:
    typedef typename helpers::VocabularyContainer::VocabForDf VocabForDf;
    typedef typename std::vector<VocabForDf> Vocabulary;

    // ------------------------------------------------------------------------

   public:
    typedef FitParams FitParamsType;
    typedef TransformParams TransformParamsType;

    typedef fastprop::containers::DataFrame DataFrameType;
    typedef fastprop::containers::DataFrameView DataFrameViewType;
    typedef fastprop::containers::Features FeaturesType;
    typedef fastprop::Hyperparameters HypType;
    typedef fastprop::containers::Placeholder PlaceholderType;

    typedef DataFrameType::FloatColumnType FloatColumnType;
    typedef DataFrameType::IntColumnType IntColumnType;
    typedef DataFrameType::StringColumnType StringColumnType;

    constexpr static bool is_time_series_ = false;
    constexpr static bool premium_only_ = false;
    constexpr static bool supports_multiple_targets_ = true;

    // ------------------------------------------------------------------------

   public:
    FastProp(
        const std::shared_ptr<const Hyperparameters>& _hyperparameters,
        const std::shared_ptr<const std::vector<std::string>>& _peripheral,
        const std::shared_ptr<const containers::Placeholder>& _placeholder );

    FastProp( const Poco::JSON::Object& _obj );

    ~FastProp() = default;

    // ------------------------------------------------------------------------

   public:
    /// Calculates the column importances for this ensemble.
    std::map<helpers::ColumnDescription, Float> column_importances(
        const std::vector<Float>& _importance_factors,
        const bool _is_subfeatures ) const;

    /// Fits the FastProp.
    void fit( const FitParams& _params, const bool _as_subfeatures = false );

    /// Returns the features underlying the model (the predictions of the
    /// individual trees as opposed to the entire prediction)
    containers::Features transform(
        const TransformParams& _params,
        const std::shared_ptr<std::vector<size_t>>& _rownums = nullptr,
        const bool _as_subfeatures = false ) const;

    /// Expresses FastProp as Poco::JSON::Object.
    Poco::JSON::Object to_json_obj( const bool _schema_only = false ) const;

    /// Expresses FastProp as SQL code.
    std::vector<std::string> to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories,
        const helpers::VocabularyTree& _vocabulary,
        const std::string& _feature_prefix = "",
        const size_t _offset = 0,
        const bool _subfeatures = true ) const;

   private:
    /// Turns and _abstract_feature into an actual feature.
    std::shared_ptr<std::vector<Float>> build_feature(
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const containers::AbstractFeature& _abstract_feature ) const;

    /// Builds a new row of features and inserts it.
    void build_row(
        const TableHolder& _table_holder,
        const std::vector<containers::Features>& _subfeatures,
        const std::vector<size_t>& _index,
        const std::vector<std::function<bool( const containers::Match& )>>&
            _condition_functions,
        const size_t _rownum,
        containers::Features* _features ) const;

    /// Builds all rows for the thread associated with _thread_num
    void build_rows(
        const TransformParams& _params,
        const std::vector<containers::Features>& _subfeatures,
        const std::shared_ptr<std::vector<size_t>>& _rownums,
        const size_t _thread_num,
        std::atomic<size_t>* _num_completed,
        containers::Features* _features ) const;

    /// Builds the subfeatures.
    std::vector<containers::Features> build_subfeatures(
        const TransformParams& _params,
        const std::shared_ptr<std::vector<size_t>>& _rownums ) const;

    /// Calculates the R-squared for each feature vis-a-vis the targets.
    std::vector<Float> calc_r_squared(
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const helpers::WordIndexContainer& _word_indices,
        const std::shared_ptr<std::vector<size_t>>& _rownums ) const;

    /// Calculates the threshold on the basis of which we throw out features.
    Float calc_threshold( const std::vector<Float>& _r_squared ) const;

    /// We only calculate the subfeatures that we actually need - but we still
    /// need to make sure that they are located at the right position. This is
    /// what expand_subfeatures(...) does.
    containers::Features expand_subfeatures(
        const containers::Features& _subfeatures,
        const std::vector<size_t>& _subfeature_index,
        const size_t _num_subfeatures ) const;

    /// Extracts the schemas from the training set.
    void extract_schemas(
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral );

    /// Extracts the schemas from the table holder in the training set - needed
    /// to generate the SQL code. In the other algorithms, this information is
    /// held by the individual trees.
    void extract_schemas( const TableHolder& _table_holder );

    /// Returns the most frequent categories of a categorical column
    std::vector<Int> find_most_frequent_categories(
        const containers::Column<Int>& _col ) const;

    /// Finds the DataFrame associated with _name.
    containers::DataFrame find_peripheral(
        const std::vector<containers::DataFrame>& _peripheral,
        const std::string& _name ) const;

    /// Returns the index related to the peripheral table.
    size_t find_peripheral_ix( const std::string& _name ) const;

    /// Fits abstract features on the categorical columns.
    void fit_on_categoricals(
        const containers::DataFrame& _peripheral,
        const size_t _peripheral_ix,
        const std::vector<containers::Condition>& _conditions,
        std::shared_ptr<std::vector<containers::AbstractFeature>>
            _abstract_features ) const;

    /// Fits abstract features on the categorical columns by identifying the
    /// most frequent categories.
    void fit_on_categoricals_by_categories(
        const containers::DataFrame& _population,
        const containers::DataFrame& _peripheral,
        const size_t _peripheral_ix,
        const std::vector<containers::Condition>& _conditions,
        std::shared_ptr<std::vector<containers::AbstractFeature>>
            _abstract_features ) const;

    /// Fits abstract features on the discrete columns.
    void fit_on_discretes(
        const containers::DataFrame& _population,
        const containers::DataFrame& _peripheral,
        const size_t _peripheral_ix,
        const std::vector<containers::Condition>& _conditions,
        std::shared_ptr<std::vector<containers::AbstractFeature>>
            _abstract_features ) const;

    /// Fits abstract features on the numerical columns.
    void fit_on_numericals(
        const containers::DataFrame& _population,
        const containers::DataFrame& _peripheral,
        const size_t _peripheral_ix,
        const std::vector<containers::Condition>& _conditions,
        std::shared_ptr<std::vector<containers::AbstractFeature>>
            _abstract_features ) const;

    /// Fits abstract features on a particular peripheral table.
    void fit_on_peripheral(
        const containers::DataFrame& _population,
        const containers::DataFrame& _peripheral,
        const size_t _peripheral_ix,
        const std::vector<std::vector<containers::Condition>>& _conditions,
        std::shared_ptr<std::vector<containers::AbstractFeature>>
            _abstract_features ) const;

    /// Fits abstract features on categorical columns with the same unit.
    void fit_on_same_units_categorical(
        const containers::DataFrame& _population,
        const containers::DataFrame& _peripheral,
        const size_t _peripheral_ix,
        const std::vector<containers::Condition>& _conditions,
        std::shared_ptr<std::vector<containers::AbstractFeature>>
            _abstract_features ) const;

    /// Fits abstract features on discrete columns with the same unit.
    void fit_on_same_units_discrete(
        const containers::DataFrame& _population,
        const containers::DataFrame& _peripheral,
        const size_t _peripheral_ix,
        const std::vector<containers::Condition>& _conditions,
        std::shared_ptr<std::vector<containers::AbstractFeature>>
            _abstract_features ) const;

    /// Fits abstract features on numerical columns with the same unit.
    void fit_on_same_units_numerical(
        const containers::DataFrame& _population,
        const containers::DataFrame& _peripheral,
        const size_t _peripheral_ix,
        const std::vector<containers::Condition>& _conditions,
        std::shared_ptr<std::vector<containers::AbstractFeature>>
            _abstract_features ) const;

    /// Fits abstract features on the subfeatures.
    void fit_on_subfeatures(
        const containers::DataFrame& _population,
        const containers::DataFrame& _peripheral,
        const size_t _peripheral_ix,
        const std::vector<containers::Condition>& _conditions,
        std::shared_ptr<std::vector<containers::AbstractFeature>>
            _abstract_features ) const;

    /// Fits the subfeatures of the FastProp.
    std::shared_ptr<const std::vector<std::optional<FastProp>>> fit_subfeatures(
        const FitParams& _params, const TableHolder& _table_holder ) const;

    /// Infers the appropriate number of threads.
    size_t get_num_threads() const;

    /// Generates importances from the features.
    std::vector<std::pair<helpers::ColumnDescription, Float>> infer_importance(
        const size_t _feature_num,
        const Float _importance_factor,
        std::vector<std::vector<Float>>* _subimportance_factors ) const;

    /// Initializes a set of empty features.
    containers::Features init_features(
        const size_t _nrows, const size_t _ncols ) const;

    /// Initializes the importance factors for the subfeatures.
    std::vector<std::vector<Float>> init_subimportance_factors() const;

    /// Infers whether the aggregation _agg can be applied to a categorical.
    bool is_categorical( const std::string& _agg ) const;

    /// Infers whether the aggregation _agg can be applied to a numerical or
    /// discrete column.
    bool is_numerical( const std::string& _agg ) const;

    /// Logs the progress of building the features.
    void log_progress(
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const size_t _nrows,
        const size_t _num_completed ) const;

    /// Generates WHERE-conditions to apply to the aggregations.
    std::vector<std::vector<containers::Condition>> make_conditions(
        const TableHolder& _table_holder ) const;

    /// Generates conditions based on categorical columns.
    void make_categorical_conditions(
        const containers::DataFrame& _peripheral,
        const size_t _peripheral_ix,
        std::vector<std::vector<containers::Condition>>* _conditions ) const;

    /// Generates conditions based on lag variables.
    void make_lag_conditions(
        const containers::DataFrame& _population,
        const containers::DataFrame& _peripheral,
        const size_t _peripheral_ix,
        std::vector<std::vector<containers::Condition>>* _conditions ) const;

    /// Generates conditions based on the categorical columns with the same
    /// unit.
    void make_same_units_categorical_conditions(
        const containers::DataFrame& _population,
        const containers::DataFrame& _peripheral,
        const size_t _peripheral_ix,
        std::vector<std::vector<containers::Condition>>* _conditions ) const;

    /// Generates an index of all subfeatures required for this particular set
    /// of indices.
    std::vector<size_t> make_subfeature_index(
        const size_t _peripheral_ix, const std::vector<size_t>& _index ) const;

    /// Generates the rownums required for the subfeatures.
    std::shared_ptr<std::vector<size_t>> make_subfeature_rownums(
        const std::shared_ptr<std::vector<size_t>>& _rownums,
        const containers::DataFrame& _population,
        const containers::DataFrame& _peripheral,
        const size_t _ix ) const;

    /// Generates the matches for a particular row in the population table.
    std::vector<std::vector<containers::Match>> make_matches(
        const TableHolder& _table_holder, const size_t _rownum ) const;

    /// Generates the rownums for the thread signified by _thread_num
    std::shared_ptr<std::vector<size_t>> make_rownums(
        const size_t _thread_num,
        const size_t _nrows,
        const std::shared_ptr<std::vector<size_t>>& _rownums ) const;

    /// Creates a random subsample for fitting.
    std::shared_ptr<std::vector<size_t>> sample_from_population(
        const size_t _nrows ) const;

    /// Weeds out features for which the correlation coefficient is too small.
    std::shared_ptr<const std::vector<containers::AbstractFeature>>
    select_features(
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const helpers::WordIndexContainer& _word_indices,
        const std::shared_ptr<std::vector<size_t>>& _rownums ) const;

    /// Returns true if _agg is FIRST or LAST, but there are no time stamps in
    /// _peripheral.
    bool skip_first_last(
        const std::string& _agg,
        const containers::DataFrame& _population,
        const containers::DataFrame& _peripheral ) const;

    /// Spawns the threads for building the features.
    void spawn_threads(
        const TransformParams& _params,
        const std::vector<containers::Features>& _subfeatures,
        const std::shared_ptr<std::vector<size_t>>& _rownums,
        containers::Features* _features ) const;

    /// Expresses the subfeatures as SQL code.
    void subfeatures_to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories,
        const helpers::VocabularyTree& _vocabulary,
        const std::string& _feature_prefix,
        const size_t _offset,
        std::vector<std::string>* _sql ) const;

    // ------------------------------------------------------------------------

   public:
    /// Trivial accessor
    bool& allow_http() { return allow_http_; }

    /// Trivial accessor
    bool allow_http() const { return allow_http_; }

    /// Trivial (const) accessor
    const Hyperparameters& hyperparameters() const
    {
        throw_unless( hyperparameters_, "FastProp has no hyperparameters." );
        return *hyperparameters_;
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
        return hyperparameters().loss_function_ ==
               Hyperparameters::CROSS_ENTROPY_LOSS;
    }

    /// Trivial accessor.
    size_t num_features() const
    {
        return abstract_features_ ? abstract_features().size()
                                  : static_cast<size_t>( 0 );
    }

    /// Trivial accessor.
    const std::vector<std::string>& peripheral() const
    {
        throw_unless(
            peripheral_,
            "Model has no peripheral - did you maybe forget to fit "
            "it?" );
        return *peripheral_;
    }

    /// Trivial (const) accessor
    const std::vector<helpers::Schema>& peripheral_schema() const
    {
        throw_unless(
            peripheral_schema_,
            "Model has no peripheral schema - did you maybe forget to fit "
            "it?" );
        return *peripheral_schema_;
    }

    /// Trivial accessor.
    const containers::Placeholder& placeholder() const
    {
        throw_unless( placeholder_, "Model has no placeholder." );
        return *placeholder_;
    }

    /// Trivial (const) accessor
    const helpers::Schema& population_schema() const
    {
        throw_unless(
            population_schema_,
            "Model has no population schema - did you may be forget to fit "
            "it?" );
        return *population_schema_;
    }

    // ------------------------------------------------------------------------

   private:
    /// Trivial accessor
    const std::vector<containers::AbstractFeature>& abstract_features() const
    {
        throw_unless( abstract_features_, "FastProp has not been fitted." );
        return *abstract_features_;
    }

    /// Whether there is a COUNT aggregation among the aggregations in the
    /// hyperparameter.
    bool has_count() const
    {
        return std::any_of(
            hyperparameters().aggregations_.begin(),
            hyperparameters().aggregations_.end(),
            []( const std::string& agg ) -> bool {
                return agg == enums::Parser<enums::Aggregation>::COUNT;
            } );
    }

    /// Whether the column is a time stamp.
    bool is_ts( const std::string& _name, const std::string& _unit ) const
    {
        const auto not_contains_rowid =
            _name.find( "$GETML_ROWID" ) == std::string::npos;
        return not_contains_rowid &&
               ( _unit.find( "time stamp" ) != std::string::npos );
    }

    /// Generates a filter function for the conditions.
    std::function<bool( const std::vector<containers::Condition>& )>
    make_condition_filter( const size_t _peripheral_ix ) const
    {
        const auto peripheral_matches =
            [_peripheral_ix]( const containers::Condition& c ) -> bool {
            return c.peripheral_ == _peripheral_ix;
        };

        return [peripheral_matches](
                   const std::vector<containers::Condition>& c ) -> bool {
            return std::all_of( c.begin(), c.end(), peripheral_matches );
        };
    }

    /// Trivial accessor
    const std::vector<helpers::Schema>& main_table_schemas() const
    {
        assert_true( main_table_schemas_ );
        return *main_table_schemas_;
    }

    /// Trivial accessor
    const std::vector<helpers::Schema>& peripheral_table_schemas() const
    {
        assert_true( peripheral_table_schemas_ );
        return *peripheral_table_schemas_;
    }

    /// Trivial (private) setter.
    void set_comm( multithreading::Communicator* _comm ) { comm_ = _comm; }

    /// Trivial (private) accessor
    const std::vector<std::optional<FastProp>>& subfeatures() const
    {
        assert_true( subfeatures_ );
        return *subfeatures_;
    }

    // ------------------------------------------------------------------------

   private:
    /// Abstract representation of the features.
    std::shared_ptr<const std::vector<containers::AbstractFeature>>
        abstract_features_;

    /// Whether we want to allow this model to be used as an http endpoint.
    bool allow_http_;

    /// raw pointer to the communicator.
    multithreading::Communicator* comm_;

    /// Hyperparameters used to train the relboost model.
    std::shared_ptr<const Hyperparameters> hyperparameters_;

    /// Schema of the main tables taken from the table holder  - needed
    /// to generate the SQL code. In the other algorithms, this information is
    /// held by the individual trees.
    std::shared_ptr<const std::vector<helpers::Schema>> main_table_schemas_;

    /// Names of the peripheral tables, as they are referred in placeholder
    std::shared_ptr<const std::vector<std::string>> peripheral_;

    /// Schema of the peripheral tables.
    std::shared_ptr<const std::vector<helpers::Schema>> peripheral_schema_;

    /// Schema of the peripheral tables taken from the table holder  - needed
    /// to generate the SQL code. In the other algorithms, this information is
    /// held by the individual trees.
    std::shared_ptr<const std::vector<helpers::Schema>>
        peripheral_table_schemas_;

    /// Placeholder object used to define the data schema.
    std::shared_ptr<const containers::Placeholder> placeholder_;

    /// Schema of the population table.
    std::shared_ptr<const helpers::Schema> population_schema_;

    /// Contains the algorithms for the subfeatures.
    std::shared_ptr<const std::vector<std::optional<FastProp>>> subfeatures_;

    // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace algorithm
}  // namespace fastprop

#endif  // FASTPROP_ALGORITHM_FASTPROP_HPP_
