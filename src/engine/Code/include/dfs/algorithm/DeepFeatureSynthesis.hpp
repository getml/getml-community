#ifndef DFS_ALGORITHM_DEEPFEATURESYNTHESIS_HPP_
#define DFS_ALGORITHM_DEEPFEATURESYNTHESIS_HPP_

// ----------------------------------------------------------------------------

namespace dfs
{
namespace algorithm
{
// ------------------------------------------------------------------------

class DeepFeatureSynthesis
{
    // ------------------------------------------------------------------------

   public:
    typedef dfs::containers::DataFrame DataFrameType;
    typedef dfs::containers::DataFrameView DataFrameViewType;
    typedef dfs::containers::Features FeaturesType;
    typedef dfs::Hyperparameters HypType;
    typedef dfs::containers::Placeholder PlaceholderType;

    typedef DataFrameType::FloatColumnType FloatColumnType;
    typedef DataFrameType::IntColumnType IntColumnType;

    constexpr static bool is_time_series_ = false;
    constexpr static bool premium_only_ = false;
    constexpr static bool supports_multiple_targets_ = true;

    // ------------------------------------------------------------------------

   public:
    DeepFeatureSynthesis(
        const std::shared_ptr<const Hyperparameters>& _hyperparameters,
        const std::shared_ptr<const std::vector<std::string>>& _peripheral,
        const std::shared_ptr<const containers::Placeholder>& _placeholder,
        const std::shared_ptr<const std::vector<containers::Placeholder>>&
            _peripheral_schema = nullptr,
        const std::shared_ptr<const containers::Placeholder>&
            _population_schema = nullptr );

    DeepFeatureSynthesis( const Poco::JSON::Object& _obj );

    ~DeepFeatureSynthesis() = default;

    // ------------------------------------------------------------------------

   public:
    /// Calculates the column importances for this ensemble.
    std::map<helpers::ColumnDescription, Float> column_importances(
        const std::vector<Float>& _importance_factors ) const;

    /// Fits the DeepFeatureSynthesis.
    void fit(
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger =
            std::shared_ptr<const logging::AbstractLogger>() );

    /// Saves the DeepFeatureSynthesis into a JSON file.
    void save( const std::string& _fname ) const;

    /// Returns the features underlying the model (the predictions of the
    /// individual trees as opposed to the entire prediction)
    containers::Features transform(
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::optional<std::vector<size_t>>& _index = std::nullopt,
        const std::shared_ptr<const logging::AbstractLogger> _logger =
            std::shared_ptr<const logging::AbstractLogger>() ) const;

    /// Expresses DeepFeatureSynthesis as Poco::JSON::Object.
    Poco::JSON::Object to_json_obj( const bool _schema_only = false ) const;

    /// Expresses DeepFeatureSynthesis as SQL code.
    std::vector<std::string> to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories,
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
        const size_t _rownum,
        containers::Features* _features ) const;

    /// Builds all rows for the thread associated with _thread_num
    void build_rows(
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::vector<containers::Features>& _subfeatures,
        const std::vector<size_t>& _index,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const size_t _thread_num,
        std::atomic<size_t>* _num_completed,
        containers::Features* _features ) const;

    /// Builds the subfeatures.
    std::vector<containers::Features> build_subfeatures(
        const std::vector<containers::DataFrame>& _peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger ) const;

    /// Extracts the schemas from the training set.
    void extract_schemas(
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral );

    /// Extracts the schemas from the table holder in the training set - needed
    /// to generate the SQL code. In the other algorithms, this information is
    /// held by the individual trees.
    void extract_schemas( const TableHolder& _table_holder );

    /// Finds the DataFrame associated with _name.
    containers::DataFrame find_peripheral(
        const std::vector<containers::DataFrame>& _peripheral,
        const std::string& _name ) const;

    /// Fits abstract features on the categorical columns.
    void fit_on_categoricals(
        const containers::DataFrame& _peripheral,
        const size_t _peripheral_ix,
        std::shared_ptr<std::vector<containers::AbstractFeature>>
            _abstract_features ) const;

    /// Fits abstract features on the discrete columns.
    void fit_on_discretes(
        const containers::DataFrame& _peripheral,
        const size_t _peripheral_ix,
        std::shared_ptr<std::vector<containers::AbstractFeature>>
            _abstract_features ) const;

    /// Fits abstract features on the numerical columns.
    void fit_on_numericals(
        const containers::DataFrame& _peripheral,
        const size_t _peripheral_ix,
        std::shared_ptr<std::vector<containers::AbstractFeature>>
            _abstract_features ) const;

    /// Fits abstract features on categorical columns with the same unit.
    void fit_on_same_units_categorical(
        const containers::DataFrame& _population,
        const containers::DataFrame& _peripheral,
        const size_t _peripheral_ix,
        std::shared_ptr<std::vector<containers::AbstractFeature>>
            _abstract_features ) const;

    /// Fits abstract features on discrete columns with the same unit.
    void fit_on_same_units_discrete(
        const containers::DataFrame& _population,
        const containers::DataFrame& _peripheral,
        const size_t _peripheral_ix,
        std::shared_ptr<std::vector<containers::AbstractFeature>>
            _abstract_features ) const;

    /// Fits abstract features on numerical columns with the same unit.
    void fit_on_same_units_numerical(
        const containers::DataFrame& _population,
        const containers::DataFrame& _peripheral,
        const size_t _peripheral_ix,
        std::shared_ptr<std::vector<containers::AbstractFeature>>
            _abstract_features ) const;

    /// Fits abstract features on the subfeatures.
    void fit_on_subfeatures(
        const size_t _peripheral_ix,
        std::shared_ptr<std::vector<containers::AbstractFeature>>
            _abstract_features ) const;

    /// Fits abstract features on a particular peripheral table.
    void fit_on_peripheral(
        const containers::DataFrame& _population,
        const containers::DataFrame& _peripheral,
        const size_t _peripheral_ix,
        std::shared_ptr<std::vector<containers::AbstractFeature>>
            _abstract_features ) const;

    /// Fits the subfeatures of the DFS.
    std::shared_ptr<const std::vector<std::optional<DeepFeatureSynthesis>>>
    fit_subfeatures(
        const std::vector<containers::DataFrame>& _peripheral,
        const std::shared_ptr<const logging::AbstractLogger> _logger ) const;

    /// Infers the appropriate number of threads.
    size_t get_num_threads() const;

    /// Logs the progress of building the features.
    void log_progress(
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const size_t _nrows,
        const size_t _num_completed ) const;

    /// Generates importances from the features.
    std::vector<std::pair<helpers::ColumnDescription, Float>> infer_importance(
        const size_t _feature_num,
        const Float _importance_factor,
        std::vector<std::vector<Float>>* _subimportance_factors ) const;

    /// Initializes a set of empty features.
    containers::Features init_features(
        const size_t _nrows, const size_t _ncols ) const;

    /// Infers the indices of the features needed.
    std::vector<size_t> infer_index(
        const std::optional<std::vector<size_t>>& _index ) const;

    /// Initializes the importance factors for the subfeatures.
    std::vector<std::vector<Float>> init_subimportance_factors() const;

    /// Infers whether the aggregation _agg can be applied to a categorical.
    bool is_categorical( const std::string& _agg ) const;

    /// Infers whether the aggregation _agg can be applied to a numerical or
    /// discrete column.
    bool is_numerical( const std::string& _agg ) const;

    /// Generates the matches for a particular row in the population table.
    std::vector<std::vector<containers::Match>> make_matches(
        const TableHolder& _table_holder, const size_t _rownum ) const;

    /// Generates the rownums for the thread signified by _thread_num
    std::shared_ptr<std::vector<size_t>> make_rownums(
        const size_t _thread_num, const size_t _nrows ) const;

    /// Spawns the threads for building the features.
    void spawn_threads(
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::vector<containers::Features>& _subfeatures,
        const std::vector<size_t>& _index,
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        containers::Features* _features ) const;

    /// Expresses the subfeatures as SQL code.
    void subfeatures_to_sql(
        const std::shared_ptr<const std::vector<strings::String>>& _categories,
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
        throw_unless( hyperparameters_, "DFS has no hyperparameters." );
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
    const std::vector<containers::Placeholder>& peripheral_schema() const
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
    const containers::Placeholder& population_schema() const
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
        throw_unless( abstract_features_, "DFS has not been fitted." );
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

    /// Trivial accessor
    const std::vector<containers::Placeholder>& main_table_schemas() const
    {
        assert_true( main_table_schemas_ );
        return *main_table_schemas_;
    }

    /// Trivial accessor
    const std::vector<containers::Placeholder>& peripheral_table_schemas() const
    {
        assert_true( peripheral_table_schemas_ );
        return *peripheral_table_schemas_;
    }

    /// Trivial (private) setter.
    void set_comm( multithreading::Communicator* _comm ) { comm_ = _comm; }

    /// Trivial (private) accessor
    const std::vector<std::optional<DeepFeatureSynthesis>>& subfeatures() const
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
    std::shared_ptr<const std::vector<containers::Placeholder>>
        main_table_schemas_;

    /// Names of the peripheral tables, as they are referred in placeholder
    std::shared_ptr<const std::vector<std::string>> peripheral_;

    /// Schema of the peripheral tables.
    std::shared_ptr<const std::vector<containers::Placeholder>>
        peripheral_schema_;

    /// Schema of the peripheral tables taken from the table holder  - needed
    /// to generate the SQL code. In the other algorithms, this information is
    /// held by the individual trees.
    std::shared_ptr<const std::vector<containers::Placeholder>>
        peripheral_table_schemas_;

    /// Placeholder object used to define the data schema.
    std::shared_ptr<const containers::Placeholder> placeholder_;

    /// Schema of the population table.
    std::shared_ptr<const containers::Placeholder> population_schema_;

    /// Contains the algorithms for the subfeatures.
    std::shared_ptr<const std::vector<std::optional<DeepFeatureSynthesis>>>
        subfeatures_;

    // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace algorithm
}  // namespace dfs

#endif  // DFS_ALGORITHM_DEEPFEATURESYNTHESIS_HPP_
