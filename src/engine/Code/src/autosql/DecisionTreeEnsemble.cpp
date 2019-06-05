#include "decisiontrees/decisiontrees.hpp"

namespace autosql
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble() {}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble(
    const std::shared_ptr<containers::Encoding> &_categories )
    : impl_( _categories )
{
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble(
    const std::shared_ptr<containers::Encoding> &_categories,
    const std::vector<std::string> &_placeholder_peripheral,
    const Placeholder &_placeholder_population )
    : impl_( _categories, _placeholder_peripheral, _placeholder_population )
{
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble( const DecisionTreeEnsemble &_other )
    : impl_( _other.impl() )
{
    debug_message( "Model: Copy constructor..." );

    if ( _other.loss_function_ )
        {
            assert( _other.loss_function_->type() != "" );

            loss_function(
                parse_loss_function( _other.loss_function_->type() ) );
        }
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble(
    DecisionTreeEnsemble &&_other ) noexcept
    : impl_( std::move( _other.impl() ) ),
      loss_function_( std::move( _other.loss_function_ ) )
{
    debug_message( "Model: Move constructor..." );
}

// ----------------------------------------------------------------------------

std::list<DecisionTree> DecisionTreeEnsemble::build_candidates(
    const AUTOSQL_INT _ix_feature,
    const std::vector<descriptors::SameUnits> &_same_units,
    TableHolder &_table_holder )
{
#ifdef AUTOSQL_PARALLEL

    return CandidateTreeBuilder::build_candidates(
        _table_holder,
        _same_units,
        _ix_feature,
        *hyperparameters(),
        aggregation_impl(),
        *random_number_generator(),
        comm() );

#else  // AUTOSQL_PARALLEL

    return CandidateTreeBuilder::build_candidates(
        _table_holder,
        _same_units,
        _ix_feature,
        *hyperparameters(),
        aggregation_impl(),
        *random_number_generator() );

#endif  // AUTOSQL_PARALLEL
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::calculate_feature_stats(
    const containers::Matrix<AUTOSQL_FLOAT> &_predictions,
    const containers::DataFrameView &_targets )
{
    scores().feature_correlations() =
        containers::Summarizer::calculate_feature_correlations(
            _predictions, _targets, comm() );

    containers::Summarizer::calculate_feature_plots(
        20,
        _predictions,
        _targets,
        comm(),
        scores().labels(),
        scores().feature_densities(),
        scores().average_targets() );
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::calculate_sampling_rate(
    const AUTOSQL_INT _population_nrows )
{
    AUTOSQL_FLOAT nrows = static_cast<AUTOSQL_FLOAT>( _population_nrows );

#ifdef AUTOSQL_PARALLEL

    AUTOSQL_FLOAT nrows_global = 0.0;

    AUTOSQL_PARALLEL_LIB::all_reduce(
        *comm(),                   // comm
        nrows,                     // in_values
        nrows_global,              // out_values
        std::plus<AUTOSQL_FLOAT>()  // op
    );

    comm()->barrier();

    nrows = nrows_global;

#endif  // AUTOSQL_PARALLEL

    if ( nrows <= 0.0 )
        {
            throw std::invalid_argument(
                "The population table needs to contain at least some data!" );
        }

    hyperparameters()->sampling_rate = std::min(
        hyperparameters()->sampling_factor * ( 2000.0 / nrows ), 1.0 );
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::check_plausibility(
    const std::vector<containers::DataFrame> &_peripheral_tables,
    const containers::DataFrame &_population_table )
{
    // -----------------------------------------------------
    // Sizes of arrays must match

    auto expected_size = _population_table.num_join_keys();

    if ( expected_size == 0 )
        {
            throw std::invalid_argument(
                "You must provide join keys for the population table!" );
        }

    assert( expected_size == _peripheral_tables.size() );

    assert( expected_size == num_columns_peripheral_categorical().size() );

    // -----------------------------------------------------
    // Number of rows must match

    for ( AUTOSQL_SIZE i = 0; i < expected_size; ++i )
        {
            _peripheral_tables[i].check_plausibility();
        }

    _population_table.check_plausibility();

    // -----------------------------------------------------
    // Number of columns must match

    if ( _population_table.categorical().ncols() !=
         num_columns_population_categorical() )
        {
            throw std::invalid_argument(
                "Wrong number of categorical columns in population table! "
                "Expected " +
                std::to_string( num_columns_population_categorical() ) +
                ", got " +
                std::to_string( _population_table.categorical().ncols() ) +
                "!" );
        }

    if ( _population_table.discrete().ncols() !=
         num_columns_population_discrete() )
        {
            throw std::invalid_argument(
                "Wrong number of discrete columns in population table! "
                "Expected " +
                std::to_string( num_columns_population_discrete() ) + ", got " +
                std::to_string( _population_table.discrete().ncols() ) + "!" );
        }

    if ( _population_table.numerical().ncols() !=
         num_columns_population_numerical() )
        {
            throw std::invalid_argument(
                "Wrong number of numerical columns in population table! "
                "Expected " +
                std::to_string( num_columns_population_numerical() ) +
                ", got " +
                std::to_string( _population_table.numerical().ncols() ) + "!" );
        }

    for ( AUTOSQL_SIZE i = 0; i < _peripheral_tables.size(); ++i )
        {
            if ( _peripheral_tables[i].categorical().ncols() !=
                 num_columns_peripheral_categorical()[i] )
                {
                    throw std::invalid_argument(
                        "Wrong number of categorical columns in peripheral "
                        "table " +
                        std::to_string( i ) + "! Expected " +
                        std::to_string(
                            num_columns_peripheral_categorical()[i] ) +
                        ", got " +
                        std::to_string(
                            _peripheral_tables[i].categorical().ncols() ) +
                        "!" );
                }
        }

    for ( AUTOSQL_SIZE i = 0; i < _peripheral_tables.size(); ++i )
        {
            if ( _peripheral_tables[i].discrete().ncols() !=
                 num_columns_peripheral_discrete()[i] )
                {
                    throw std::invalid_argument(
                        "Wrong number of discrete columns in peripheral "
                        "table " +
                        std::to_string( i ) + "! Expected " +
                        std::to_string( num_columns_peripheral_discrete()[i] ) +
                        ", got " +
                        std::to_string(
                            _peripheral_tables[i].discrete().ncols() ) +
                        "!" );
                }
        }

    for ( AUTOSQL_SIZE i = 0; i < _peripheral_tables.size(); ++i )
        {
            if ( _peripheral_tables[i].numerical().ncols() !=
                 num_columns_peripheral_numerical()[i] )
                {
                    throw std::invalid_argument(
                        "Wrong number of numerical columns in peripheral "
                        "table " +
                        std::to_string( i ) + "! Expected " +
                        std::to_string(
                            num_columns_peripheral_numerical()[i] ) +
                        ", got " +
                        std::to_string(
                            _peripheral_tables[i].numerical().ncols() ) +
                        "!" );
                }
        }

    // -----------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::check_plausibility_of_targets(
    const containers::DataFrameView &_population_table )
{
    _population_table.df().check_plausibility();

    if ( _population_table.df().targets().ncols() < 1 )
        {
            throw std::invalid_argument(
                "Targets must have at least one column!" );
        }

    for ( AUTOSQL_INT i = 0; i < _population_table.nrows(); ++i )
        {
            for ( AUTOSQL_INT j = 0;
                  j < _population_table.df().targets().ncols();
                  ++j )
                {
                    if ( _population_table.targets( i, j ) !=
                         _population_table.targets( i, j ) )
                        {
                            throw std::invalid_argument(
                                "Target values can not be NULL!" );
                        }
                }
        }

    if ( hyperparameters()->share_aggregations < 0.0 ||
         hyperparameters()->share_aggregations > 1.0 )
        {
            throw std::invalid_argument(
                "share_aggregations must be between 0.0 and 1.0!" );
        }

    if ( has_been_fitted() )
        {
            assert( trees().size() > 0 );

            assert( linear_regressions().size() > 0 );

            assert( linear_regressions().size() == trees().size() );

            if ( linear_regressions()[0].ncols() !=
                 _population_table.df().targets().ncols() )
                {
                    throw std::invalid_argument(
                        "Number of columns in targets cannot change throughout "
                        "different training episodes!" );
                }
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::feature_importances()
{
    // ----------------------------------------------------------------
    // Extract feature importances

    std::vector<std::vector<AUTOSQL_FLOAT>> feature_importances_transposed;

    for ( auto &predictor : predictors() )
        {
            feature_importances_transposed.push_back(
                predictor->feature_importances( trees().size() ) );
        }

    // ----------------------------------------------------------------
    // Transpose feature importances

    scores().feature_importances().clear();

    if ( feature_importances_transposed.size() == 0 )
        {
            return;
        }

    for ( std::size_t i = 0; i < feature_importances_transposed[0].size(); ++i )
        {
            std::vector<AUTOSQL_FLOAT> temp;

            for ( auto &feat : feature_importances_transposed )
                {
                    temp.push_back( feat[i] );
                }

            scores().feature_importances().push_back( temp );
        }

    // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::string DecisionTreeEnsemble::fit(
    const std::shared_ptr<const logging::Logger> _logger,
    std::vector<containers::DataFrame> _peripheral_tables_raw,
    containers::DataFrameView _population_table_raw,
    descriptors::Hyperparameters _hyperparameters )
{
    debug_message( "fit: Beginning to fit features..." );

    // ----------------------------------------------------------------

    hyperparameters().reset(
        new descriptors::Hyperparameters( _hyperparameters ) );

    calculate_sampling_rate( _population_table_raw.nrows() );

    predictors().clear();

    // ----------------------------------------------------------------
    // Create abstractions over the peripheral_tables and the population
    // table - for convenience

#ifdef AUTOSQL_MULTINODE_MPI

    // Prepare tables is already called by rearrange_tables
    // - no need to do it again!

    auto peripheral_tables = _peripheral_tables_raw;

    auto population_table = _population_table_raw;

#else  // AUTOSQL_MULTINODE_MPI

    TableHolder table_holder = TablePreparer::prepare_tables(
        *placeholder_population(),
        placeholder_peripheral(),
        _peripheral_tables_raw,
        _population_table_raw );

#endif  // AUTOSQL_MULTINODE_MPI

    // ----------------------------------------------------------------
    // Store the column numbers, so we can make sure that the user
    // passes properly formatted data to predict(...)

    if ( has_been_fitted() == false )
        {
            debug_message( "fit: Storing column numbers..." );

            set_num_columns(
                table_holder.peripheral_tables, table_holder.main_table.df() );
        }

    // ----------------------------------------------------------------
    // Make sure that the data passed by the user is plausible

    debug_message( "fit: Checking plausibility of input..." );

    check_plausibility(
        table_holder.peripheral_tables, table_holder.main_table.df() );

    check_plausibility_of_targets( table_holder.main_table );

    // ----------------------------------------------------------------
    // Store names of the targets

    targets() = *table_holder.main_table.df().targets().colnames();

    // ----------------------------------------------------------------
    // aggregations::AggregationImpl stores most of the data for the
    // aggregations. We do not want to reallocate the data all the time.

    aggregation_impl().reset(
        new aggregations::AggregationImpl( table_holder.main_table.nrows() ) );

    if ( has_been_fitted() )
        {
            for ( auto &tree : trees() )
                {
                    tree.set_aggregation_impl( aggregation_impl() );
                }
        }

    // ----------------------------------------------------------------
    // Columns that share the same units are candidates for direct
    // comparison

    debug_message( "fit: Identifying same units..." );

    assert( table_holder.peripheral_tables.size() > 0 );

    auto same_units = SameUnitIdentifier::identify_same_units(
        table_holder.peripheral_tables, table_holder.main_table.df() );

    // ----------------------------------------------------------------
    // Initialize the other objects we need

    loss_function( parse_loss_function( hyperparameters()->loss_function ) );

#ifdef AUTOSQL_PARALLEL
    loss_function()->set_comm( comm() );
#endif  // AUTOSQL_PARALLEL

    optimizationcriteria::RSquaredCriterion opt( static_cast<AUTOSQL_FLOAT>(
        hyperparameters()->tree_hyperparameters.min_num_samples ) );

    // ----------------------------------------------------------------
    // Sample weights are needed for the random-forest-like functionality

    debug_message( "fit: Setting up sampling..." );

    if ( hyperparameters()->seed < 0 )
        {
            throw std::invalid_argument( "Seed must be positive!" );
        }

    containers::Matrix<AUTOSQL_FLOAT> sample_weights(
        table_holder.main_table.nrows(), 1 );

    if ( !random_number_generator() )
        {
            random_number_generator().reset( new std::mt19937(
                static_cast<size_t>( hyperparameters()->seed ) ) );
        }

    if ( hyperparameters()->sampling_rate <= 0.0 )
        {
            std::fill( sample_weights.begin(), sample_weights.end(), 1.0 );
        }

    // ----------------------------------------------------------------
    // For the gradient-tree-boosting-like functionality, we intialize
    // yhat_old at 0.0. If this has already been fitted, we obviously
    // use the previous features.

    containers::Matrix<AUTOSQL_FLOAT> yhat_old(
        table_holder.main_table.nrows(),
        table_holder.main_table.df().targets().ncols() );

    if ( has_been_fitted() )
        {
            yhat_old = predict(
                _logger,
                table_holder.peripheral_tables,
                table_holder.main_table );
        }

    // ----------------------------------------------------------------
    // Calculate the pseudo-residuals - on which the tree will be
    // predicted

    containers::Matrix<AUTOSQL_FLOAT> residuals =
        loss_function()->calculate_residuals(
            yhat_old, table_holder.main_table );

    // ----------------------------------------------------------------
    // Sample containers are pointers to simple structs, which represent a match
    // between a key in the peripheral table and a key in the population table.

    debug_message( "fit: Creating samples..." );

    const auto num_peripheral = table_holder.peripheral_tables.size();

    std::vector<AUTOSQL_SAMPLES> samples( num_peripheral );

    std::vector<AUTOSQL_SAMPLE_CONTAINER> sample_containers( num_peripheral );

    if ( hyperparameters()->sampling_rate <= 0.0 )
        {
            for ( AUTOSQL_SIZE i = 0; i < num_peripheral; ++i )
                {
                    SampleContainer::create_samples_and_sample_containers(
                        *hyperparameters(),
                        table_holder.peripheral_tables,
                        table_holder.main_table,
                        samples,
                        sample_containers );
                }
        }

    // ----------------------------------------------------------------

    for ( AUTOSQL_INT ix_feature = 0;
          ix_feature < hyperparameters()->num_features;
          ++ix_feature )
        {
            // ----------------------------------------------------------------
            // Sample for a random-forest-like algorithm - can be turned off
            // by setting _sampling_rate to 0.0

            debug_message( "fit: Sampling from population..." );

            if ( hyperparameters()->sampling_rate > 0.0 )
                {
                    SampleContainer::create_samples_and_sample_containers(
                        *hyperparameters(),
                        table_holder.peripheral_tables,
                        table_holder.main_table,
                        *random_number_generator(),
                        sample_weights,
                        samples,
                        sample_containers );
                }

            // ----------------------------------------------------------------
            // Reset the optimization criterion based on the residuals
            // generated
            // from the last prediction

            debug_message( "fit: Preparing optimization criterion..." );

#ifdef AUTOSQL_PARALLEL

            opt.set_comm( comm() );

#endif  // AUTOSQL_PARALLEL

            opt.init( residuals, sample_weights );

            // ----------------------------------------------------------------

            debug_message( "fit: Building candidates..." );

            auto candidate_trees =
                build_candidates( ix_feature, same_units, table_holder );

            // ----------------------------------------------------------------
            // Fit the trees

            debug_message( "fit: Fitting features..." );

            TreeFitter tree_fitter(
                categories(),
                comm(),
                *hyperparameters(),
                *random_number_generator() );

            tree_fitter.fit(
                table_holder,
                samples,
                sample_containers,
                &opt,
                candidate_trees,
                trees() );

            // -------------------------------------------------------------
            // Recalculate residuals, which is needed for the gradient boosting
            // algorithm. If the shrinkage is 0.0, we still need a proper
            // LinearRegression, which is why we use one with parameters 0.0 to
            // save time.

            debug_message( "fit: Recalculating residuals..." );

            if ( hyperparameters()->shrinkage != 0.0 )
                {
                    fit_linear_regressions_and_recalculate_residuals(
                        table_holder,
                        hyperparameters()->shrinkage,
                        sample_weights,
                        yhat_old,
                        residuals );
                }
            else
                {
                    linear_regressions().push_back( LinearRegression(
                        table_holder.main_table.df().targets().ncols() ) );
                }

            // -------------------------------------------------------------

            log( _logger,
                 "Trained FEATURE_" + std::to_string( trees().size() ) + "." );
        }

    // ----------------------------------------------------------------
    // Clean up

    aggregation_impl().reset();

    // ----------------------------------------------------------------
    // Return message

    debug_message( "fit: Done..." );

    std::stringstream msg;

    msg << std::endl << "Trained " << trees().size() << " features.";

    return msg.str();
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::fit_linear_regressions_and_recalculate_residuals(
    TableHolder &_table_holder,
    const AUTOSQL_FLOAT _shrinkage,
    containers::Matrix<AUTOSQL_FLOAT> &_sample_weights,
    containers::Matrix<AUTOSQL_FLOAT> &_yhat_old,
    containers::Matrix<AUTOSQL_FLOAT> &_residuals )
{
    // ----------------------------------------------------------------
    // Generate new feature

    containers::Matrix<AUTOSQL_FLOAT> new_feature = last_tree()->transform(
        _table_holder, hyperparameters()->use_timestamps );

    // ----------------------------------------------------------------
    // Train a linear regression from the prediction of the last
    // tree on the residuals and generate predictions f_t on that basis

    linear_regressions().push_back( LinearRegression() );

#ifdef AUTOSQL_PARALLEL
    last_linear_regression()->set_comm( comm() );
#endif  // AUTOSQL_PARALLEL

    last_linear_regression()->fit( new_feature, _residuals, _sample_weights );

    const auto f_t = last_linear_regression()->predict( new_feature );

    // ----------------------------------------------------------------
    // Find the optimal update_rates and update parameters of linear
    // regression accordingly

    containers::Matrix<AUTOSQL_FLOAT> update_rates =
        loss_function()->calculate_update_rates(
            _yhat_old, f_t, _table_holder.main_table, _sample_weights );

    for ( AUTOSQL_INT i = 0; i < _table_holder.main_table.df().targets().ncols();
          ++i )
        {
            last_linear_regression()->intercepts( i ) *=
                update_rates( 0, i ) * _shrinkage;
        }

    for ( AUTOSQL_INT i = 0; i < _table_holder.main_table.df().targets().ncols();
          ++i )
        {
            last_linear_regression()->slopes( i ) *=
                update_rates( 0, i ) * _shrinkage;
        }

    // ----------------------------------------------------------------
    // Get rid of out-of-range-values

    auto in_range = []( AUTOSQL_FLOAT &val ) {
        val = ( ( std::isnan( val ) || std::isinf( val ) ) ? 0.0 : val );
    };

    std::for_each(
        last_linear_regression()->slopes().begin(),
        last_linear_regression()->slopes().end(),
        in_range );

    std::for_each(
        last_linear_regression()->intercepts().begin(),
        last_linear_regression()->intercepts().end(),
        in_range );

    // ----------------------------------------------------------------
    // Do the actual updates

    for ( AUTOSQL_INT i = 0; i < _yhat_old.nrows(); ++i )
        {
            for ( AUTOSQL_INT j = 0; j < _yhat_old.ncols(); ++j )
                {
                    const AUTOSQL_FLOAT update =
                        f_t( i, j ) * update_rates( 0, j ) * _shrinkage;

                    _yhat_old( i, j ) +=
                        ( ( std::isnan( update ) || std::isinf( update ) )
                              ? 0.0
                              : update );
                }
        }

    // ----------------------------------------------------------------
    // Recalculate the pseudo-residuals - on which the tree will
    // be predicted

    _residuals = loss_function()->calculate_residuals(
        _yhat_old, _table_holder.main_table );
}

// -------------------------------------------------------------------------

std::string DecisionTreeEnsemble::fit_predictors(
    const std::shared_ptr<const logging::Logger> _logger,
    containers::Matrix<AUTOSQL_FLOAT> _features,
    containers::Matrix<AUTOSQL_FLOAT> _targets )
{
    // ----------------------------------------------------------------

    if ( has_predictors() == false )
        {
            std::invalid_argument( "This Model has no predictors!" );
        }

    if ( _features.ncols() != static_cast<AUTOSQL_INT>( trees().size() ) )
        {
            std::invalid_argument(
                "Wrong number of features! Expected: " +
                std::to_string( trees().size() ) +
                ", got: " + std::to_string( _features.ncols() ) + "." );
        }

    // ----------------------------------------------------------------
    // Fit predictors

    init_predictors(
        static_cast<size_t>( _targets.ncols() ),
        *hyperparameters()->predictor_hyperparams );

    std::string msg = "";

    for ( AUTOSQL_INT col = 0; col < _targets.ncols(); ++col )
        {
            containers::Matrix<AUTOSQL_FLOAT> y = _targets.column( col );

            msg += predictors()[col]->fit( _logger, _features, y );
        }

    // ----------------------------------------------------------------
    // Calculate feature importances

    feature_importances();

    // ----------------------------------------------------------------

    return msg;

    // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::from_json_obj( const Poco::JSON::Object &_json_obj )
{
    // ----------------------------------------

    assert( categories() );

    DecisionTreeEnsemble model( categories() );

    // ----------------------------------------
    // Plausibility checks

    if ( _json_obj.AUTOSQL_GET_ARRAY( "features_" )->size() == 0 )
        {
            std::invalid_argument(
                "JSON object does not contain any features!" );
        }

    if ( ( _json_obj.AUTOSQL_GET_ARRAY( "update_rates1_" )->size() %
           _json_obj.AUTOSQL_GET_ARRAY( "features_" )->size() ) != 0 )
        {
            std::invalid_argument(
                "Error in JSON: Number of elements in update_rates must be "
                "divisible by number of features!" );
        }

    if ( _json_obj.AUTOSQL_GET_ARRAY( "update_rates1_" )->size() !=
         _json_obj.AUTOSQL_GET_ARRAY( "update_rates2_" )->size() )
        {
            std::invalid_argument(
                "Error in JSON: Number of elements update_rates1_ must be "
                "equal to number of elements in update_rates2_!" );
        }

    // -------------------------------------------
    // Parse trees

    auto features = _json_obj.AUTOSQL_GET_ARRAY( "features_" );

    for ( size_t i = 0; i < features->size(); ++i )
        {
            auto obj = features->getObject( static_cast<unsigned int>( i ) );

            model.trees().push_back( DecisionTree( *obj ) );
        }

    // ----------------------------------------
    // Set categories for trees

    for ( auto &tree : model.trees() )
        {
            tree.set_categories( model.categories() );
        }

    // ----------------------------------------
    // Parse targets

    model.targets() = JSON::array_to_vector<std::string>(
        _json_obj.AUTOSQL_GET_ARRAY( "targets_" ) );

    // ----------------------------------------
    // Parse parameters for linear regressions.

    model.parse_linear_regressions( _json_obj );

    // -------------------------------------------
    // Parse placeholders

    model.placeholder_peripheral() = JSON::array_to_vector<std::string>(
        _json_obj.AUTOSQL_GET_ARRAY( "peripheral_" ) );

    model.placeholder_population().reset(
        new Placeholder( *_json_obj.AUTOSQL_GET_OBJECT( "population_" ) ) );

    // ------------------------------------------
    // Parse num_columns

    model.num_columns_peripheral_categorical() =
        JSON::array_to_vector<AUTOSQL_INT>( _json_obj.AUTOSQL_GET_ARRAY(
            "num_columns_peripheral_categorical_" ) );

    model.num_columns_peripheral_numerical() =
        JSON::array_to_vector<AUTOSQL_INT>(
            _json_obj.AUTOSQL_GET_ARRAY( "num_columns_peripheral_numerical_" ) );

    model.num_columns_peripheral_discrete() = JSON::array_to_vector<AUTOSQL_INT>(
        _json_obj.AUTOSQL_GET_ARRAY( "num_columns_peripheral_discrete_" ) );

    model.num_columns_population_categorical() =
        _json_obj.AUTOSQL_GET( "num_columns_population_categorical_" );

    model.num_columns_population_numerical() =
        _json_obj.AUTOSQL_GET( "num_columns_population_numerical_" );

    model.num_columns_population_discrete() =
        _json_obj.AUTOSQL_GET( "num_columns_population_discrete_" );

    // -------------------------------------------
    // Parse hyperparameters

    model.hyperparameters().reset( new descriptors::Hyperparameters(
        *_json_obj.AUTOSQL_GET_OBJECT( "hyperparameters_" ) ) );

    // -------------------------------------------
    // Parse scores

    model.scores() =
        descriptors::Scores( *_json_obj.AUTOSQL_GET_OBJECT( "scores_" ) );

    // -------------------------------------------

    *this = std::move( model );

    // -------------------------------------------
}

// -------------------------------------------------------------------------

void DecisionTreeEnsemble::init_predictors(
    const size_t &_num_predictors,
    const Poco::JSON::Object &_predictor_hyperparameters )
{
    predictors().clear();

    auto type =
        _predictor_hyperparameters.AUTOSQL_GET_VALUE<std::string>( "type_" );

    if ( type == "XGBoostPredictor" )
        {
            auto hyperparams =
                descriptors::XGBoostHyperparams( _predictor_hyperparameters );

            for ( size_t i = 0; i < _num_predictors; ++i )
                {
                    predictors().push_back(
                        std::make_shared<predictors::XGBoostPredictor>(
                            hyperparams ) );
                }
        }
    else
        {
            throw std::invalid_argument(
                "Predictor of type '" + type + "' not known!" );
        }
}

// -------------------------------------------------------------------------

void DecisionTreeEnsemble::load( const std::string &_path )
{
    // ------------------------------------------------------------
    // Load and parse JSON

    std::stringstream json_stringstream;

    std::ifstream input( _path + "Model.json" );

    std::string line;

    if ( input.is_open() )
        {
            while ( std::getline( input, line ) )
                {
                    json_stringstream << line;
                }

            input.close();
        }
    else
        {
            throw std::invalid_argument(
                "File '" + _path + "Model.json' not found!" );
        }

    Poco::JSON::Parser parser;

    auto obj = parser.parse( json_stringstream.str() )
                   .extract<Poco::JSON::Object::Ptr>();

    from_json_obj( *obj );

    // ------------------------------------------------------------
    // If there are no predictors, do not load them.

    if ( !hyperparameters()->predictor_hyperparams )
        {
            return;
        }

    // ------------------------------------------------------------
    // Initialize predictors

    size_t num_predictors = 0;

    while ( true )
        {
            auto name = _path + "predictor-" + std::to_string( num_predictors );

            if ( Poco::File( name ).exists() == false )
                {
                    break;
                }

            ++num_predictors;
        }

    init_predictors(
        num_predictors, *hyperparameters()->predictor_hyperparams );

    // ------------------------------------------------------------
    // Load predictors

    for ( size_t i = 0; i < num_predictors; ++i )
        {
            auto name = _path + "predictor-" + std::to_string( i );

            predictors()[i]->load( name );
        }

    // ------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::log(
    const std::shared_ptr<const logging::Logger> &_logger,
    const std::string &_msg )
{
#ifdef AUTOSQL_MULTITHREADING

    // Only the main thread is allowed to print status messages
    if ( comm()->main_thread_id() != std::this_thread::get_id() )
        {
            return;
        }

#endif  // AUTOSQL_MULTITHREADING

#ifdef AUTOSQL_MULTINODE_MPI

    // Only the root process is allowed to print status messages
    if ( comm()->rank() != 0 )
        {
            return;
        }

#endif  // AUTOSQL_MULTINODE_MPI

    _logger->log( _msg );
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble &DecisionTreeEnsemble::operator=(
    const DecisionTreeEnsemble &_other )
{
    debug_message( "Model: Copy assignment constructor..." );

    DecisionTreeEnsemble temp( _other );

    *this = std::move( temp );

    return *this;
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble &DecisionTreeEnsemble::operator=(
    DecisionTreeEnsemble &&_other ) noexcept
{
    debug_message( "Model: Move assignment constructor..." );

    if ( this == &_other )
        {
            return *this;
        }

    loss_function_ = std::move( _other.loss_function_ );

    impl_ = std::move( _other.impl_ );

    return *this;
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::parse_linear_regressions(
    const Poco::JSON::Object &_json_obj )
{
    // -------------------------------------------------------------
    // Check input

    if ( _json_obj.AUTOSQL_GET_ARRAY( "update_rates1_" )->size() !=
         _json_obj.AUTOSQL_GET_ARRAY( "update_rates2_" )->size() )
        {
            throw std::runtime_error(
                "'update_rates1_' and 'update_rates2_' must have same "
                "length!" );
        }

    if ( _json_obj.AUTOSQL_GET_ARRAY( "update_rates1_" )->size() == 0 )
        {
            throw std::runtime_error(
                "The must be at least some update rates!" );
        }

    const auto num_targets =
        _json_obj.AUTOSQL_GET_ARRAY( "update_rates1_" )->getArray( 0 )->size();

    // -------------------------------------------------------------
    // Get slopes

    std::vector<std::vector<AUTOSQL_FLOAT>> slopes;

    {
        auto arr = _json_obj.AUTOSQL_GET_ARRAY( "update_rates1_" );

        for ( size_t i = 0; i < arr->size(); ++i )
            {
                if ( arr->getArray( static_cast<unsigned int>( i ) )->size() !=
                     num_targets )
                    {
                        throw std::runtime_error(
                            "All elements in 'update_rates1_' must have the "
                            "same length!" );
                    }

                slopes.push_back( JSON::array_to_vector<AUTOSQL_FLOAT>(
                    arr->getArray( static_cast<unsigned int>( i ) ) ) );
            }
    }

    // -------------------------------------------------------------
    // Get intercepts

    std::vector<std::vector<AUTOSQL_FLOAT>> intercepts;

    {
        auto arr = _json_obj.AUTOSQL_GET_ARRAY( "update_rates2_" );

        for ( size_t i = 0; i < arr->size(); ++i )
            {
                if ( arr->getArray( static_cast<unsigned int>( i ) )->size() !=
                     num_targets )
                    {
                        throw std::runtime_error(
                            "All elements in 'update_rates2_' must have the "
                            "same length!" );
                    }

                intercepts.push_back( JSON::array_to_vector<AUTOSQL_FLOAT>(
                    arr->getArray( static_cast<unsigned int>( i ) ) ) );
            }
    }

    // -------------------------------------------------------------
    // Create linear regressions

    linear_regressions().clear();

    for ( AUTOSQL_SIZE i = 0; i < slopes.size(); ++i )
        {
            containers::Matrix<AUTOSQL_FLOAT> slopes_mat(
                static_cast<AUTOSQL_INT>( 1 ),
                static_cast<AUTOSQL_INT>( num_targets ) );

            std::copy( slopes[i].begin(), slopes[i].end(), slopes_mat.begin() );

            containers::Matrix<AUTOSQL_FLOAT> intercepts_mat(
                static_cast<AUTOSQL_INT>( 1 ),
                static_cast<AUTOSQL_INT>( num_targets ) );

            std::copy(
                intercepts[i].begin(),
                intercepts[i].end(),
                intercepts_mat.begin() );

            linear_regressions().push_back( LinearRegression() );

            last_linear_regression()->set_slopes_and_intercepts(
                slopes_mat, intercepts_mat );
        }

    // -------------------------------------------------------------
}

// ----------------------------------------------------------------------------

lossfunctions::LossFunction *DecisionTreeEnsemble::parse_loss_function(
    std::string _loss_function )
{
    if ( _loss_function == "CrossEntropyLoss" )
        {
            return new lossfunctions::CrossEntropyLoss();
        }
    else if ( _loss_function == "SquareLoss" )
        {
            return new lossfunctions::SquareLoss();
        }
    else
        {
            std::string warning_message = "Loss Function of type '";
            warning_message.append( _loss_function );
            warning_message.append( "' not known!" );

            throw std::invalid_argument( warning_message );
        }
}

// ----------------------------------------------------------------------------

containers::Matrix<AUTOSQL_FLOAT> DecisionTreeEnsemble::predict(
    const std::shared_ptr<const logging::Logger> _logger,
    std::vector<containers::DataFrame> _peripheral_tables,
    containers::DataFrameView _population_table )
{
    // ---------------------------------------------------------------------
    // Generate the features - but do not transpose, so we can apply the
    // linear regressions.

    auto features = transform(
        _logger,
        _peripheral_tables,
        _population_table,
        false  // _transpose
    );

    // ---------------------------------------------------------------------
    // Apply the linear regressions line by line. Recall that the update
    // rates and the shrinkage have already been calculated into the
    // linear regressions

    const AUTOSQL_INT ncols = linear_regressions()[0].ncols();

    containers::Matrix<AUTOSQL_FLOAT> yhat( _population_table.nrows(), ncols );

    for ( AUTOSQL_INT feat = 0; feat < features.nrows(); ++feat )
        {
            auto feature = features.subview( feat, feat + 1 ).transpose();

            auto yhat_temp = linear_regressions()[feat].predict( feature );

            for ( AUTOSQL_SIZE i = 0; i < yhat.size(); ++i )
                {
                    yhat[i] +=
                        ( ( yhat_temp[i] == yhat_temp[i] ) ? ( yhat_temp[i] )
                                                           : ( 0.0 ) );
                }
        }

    // ---------------------------------------------------------------------

    return yhat;
}

// --------------------------------------------------------------------------

containers::Matrix<AUTOSQL_FLOAT> DecisionTreeEnsemble::predict(
    containers::Matrix<AUTOSQL_FLOAT> _features )
{
    // ----------------------------------------------------------------

    if ( _features.ncols() != static_cast<AUTOSQL_INT>( trees().size() ) )
        {
            std::invalid_argument(
                "Wrong number of features! Expected: " +
                std::to_string( trees().size() ) +
                ", got: " + std::to_string( _features.ncols() ) + "." );
        }

    // ----------------------------------------------------------------

    containers::Matrix<AUTOSQL_FLOAT> yhat( 0, _features.nrows() );

    for ( auto &predictor : predictors() )
        {
            yhat.append( predictor->predict( _features ).transpose() );
        }

    // ----------------------------------------------------------------

    return yhat.transpose();
}

// -------------------------------------------------------------------------

void DecisionTreeEnsemble::save( const std::string &_path )
{
    // If the path already exists, delete it to avoid
    // conflicts with already existing files.
    if ( Poco::File( _path ).exists() )
        {
            Poco::File( _path ).remove( true );
        }

    Poco::File( _path ).createDirectories();

    if ( trees().size() == 0 )
        {
            throw std::runtime_error( "Unfitted models cannot be saved!" );
        }

    std::ofstream output( _path + "Model.json", std::ofstream::out );

    output << to_json();

    output.close();

    if ( has_fitted_predictors() )
        {
            for ( size_t i = 0; i < predictors().size(); ++i )
                {
                    predictors()[i]->save(
                        _path + "predictor-" + std::to_string( i ) );
                }
        }
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DecisionTreeEnsemble::score(
    const containers::Matrix<AUTOSQL_FLOAT> &_yhat,
    const containers::Matrix<AUTOSQL_FLOAT> &_y )
{
    // ------------------------------------------------------------------------
    // Make sure that the loss function exists.

    if ( !loss_function_ )
        {
            throw std::runtime_error( "Model has not been fitted!" );
        }

    // ------------------------------------------------------------------------
    // Build up names.

    std::vector<std::string> names;

    if ( loss_function()->type() == "CrossEntropyLoss" )
        {
            names.push_back( "accuracy_" );
            names.push_back( "auc_" );
            names.push_back( "cross_entropy_" );
        }
    else if ( loss_function()->type() == "SquareLoss" )
        {
            names.push_back( "mae_" );
            names.push_back( "rmse_" );
            names.push_back( "rsquared_" );
        }
    else
        {
            assert( false && "loss function type not recognized" );
        }

    // ------------------------------------------------------------------------
    // Do the actual scoring.

    Poco::JSON::Object obj;

    for ( const auto &name : names )
        {
            const auto metric = metrics::MetricParser::parse( name );

#ifdef AUTOSQL_PARALLEL
            metric->set_comm( comm() );
#endif  // AUTOSQL_PARALLEL

            const auto scores = metric->score( _yhat, _y );

            for ( auto it = scores.begin(); it != scores.end(); ++it )
                {
                    obj.set( it->first, it->second );
                }
        }

    // ------------------------------------------------------------------------
    // Store scores.

    scores().from_json_obj( obj );

    // ------------------------------------------------------------------------
    // Extract values that can be send back to the client.

    Poco::JSON::Object client_obj;

    for ( const auto &name : names )
        {
            client_obj.set( name, obj.getArray( name ) );
        }

    // ------------------------------------------------------------------------

    return client_obj;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::string DecisionTreeEnsemble::select_features(
    const std::shared_ptr<const logging::Logger> _logger,
    containers::Matrix<AUTOSQL_FLOAT> _features,
    containers::Matrix<AUTOSQL_FLOAT> _targets )
{
    // -----------------------------------------------------------
    // Plausibility checks

    if ( hyperparameters()->num_selected_features <= 0 )
        {
            throw std::invalid_argument(
                "Number of features must be positive!" );
        }

    if ( !has_feature_selectors() )
        {
            std::invalid_argument( "This Model has no feature selectors!" );
        }

    if ( _features.ncols() != static_cast<AUTOSQL_INT>( trees().size() ) )
        {
            std::invalid_argument(
                "Wrong number of features! Expected: " +
                std::to_string( trees().size() ) +
                ", got: " + std::to_string( _features.ncols() ) + "." );
        }

    // ----------------------------------------------------------------
    // Fit predictors

    init_predictors(
        static_cast<size_t>( _targets.ncols() ),
        *hyperparameters()->feature_selector_hyperparams );

    std::stringstream msg;

    for ( AUTOSQL_INT col = 0; col < _targets.ncols(); ++col )
        {
            containers::Matrix<AUTOSQL_FLOAT> y = _targets.column( col );

            msg << predictors()[col]->fit( _logger, _features, y );
        }

    // -----------------------------------------------------------
    // Calculate sum of feature importances

    std::vector<AUTOSQL_FLOAT> feature_importances( trees().size() );

    for ( auto &predictor : predictors() )
        {
            auto temp = predictor->feature_importances( trees().size() );

            std::transform(
                temp.begin(),
                temp.end(),
                feature_importances.begin(),
                feature_importances.begin(),
                std::plus<AUTOSQL_FLOAT>() );
        }

    // -----------------------------------------------------------
    // Build index and sort by sum of feature importances

    std::vector<AUTOSQL_SIZE> index( trees().size() );

    for ( AUTOSQL_SIZE ix = 0; ix < index.size(); ++ix )
        {
            index[ix] = ix;
        }

    std::sort(
        index.begin(),
        index.end(),
        [feature_importances](
            const AUTOSQL_SIZE &ix1, const AUTOSQL_SIZE &ix2 ) {
            return feature_importances[ix1] > feature_importances[ix2];
        } );

    // -----------------------------------------------------------
    // Select the first _num_selected_features trees and linear regressions

    std::vector<DecisionTree> selected_trees;

    std::vector<LinearRegression> selected_linear_regressions;

    const AUTOSQL_INT nselect = std::min(
        hyperparameters()->num_selected_features,
        static_cast<AUTOSQL_INT>( index.size() ) );

    for ( AUTOSQL_INT i = 0; i < nselect; ++i )
        {
            auto ix = index[i];

            if ( feature_importances[ix] == 0.0 )
                {
                    break;
                }

            selected_trees.push_back( trees()[ix] );

            selected_linear_regressions.push_back( linear_regressions()[ix] );
        }

    trees() = std::move( selected_trees );

    linear_regressions() = std::move( selected_linear_regressions );

    // -----------------------------------------------------------
    // Clear scores, predictors

    scores() = descriptors::Scores{};

    predictors().clear();

    // -----------------------------------------------------------
    // Return message

    msg << std::endl << "Selected " << trees().size() << " features.";

    return msg.str();

    // -----------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::set_num_columns(
    std::vector<containers::DataFrame> &_peripheral_tables,
    containers::DataFrame &_population_table )
{
    num_columns_peripheral_categorical().clear();

    for ( auto &df : _peripheral_tables )
        {
            num_columns_peripheral_categorical().push_back(
                df.categorical().ncols() );
        }

    num_columns_peripheral_discrete().clear();

    for ( auto &df : _peripheral_tables )
        {
            num_columns_peripheral_discrete().push_back(
                df.discrete().ncols() );
        }

    num_columns_peripheral_numerical().clear();

    for ( auto &df : _peripheral_tables )
        {
            num_columns_peripheral_numerical().push_back(
                df.numerical().ncols() );
        }

    num_columns_population_categorical() =
        _population_table.categorical().ncols();

    num_columns_population_discrete() = _population_table.discrete().ncols();

    num_columns_population_numerical() = _population_table.numerical().ncols();
}

// ----------------------------------------------------------------------------

descriptors::SourceImportances DecisionTreeEnsemble::source_importances()
{
    // ----------------------------------------------------------------

    descriptors::SourceImportances importances;

    // ----------------------------------------------------------------
    // Get importances from trees

    for ( auto &tree : trees() )
        {
            tree.source_importances( importances );
        }

    // ----------------------------------------------------------------
    // Normalize to 1.0

    {
        AUTOSQL_FLOAT total = std::accumulate(
            importances.aggregation_imp_.begin(),
            importances.aggregation_imp_.end(),
            0.0,
            []( const AUTOSQL_FLOAT &init,
                const std::pair<
                    descriptors::SourceImportancesColumn,
                    AUTOSQL_FLOAT> &elem ) { return init + elem.second; } );

        for ( auto it = importances.aggregation_imp_.begin();
              it != importances.aggregation_imp_.end();
              ++it )
            {
                it->second /= total;
            }
    }

    {
        AUTOSQL_FLOAT total = std::accumulate(
            importances.condition_imp_.begin(),
            importances.condition_imp_.end(),
            0.0,
            []( const AUTOSQL_FLOAT &init,
                const std::pair<
                    descriptors::SourceImportancesColumn,
                    AUTOSQL_FLOAT> &elem ) { return init + elem.second; } );

        for ( auto it = importances.condition_imp_.begin();
              it != importances.condition_imp_.end();
              ++it )
            {
                it->second /= total;
            }
    }

    // ----------------------------------------------------------------

    return importances;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DecisionTreeEnsemble::to_monitor(
    const std::string _name ) const
{
    // ----------------------------------------

    Poco::JSON::Object obj;

    obj.set( "name_", _name );

    // ----------------------------------------

    if ( has_been_fitted() )
        {
            // ----------------------------------------
            // Express model as JSON string
            {
                Poco::JSON::Object obj_json;

                // ----------------------------------------
                // Extract placeholders

                obj.set(
                    "peripheral_",
                    JSON::vector_to_array( placeholder_peripheral() ) );

                obj.set(
                    "population_", placeholder_population()->to_json_obj() );

                // ----------------------------------------
                // Extract features

                Poco::JSON::Array features;

                for ( size_t i = 0; i < trees().size(); ++i )
                    {
                        features.add( trees()[i].to_monitor(
                            std::to_string( i + 1 ),
                            hyperparameters().use_timestamps ) );
                    }

                obj_json.set( "features_", features );

                // ----------------------------------------
                // Extract targets

                obj.set(
                    "targets_",
                    JSON::vector_to_array<std::string>( targets() ) );

                // ----------------------------------------
                // Add to obj

                obj.set( "json_", JSON::stringify( obj_json ) );

                // ----------------------------------------
            }

            // ----------------------------------------
            // Insert placeholders

            obj.set(
                "peripheral_",
                JSON::vector_to_array( placeholder_peripheral() ) );

            obj.set( "population_", placeholder_population()->to_json_obj() );

            // ----------------------------------------
            // Insert hyperparameters

            obj.set( "hyperparameters_", hyperparameters().to_json_obj() );

            // ----------------------------------------
            // Insert scores

            obj.set( "scores_", scores().to_json_obj() );

            // ----------------------------------------
            // Insert sql

            {
                std::vector<std::string> sql;

                for ( AUTOSQL_INT i = 0;
                      i < static_cast<AUTOSQL_INT>( trees().size() );
                      ++i )
                    {
                        sql.push_back( trees()[i].to_sql(
                            std::to_string( i + 1 ),
                            hyperparameters().use_timestamps ) );
                    }

                obj.set( "sql_", sql );
            }

            // ----------------------------------------
            // Insert other pieces of information

            obj.set( "nfeatures_", trees().size() );

            // ----------------------------------------
        }

    return obj;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DecisionTreeEnsemble::to_json_obj()
{
    // ----------------------------------------

    Poco::JSON::Object obj;

    // ----------------------------------------
    // Extract placeholders

    obj.set( "peripheral_", JSON::vector_to_array( placeholder_peripheral() ) );

    obj.set( "population_", placeholder_population()->to_json_obj() );

    // ----------------------------------------

    if ( has_been_fitted() )
        {
            // ----------------------------------------
            // Extract expected number of columns

            obj.set(
                "num_columns_peripheral_categorical_",
                num_columns_peripheral_categorical() );

            obj.set(
                "num_columns_peripheral_numerical_",
                num_columns_peripheral_numerical() );

            obj.set(
                "num_columns_peripheral_discrete_",
                num_columns_peripheral_discrete() );

            obj.set(
                "num_columns_population_categorical_",
                num_columns_population_categorical() );

            obj.set(
                "num_columns_population_numerical_",
                num_columns_population_numerical() );

            obj.set(
                "num_columns_population_discrete_",
                num_columns_population_discrete() );

            // ----------------------------------------
            // Extract features

            {
                Poco::JSON::Array features;

                for ( auto &tree : trees() )
                    {
                        features.add( tree.to_json_obj() );
                    }

                obj.set( "features_", features );
            }

            // ----------------------------------------
            // Extract targets

            obj.set(
                "targets_", JSON::vector_to_array<std::string>( targets() ) );

            // ----------------------------------------
            // Extract slopes from linear regressions.

            {
                Poco::JSON::Array slopes;

                for ( auto &linear_regression : linear_regressions() )
                    {
                        Poco::JSON::Array elem;

                        for ( AUTOSQL_FLOAT &val : linear_regression.slopes() )
                            {
                                elem.add( val );
                            }

                        slopes.add( elem );
                    }

                obj.set( "update_rates1_", slopes );
            }

            // ----------------------------------------
            // Extract intercepts from linear regressions.

            {
                Poco::JSON::Array intercepts;

                for ( auto &linear_regression : linear_regressions() )
                    {
                        Poco::JSON::Array elem;

                        for ( AUTOSQL_FLOAT &val :
                              linear_regression.intercepts() )
                            {
                                elem.add( val );
                            }

                        intercepts.add( elem );
                    }

                obj.set( "update_rates2_", intercepts );
            }

            // ----------------------------------------
            // Extract hyperparameters

            obj.set( "hyperparameters_", hyperparameters()->to_json_obj() );

            // ----------------------------------------
            // Extract scores

            obj.set( "scores_", scores().to_json_obj() );

            // ----------------------------------------
        }

    return obj;
}

// ----------------------------------------------------------------------------

std::string DecisionTreeEnsemble::to_sql() const
{
    std::stringstream sql;

    for ( AUTOSQL_SIZE i = 0; i < trees().size(); ++i )
        {
            sql << trees()[i].to_sql(
                std::to_string( i + 1 ), hyperparameters().use_timestamps );
        }

    return sql.str();
}

// ----------------------------------------------------------------------------

containers::Matrix<AUTOSQL_FLOAT> DecisionTreeEnsemble::transform(
    const std::shared_ptr<const logging::Logger> _logger,
    std::vector<containers::DataFrame> _peripheral_tables_raw,
    containers::DataFrameView _population_table_raw,
    const bool _transpose,
    const bool _score )
{
    // ----------------------------------------------------------------

    if ( has_been_fitted() == false )
        {
            throw std::invalid_argument( "Model has not been fitted!" );
        }

        // ----------------------------------------------------------------
        // Create abstractions over the peripheral_tables and the population
        // table - for convenience

#ifdef AUTOSQL_MULTINODE_MPI

    // Prepare tables is already called by rearrange_tables
    // - no need to do it again!

    auto peripheral_tables = _peripheral_tables_raw;

    auto population_table = _population_table_raw;

#else  // AUTOSQL_MULTINODE_MPI

    TableHolder table_holder = TablePreparer::prepare_tables(
        *placeholder_population(),
        placeholder_peripheral(),
        _peripheral_tables_raw,
        _population_table_raw );

#endif  // AUTOSQL_MULTINODE_MPI

    // ----------------------------------------------------------------
    // Make sure that the data passed by the user is plausible

    check_plausibility(
        table_holder.peripheral_tables, table_holder.main_table.df() );

    // ----------------------------------------------------------------
    // aggregations::AggregationImpl stores most of the data for the
    // aggregations. We do not want to reallocate the data all the time.

    bool reset_aggregation_impl = false;

    if ( !aggregation_impl() )
        {
            aggregation_impl().reset( new aggregations::AggregationImpl(
                table_holder.main_table.nrows() ) );

            reset_aggregation_impl = true;
        }

    for ( auto &tree : trees() )
        {
            tree.set_aggregation_impl( aggregation_impl() );
        }

    // ----------------------------------------------------------------
    // Every tree in the ensemble stands for one feature - extract
    // the feature and append

    containers::Matrix<AUTOSQL_FLOAT> features(
        0, table_holder.main_table.nrows() );

    for ( AUTOSQL_SIZE ix_feature = 0; ix_feature < trees().size();
          ++ix_feature )
        {
            auto &tree = trees()[ix_feature];

            auto new_feature =
                tree.transform(
                        table_holder, hyperparameters()->use_timestamps )
                    .transpose();

            debug_message(
                "transform: Adding new feature to existing features..." );

            features.append( new_feature );

            log( _logger,
                 "Built FEATURE_" + std::to_string( ix_feature + 1 ) + "." );
        }

    debug_message( "transform: Built all features." );

    // ----------------------------------------------------------------
    // Clean up

    if ( reset_aggregation_impl )
        {
            aggregation_impl().reset();
        }

    // ----------------------------------------------------------------
    // Calculate feature statistics, if necessary

    if ( _score )
        {
            features = features.transpose();

            calculate_feature_stats( features, table_holder.main_table );
        }

    // ----------------------------------------------------------------

    if ( _transpose != _score )
        {
            return features.transpose();
        }
    else
        {
            return features;
        }

    // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace autosql
