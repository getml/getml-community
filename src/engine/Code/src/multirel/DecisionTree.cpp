#include "multirel/decisiontrees/decisiontrees.hpp"

namespace multirel
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

DecisionTree::DecisionTree(
    const std::shared_ptr<const std::vector<strings::String>> &_categories,
    const std::shared_ptr<const descriptors::TreeHyperparameters>
        &_tree_hyperparameters,
    const Poco::JSON::Object &_json_obj )
    : impl_( _categories, _tree_hyperparameters )
{
    debug_log( "Feature: Normal constructor..." );

    from_json_obj( _json_obj );

    impl_.comm_ = nullptr;
};

// ----------------------------------------------------------------------------

DecisionTree::DecisionTree(
    const std::string &_agg,
    const std::shared_ptr<const std::vector<strings::String>> &_categories,
    const std::shared_ptr<const descriptors::TreeHyperparameters>
        &_tree_hyperparameters,
    const size_t _ix_perip_used,
    const enums::DataUsed _data_used,
    const size_t _ix_column_used,
    const descriptors::SameUnits &_same_units,
    std::mt19937 *_random_number_generator,
    containers::Optional<aggregations::AggregationImpl> *_aggregation_impl,
    multithreading::Communicator *_comm )
    : impl_( _categories, _tree_hyperparameters )
{
    set_same_units( _same_units );

    column_to_be_aggregated().ix_perip_used = _ix_perip_used;

    column_to_be_aggregated().data_used = _data_used;

    column_to_be_aggregated().ix_column_used = _ix_column_used;

    assert_true( _agg != "" );

    impl_.aggregation_type_ = _agg;

    aggregation_ptr() = make_aggregation( enums::Mode::fit );

    impl_.tree_hyperparameters_ = _tree_hyperparameters;

    impl_.comm_ = _comm;

    impl()->random_number_generator_ = _random_number_generator;

    set_aggregation_impl( _aggregation_impl );
}

// ----------------------------------------------------------------------------

DecisionTree::DecisionTree( const DecisionTree &_other )
    : impl_( _other.impl_ ),
      root_( _other.root_ ),
      subtrees_( _other.subtrees() )
{
    debug_log( "Feature: Copy constructor..." );

    assert_true( _other.impl_.aggregation_type_ != "" );

    aggregation_ptr() = _other.make_aggregation( enums::Mode::fit );

    if ( root() )
        {
            root().get()->set_tree( impl() );
        }
}

// ----------------------------------------------------------------------------

DecisionTree::DecisionTree( DecisionTree &&_other ) noexcept
    : impl_( std::move( _other.impl_ ) ),
      root_( std::move( _other.root_ ) ),
      subtrees_( std::move( _other.subtrees() ) )
{
    debug_log( "Feature: Move constructor..." );

    if ( root() )
        {
            root().get()->set_tree( impl() );
        }
}

// ----------------------------------------------------------------------------

void DecisionTree::fit(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const containers::Subfeatures &_subfeatures,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    optimizationcriteria::OptimizationCriterion *_optimization_criterion )
{
    // ------------------------------------------------------------

    impl()->input_.reset(
        new containers::Placeholder( _peripheral.to_schema() ) );

    impl()->output_.reset(
        new containers::Placeholder( _population.df().to_schema() ) );

    // ------------------------------------------------------------
    // Prepare the root, the aggregation and the optimization criterion

    debug_log( "fit: Preparing new candidate..." );

    root().reset( new DecisionTreeNode(
        true,   // _is_activated
        1,      // _depth
        impl()  // _tree
        ) );

    aggregation()->reset();

    impl()->optimization_criterion_ = _optimization_criterion;

    aggregation()->set_optimization_criterion( optimization_criterion() );

    // ------------------------------------------------------------
    // Do the actual fitting (most of the time will be spent here)

    debug_log( "fit: Trying conditions..." );

    root()->fit_as_root(
        _population,
        _peripheral,
        _subfeatures,
        _match_container_begin,
        _match_container_end );

    // ------------------------------------------------------------
    // Clean up

    impl()->clear();

    // ------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::map<std::string, Float> DecisionTree::column_importances(
    const Float _importance_factor ) const
{
    if ( _importance_factor == 0.0 )
        {
            return std::map<std::string, Float>();
        }

    auto importance_maker = utils::ImportanceMaker();

    assert_true( root_ );

    root_->column_importances( &importance_maker );

    importance_maker.normalize();

    importance_maker.multiply( _importance_factor );

    return importance_maker.importances();
}

// ----------------------------------------------------------------------------

void DecisionTree::from_json_obj( const Poco::JSON::Object &_json_obj )
{
    // -----------------------------------

    impl()->input_.reset( new containers::Placeholder(
        *JSON::get_object( _json_obj, "input_" ) ) );

    impl()->output_.reset( new containers::Placeholder(
        *JSON::get_object( _json_obj, "output_" ) ) );

    column_to_be_aggregated() = descriptors::ColumnToBeAggregated(
        *JSON::get_object( _json_obj, "column_" ) );

    impl()->same_units_ =
        descriptors::SameUnits( *JSON::get_object( _json_obj, "same_units_" ) );

    // -----------------------------------

    const auto agg = JSON::get_value<std::string>( _json_obj, "aggregation_" );

    assert_true( agg != "" );

    impl_.aggregation_type_ = agg;

    aggregation_ptr() = make_aggregation( enums::Mode::fit );

    // -----------------------------------

    root().reset( new DecisionTreeNode(
        true,   // _is_activated
        1,      // _depth
        impl()  // _tree
        ) );

    root()->from_json_obj( *JSON::get_object( _json_obj, "conditions_" ) );

    // -----------------------------------
}

// ---------------------------------------------------------------------------

DecisionTree &DecisionTree::operator=( const DecisionTree &_other )
{
    debug_log( "Feature: Copy assignment constructor..." );

    DecisionTree temp( _other );

    *this = std::move( temp );

    if ( root() )
        {
            root().get()->set_tree( impl() );
        }

    return *this;
}

// ----------------------------------------------------------------------------

DecisionTree &DecisionTree::operator=( DecisionTree &&_other ) noexcept
{
    debug_log( "Feature: Move assignment constructor..." );

    if ( this == &_other )
        {
            return *this;
        }

    impl_ = std::move( _other.impl_ );

    root_ = std::move( _other.root_ );

    subtrees_ = std::move( _other.subtrees_ );

    if ( root() )
        {
            root().get()->set_tree( impl() );
        }

    return *this;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DecisionTree::to_json_obj() const
{
    // -----------------------------------

    if ( !impl()->input_ || !impl()->output_ || !root() )
        {
            throw std::runtime_error( "Feature has not been trained!" );
        }

    // -----------------------------------

    Poco::JSON::Object obj;

    // -----------------------------------

    obj.set( "aggregation_", aggregation()->type() );

    obj.set( "column_", column_to_be_aggregated().to_json_obj() );

    obj.set( "conditions_", root()->to_json_obj() );

    obj.set( "input_", impl()->input().to_json_obj() );

    obj.set( "output_", impl()->output().to_json_obj() );

    obj.set( "same_units_", impl()->same_units_.to_json_obj() );

    // -----------------------------------

    return obj;

    // -----------------------------------
}

// ----------------------------------------------------------------------------

std::string DecisionTree::to_sql(
    const std::string _feature_num, const bool _use_timestamps ) const
{
    // -------------------------------------------------------------------

    std::stringstream sql;

    const auto sql_maker = utils::SQLMaker(
        impl()->categories_,
        impl()->delta_t(),
        ix_perip_used(),
        impl()->same_units_ );

    // -------------------------------------------------------------------

    sql << "CREATE TABLE \"FEATURE_" << _feature_num << "\" AS" << std::endl;

    // -------------------------------------------------------------------

    sql << "SELECT ";

    sql << sql_maker.select_statement(
        input(),
        output(),
        column_to_be_aggregated().ix_column_used,
        column_to_be_aggregated().data_used,
        aggregation_type() );

    sql << " AS \"feature_" << _feature_num << "\"," << std::endl;

    sql << "       t1.rowid AS \"rownum\"" << std::endl;

    // -------------------------------------------------------------------

    sql << "FROM \"" << output().name() << "\" t1" << std::endl;

    sql << "LEFT JOIN \"" << input().name() << "\" t2" << std::endl;

    sql << "ON t1.\"" << output().join_keys_name() << "\" = t2.\""
        << input().join_keys_name() << "\"" << std::endl;

    // -------------------------------------------------------------------

    std::vector<std::string> conditions;

    root()->to_sql( _feature_num, conditions, "" );

    for ( size_t i = 0; i < conditions.size(); ++i )
        {
            if ( i == 0 )
                {
                    sql << "WHERE (" << std::endl
                        << "   ( " << conditions[i] << " )" << std::endl;
                }
            else
                {
                    sql << "OR ( " << conditions[i] << " )" << std::endl;
                }
        }

    // -------------------------------------------------------------------

    if ( _use_timestamps && input().num_time_stamps() > 0 &&
         output().num_time_stamps() > 0 )
        {
            if ( conditions.size() > 0 )
                {
                    sql << ") AND ";
                }
            else
                {
                    sql << "WHERE ";
                }

            sql << "datetime( t2.\"" << input().time_stamps_name()
                << "\" ) <= datetime( t1.\"" << output().time_stamps_name()
                << "\" )" << std::endl;

            if ( input().num_time_stamps() == 2 )
                {
                    sql << "AND ( datetime( t2.\""
                        << input().upper_time_stamps_name()
                        << "\" ) > datetime( t1.\""
                        << output().time_stamps_name()
                        << "\" ) OR datetime( t2.\""
                        << input().upper_time_stamps_name() << "\" ) IS NULL )"
                        << std::endl;
                }
        }
    else
        {
            if ( conditions.size() > 0 )
                {
                    sql << ")" << std::endl;
                }
        }

    sql << "GROUP BY t1.rowid;" << std::endl << std::endl << std::endl;

    // -------------------------------------------------------------------

    return sql.str();
}

// ----------------------------------------------------------------------------

std::vector<Float> DecisionTree::transform(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const containers::Subfeatures &_subfeatures,
    const bool _use_timestamps,
    aggregations::AbstractAggregation *_aggregation ) const
{
    // ------------------------------------------------------
    // Prepare the aggregation

    _aggregation->reset();

    _aggregation->set_optimization_criterion( nullptr );

    // ------------------------------------------------------
    // This is put in a loop to avoid the sample containers
    // taking up too much memory.

    for ( size_t ix_x_popul = 0; ix_x_popul < _population.nrows();
          ++ix_x_popul )
        {
            // ------------------------------------------------------
            // Create matches and match pointers.

            debug_log( "transform: Create sample containers..." );

            containers::Matches matches;

            utils::Matchmaker::make_matches(
                _population,
                _peripheral,
                _use_timestamps,
                ix_x_popul,
                &matches );

            auto match_ptrs = utils::Matchmaker::make_pointers( &matches );

            // ------------------------------------------------------

            create_value_to_be_aggregated(
                _population, _peripheral, _subfeatures, _aggregation );

            // ------------------------------------------------------
            // Separate null values, tell the aggregation where the samples
            // begin and end and sort the samples, if necessary

            debug_log( "transform: Set begin, end..." );

            auto null_values_dist =
                std::distance( matches.begin(), matches.begin() );

            if ( aggregation_type() != "COUNT" )
                {
                    auto null_values_separator =
                        _aggregation->separate_null_values( &matches );

                    null_values_dist =
                        std::distance( matches.begin(), null_values_separator );

                    _aggregation->set_samples_begin_end(
                        matches.data() + null_values_dist,
                        matches.data() + matches.size() );

                    if ( _aggregation->needs_sorting() )
                        {
                            _aggregation->sort_matches(
                                null_values_separator, matches.end() );
                        }

                    // Because keep on generating matches and match_container,
                    // we do not have to explicitly sort the sample containers!
                }
            else
                {
                    _aggregation->set_samples_begin_end(
                        matches.data(), matches.data() + matches.size() );
                }

            // ------------------------------------------------------
            // Do the actual transformation

            debug_log( "transform: Activate..." );

            _aggregation->activate_all(
                false,
                match_ptrs.begin() + null_values_dist,
                match_ptrs.end() );

            debug_log( "transform: Do actual transformation..." );

            root()->transform(
                _population,
                _peripheral,
                _subfeatures,
                match_ptrs.begin() + null_values_dist,
                match_ptrs.end(),
                _aggregation );

            // ------------------------------------------------------
            // Some aggregations, such as min and max contain additional
            // containers. If we do not clear them, they will use up too
            // much memory. For other aggregations, this does nothing at
            // all.

            debug_log( "transform: Clear extras..." );

            _aggregation->clear_extras();

            // ------------------------------------------------------
        }

    // ------------------------------------------------------

    auto yhat = _aggregation->yhat();

    // ------------------------------------------------------

    return yhat;

    // ------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace multirel
