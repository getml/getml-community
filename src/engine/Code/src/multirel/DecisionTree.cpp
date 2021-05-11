#include "multirel/decisiontrees/decisiontrees.hpp"

namespace multirel
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

DecisionTree::DecisionTree(
    const std::shared_ptr<const descriptors::TreeHyperparameters>
        &_tree_hyperparameters,
    const Poco::JSON::Object &_json_obj )
    : impl_( _tree_hyperparameters )
{
    debug_log( "Feature: Normal constructor..." );

    from_json_obj( _json_obj );

    impl_.comm_ = nullptr;
};

// ----------------------------------------------------------------------------

DecisionTree::DecisionTree(
    const std::string &_agg,
    const std::shared_ptr<const descriptors::TreeHyperparameters>
        &_tree_hyperparameters,
    const size_t _ix_perip_used,
    const enums::DataUsed _data_used,
    const size_t _ix_column_used,
    const descriptors::SameUnits &_same_units,
    std::mt19937 *_random_number_generator,
    multithreading::Communicator *_comm )
    : impl_( _tree_hyperparameters )
{
    set_same_units( _same_units );

    column_to_be_aggregated().ix_perip_used = _ix_perip_used;

    column_to_be_aggregated().data_used = _data_used;

    column_to_be_aggregated().ix_column_used = _ix_column_used;

    assert_true( _agg != "" );

    impl_.aggregation_type_ = _agg;

    impl_.tree_hyperparameters_ = _tree_hyperparameters;

    impl_.comm_ = _comm;

    impl()->random_number_generator_ = _random_number_generator;
}

// ----------------------------------------------------------------------------

DecisionTree::DecisionTree( const DecisionTree &_other )
    : impl_( _other.impl_ ),
      root_( _other.root_ ),
      subtrees_( _other.subtrees() )
{
    debug_log( "Feature: Copy constructor..." );

    assert_true( _other.impl_.aggregation_type_ != "" );

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
    const std::shared_ptr<aggregations::AbstractFitAggregation> &_aggregation,
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

    assert_true( _aggregation );

    impl()->aggregation_ = _aggregation;

    impl()->optimization_criterion_ = _optimization_criterion;

    root().reset( new DecisionTreeNode(
        true,   // _is_activated
        1,      // _depth
        impl()  // _tree
        ) );

    // ------------------------------------------------------------

    root()->fit_as_root(
        _population,
        _peripheral,
        _subfeatures,
        _match_container_begin,
        _match_container_end );

    // ------------------------------------------------------------

    impl()->clear();

    // ------------------------------------------------------------
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

    // -----------------------------------

    root().reset( new DecisionTreeNode(
        true,   // _is_activated
        1,      // _depth
        impl()  // _tree
        ) );

    root()->from_json_obj( *JSON::get_object( _json_obj, "conditions_" ) );

    // -----------------------------------
}

// ----------------------------------------------------------------------------

inline std::shared_ptr<optimizationcriteria::OptimizationCriterion>
DecisionTree::make_intermediate(
    std::shared_ptr<aggregations::IntermediateAggregationImpl> _impl ) const
{
    // --------------------------------------------------

    const bool avg_intermediate =
        aggregation_type() == aggregations::AggregationType::Avg::type() ||
        aggregation_type() == aggregations::AggregationType::Max::type() ||
        aggregation_type() == aggregations::AggregationType::Median::type() ||
        aggregation_type() == aggregations::AggregationType::Min::type();

    const bool no_intermediate =
        aggregation_type() == aggregations::AggregationType::Count::type() ||
        aggregation_type() ==
            aggregations::AggregationType::CountDistinct::type() ||
        aggregation_type() ==
            aggregations::AggregationType::CountMinusCountDistinct::type();

    assert_true( !no_intermediate );

    // --------------------------------------------------

    if ( avg_intermediate )
        {
            return std::make_shared<aggregations::IntermediateAggregation<
                aggregations::AggregationType::Avg>>( _impl );
        }

    // --------------------------------------------------

    if ( aggregation_type() == aggregations::AggregationType::Stddev::type() )
        {
            return std::make_shared<aggregations::IntermediateAggregation<
                aggregations::AggregationType::Stddev>>( _impl );
        }

    // --------------------------------------------------

    if ( aggregation_type() == aggregations::AggregationType::Skewness::type() )
        {
            return std::make_shared<aggregations::IntermediateAggregation<
                aggregations::AggregationType::Skewness>>( _impl );
        }

    // --------------------------------------------------

    if ( aggregation_type() == aggregations::AggregationType::Sum::type() )
        {
            return std::make_shared<aggregations::IntermediateAggregation<
                aggregations::AggregationType::Sum>>( _impl );
        }

    // --------------------------------------------------

    if ( aggregation_type() == aggregations::AggregationType::Var::type() )
        {
            return std::make_shared<aggregations::IntermediateAggregation<
                aggregations::AggregationType::Var>>( _impl );
        }

    // --------------------------------------------------

    assert_msg( false, "Unknown aggregation type: " + aggregation_type() );

    return std::shared_ptr<optimizationcriteria::OptimizationCriterion>();
}

// ----------------------------------------------------------------------------

std::set<size_t> DecisionTree::make_subfeatures_used() const
{
    std::set<size_t> subfeatures_used;

    if ( column_to_be_aggregated().data_used == enums::DataUsed::x_subfeature )
        {
            subfeatures_used.insert( column_to_be_aggregated().ix_column_used );
        }

    root()->add_subfeatures( &subfeatures_used );

    return subfeatures_used;
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

    obj.set( "aggregation_", aggregation_type() );

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
    const std::vector<strings::String> &_categories,
    const helpers::VocabularyTree &_vocabulary,
    const std::string &_feature_prefix,
    const std::string &_feature_num,
    const bool _use_timestamps,
    const std::tuple<bool, bool, bool> _has_subfeatures ) const
{
    // -------------------------------------------------------------------

    std::stringstream sql;

    const auto sql_maker = utils::SQLMaker(
        impl()->delta_t(), ix_perip_used(), impl()->same_units_ );

    // -------------------------------------------------------------------

    sql << "DROP TABLE IF EXISTS \"FEATURE_" << _feature_prefix << _feature_num
        << "\";" << std::endl
        << std::endl;

    // -------------------------------------------------------------------

    sql << "CREATE TABLE \"FEATURE_" << _feature_prefix << _feature_num
        << "\" AS" << std::endl;

    // -------------------------------------------------------------------

    sql << "SELECT ";

    sql << sql_maker.select_statement(
        _feature_prefix,
        input(),
        output(),
        column_to_be_aggregated().ix_column_used,
        column_to_be_aggregated().data_used,
        aggregation_type() );

    sql << " AS \"feature_" << _feature_prefix << _feature_num << "\","
        << std::endl;

    sql << "       t1.rowid AS \"rownum\"" << std::endl;

    // -------------------------------------------------------------------

    sql << helpers::SQLGenerator::make_joins(
        output().name(),
        input().name(),
        output().join_keys_name(),
        input().join_keys_name() );

    // -------------------------------------------------------------------

    const auto [has_normal_subfeatures, output_has_prop, input_has_prop] =
        _has_subfeatures;

    if ( has_normal_subfeatures )
        {
            sql << helpers::SQLGenerator::make_subfeature_joins(
                _feature_prefix, ix_perip_used() );
        }

    if ( output_has_prop )
        {
            sql << helpers::SQLGenerator::make_subfeature_joins(
                _feature_prefix,
                ix_perip_used(),
                "t1",
                "_PROPOSITIONALIZATION" );
        }

    if ( input_has_prop )
        {
            sql << helpers::SQLGenerator::make_subfeature_joins(
                _feature_prefix,
                ix_perip_used(),
                "t2",
                "_PROPOSITIONALIZATION" );
        }

    // -------------------------------------------------------------------

    const bool use_time_stamps =
        ( _use_timestamps && input().num_time_stamps() > 0 &&
          output().num_time_stamps() > 0 );

    if ( use_time_stamps )
        {
            sql << "WHERE ( ";

            const auto upper_ts = input().num_time_stamps() > 1
                                      ? input().upper_time_stamps_name()
                                      : std::string( "" );

            sql << helpers::SQLGenerator::make_time_stamps(
                output().time_stamps_name(),
                input().time_stamps_name(),
                upper_ts,
                "t1",
                "t2",
                "t1" );

            sql << ") ";
        }

    // -------------------------------------------------------------------

    std::vector<std::string> conditions;

    assert_true( ix_perip_used() < _vocabulary.peripheral().size() );

    root()->to_sql(
        _categories,
        _vocabulary.population(),
        _vocabulary.peripheral().at( ix_perip_used() ),
        _feature_prefix,
        _feature_num,
        conditions,
        "" );

    // -------------------------------------------------------------------

    for ( size_t i = 0; i < conditions.size(); ++i )
        {
            if ( i == 0 )
                {
                    const auto where_or_and = use_time_stamps ? "AND" : "WHERE";

                    sql << where_or_and << " (" << std::endl
                        << "   ( " << conditions.at( i ) << " )" << std::endl;
                }
            else
                {
                    sql << "OR ( " << conditions.at( i ) << " )" << std::endl;
                }

            if ( i + 1 == conditions.size() )
                {
                    sql << ")";
                }
        }

    sql << std::endl;

    // -------------------------------------------------------------------

    sql << "GROUP BY t1.rowid;" << std::endl << std::endl << std::endl;

    // -------------------------------------------------------------------

    return sql.str();
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::vector<Float>> DecisionTree::transform(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const containers::Subfeatures &_subfeatures,
    const bool _use_timestamps ) const
{
    // ------------------------------------------------------

    const auto aggregation =
        aggregations::TransformAggregationParser::parse_aggregation(
            { .aggregation_type_ = impl()->aggregation_type_,
              .column_to_be_aggregated_ = column_to_be_aggregated(),
              .population_ = _population,
              .peripheral_ = _peripheral,
              .same_units_discrete_ = same_units_discrete(),
              .same_units_numerical_ = same_units_numerical(),
              .subfeatures_ = _subfeatures } );

    assert_true( aggregation );

    // ------------------------------------------------------

    const auto yhat =
        std::make_shared<std::vector<Float>>( _population.nrows() );

    // ------------------------------------------------------

    for ( size_t ix_x_popul = 0; ix_x_popul < _population.nrows();
          ++ix_x_popul )
        {
            // ------------------------------------------------------

            containers::Matches matches;

            utils::Matchmaker::make_matches(
                _population,
                _peripheral,
                _use_timestamps,
                ix_x_popul,
                &matches );

            auto match_ptrs = utils::Matchmaker::make_pointers( &matches );

            // ------------------------------------------------------

            size_t skip = 0;

            if ( aggregation_type() !=
                 aggregations::AggregationType::Count::type() )
                {
                    const auto null_values_separator =
                        aggregation->separate_null_values( &match_ptrs );

                    assert_true( null_values_separator >= match_ptrs.begin() );

                    skip = static_cast<size_t>( std::distance(
                        match_ptrs.begin(), null_values_separator ) );
                }

            // ------------------------------------------------------

            const auto is_deactivated =
                [this, _population, _peripheral, _subfeatures](
                    containers::Match *_m ) -> bool {
                return !root()->transform(
                    _population, _peripheral, _subfeatures, _m );
            };

            const auto deactivated_separator = std::partition(
                match_ptrs.begin() + skip, match_ptrs.end(), is_deactivated );

            assert_true( deactivated_separator >= match_ptrs.begin() );

            skip = static_cast<size_t>(
                std::distance( match_ptrs.begin(), deactivated_separator ) );

            // ------------------------------------------------------

            const auto time_stamp =
                _peripheral.num_time_stamps() > 0
                    ? _peripheral.time_stamp_col()
                    : std::optional<containers::Column<Float>>();

            ( *yhat )[ix_x_popul] =
                aggregation->aggregate( match_ptrs, skip, time_stamp );

            if ( std::isnan( ( *yhat )[ix_x_popul] ) )
                {
                    ( *yhat )[ix_x_popul] = 0.0;
                }

            // ------------------------------------------------------
        }

    // ------------------------------------------------------

    return yhat;

    // ------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace multirel
