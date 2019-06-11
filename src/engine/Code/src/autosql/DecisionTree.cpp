#include "autosql/decisiontrees/decisiontrees.hpp"

namespace autosql
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

DecisionTree::DecisionTree( const Poco::JSON::Object &_json_obj )
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
    containers::Optional<aggregations::AggregationImpl> *_aggregation_impl,
    multithreading::Communicator *_comm )
{
    set_same_units( _same_units );

    column_to_be_aggregated().ix_perip_used = _ix_perip_used;

    column_to_be_aggregated().data_used = _data_used;

    column_to_be_aggregated().ix_column_used = _ix_column_used;

    assert( _agg != "" );

    impl_.aggregation_type_ = _agg;

    aggregation_ptr() = make_aggregation();

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

    assert( _other.impl_.aggregation_type_ != "" );

    aggregation_ptr() = _other.make_aggregation();

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

void DecisionTree::create_value_to_be_aggregated(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const std::vector<containers::ColumnView<
        AUTOSQL_FLOAT,
        std::map<AUTOSQL_INT, AUTOSQL_INT>>> &_subfeatures,
    const AUTOSQL_SAMPLE_CONTAINER &_sample_container,
    aggregations::AbstractAggregation *_aggregation ) const
{
    // ------------------------------------------------------------------------

    const AUTOSQL_INT ix_column_used = column_to_be_aggregated().ix_column_used;

    switch ( column_to_be_aggregated().data_used )
        {
            case enums::DataUsed::x_perip_numerical:

                _aggregation->set_value_to_be_aggregated(
                    _peripheral.numerical_col( ix_column_used ) );

                break;

            case enums::DataUsed::x_perip_discrete:

                _aggregation->set_value_to_be_aggregated(
                    _peripheral.discrete_col( ix_column_used ) );

                break;

            case enums::DataUsed::time_stamps_diff:

                _aggregation->set_value_to_be_aggregated(
                    _peripheral.time_stamp_col() );

                _aggregation->set_value_to_be_compared(
                    _population.time_stamp_col() );

                break;

            case enums::DataUsed::same_unit_numerical:

                assert(
                    static_cast<AUTOSQL_INT>(
                        impl()->same_units_numerical().size() ) >
                    ix_column_used );

                {
                    const enums::DataUsed data_used1 =
                        std::get<0>(
                            impl()->same_units_numerical()[ix_column_used] )
                            .data_used;

                    const enums::DataUsed data_used2 =
                        std::get<1>(
                            impl()->same_units_numerical()[ix_column_used] )
                            .data_used;

                    const AUTOSQL_INT ix_column_used1 =
                        std::get<0>(
                            impl()->same_units_numerical()[ix_column_used] )
                            .ix_column_used;

                    const AUTOSQL_INT ix_column_used2 =
                        std::get<1>(
                            impl()->same_units_numerical()[ix_column_used] )
                            .ix_column_used;

                    if ( data_used1 == enums::DataUsed::x_perip_numerical )
                        {
                            assert(
                                _peripheral.num_numericals() >
                                ix_column_used1 );

                            _aggregation->set_value_to_be_aggregated(
                                _peripheral.numerical_col( ix_column_used1 ) );
                        }
                    else
                        {
                            assert( !"Unknown data_used1 in set_value_to_be_aggregated(...)!" );
                        }

                    if ( data_used2 == enums::DataUsed::x_popul_numerical )
                        {
                            assert(
                                _population.num_numericals() >
                                ix_column_used2 );

                            _aggregation->set_value_to_be_compared(
                                _population.numerical_col( ix_column_used2 ) );
                        }
                    else if ( data_used2 == enums::DataUsed::x_perip_numerical )
                        {
                            assert(
                                _peripheral.num_numericals() >
                                ix_column_used2 );

                            _aggregation->set_value_to_be_compared(
                                _peripheral.numerical_col( ix_column_used2 ) );
                        }
                    else
                        {
                            assert( !"Unknown data_used2 in set_value_to_be_compared(...)!" );
                        }
                }

                break;

            case enums::DataUsed::same_unit_discrete:

                assert(
                    static_cast<AUTOSQL_INT>(
                        impl()->same_units_discrete().size() ) >
                    ix_column_used );

                {
                    const enums::DataUsed data_used1 =
                        std::get<0>(
                            impl()->same_units_discrete()[ix_column_used] )
                            .data_used;

                    const enums::DataUsed data_used2 =
                        std::get<1>(
                            impl()->same_units_discrete()[ix_column_used] )
                            .data_used;

                    const AUTOSQL_INT ix_column_used1 =
                        std::get<0>(
                            impl()->same_units_discrete()[ix_column_used] )
                            .ix_column_used;

                    const AUTOSQL_INT ix_column_used2 =
                        std::get<1>(
                            impl()->same_units_discrete()[ix_column_used] )
                            .ix_column_used;

                    if ( data_used1 == enums::DataUsed::x_perip_discrete )
                        {
                            assert(
                                _peripheral.num_discretes() > ix_column_used1 );

                            _aggregation->set_value_to_be_aggregated(
                                _peripheral.discrete_col( ix_column_used1 ) );
                        }
                    else
                        {
                            assert( !"Unknown data_used1 in set_value_to_be_aggregated(...)!" );
                        }

                    if ( data_used2 == enums::DataUsed::x_popul_discrete )
                        {
                            assert(
                                _population.num_discretes() > ix_column_used2 );

                            _aggregation->set_value_to_be_compared(
                                _population.discrete_col( ix_column_used2 ) );
                        }
                    else if ( data_used2 == enums::DataUsed::x_perip_discrete )
                        {
                            assert(
                                _peripheral.num_discretes() > ix_column_used2 );

                            _aggregation->set_value_to_be_compared(
                                _peripheral.discrete_col( ix_column_used2 ) );
                        }
                    else
                        {
                            assert( !"Unknown data_used2 in set_value_to_be_compared(...)!" );
                        }
                }

                break;

            case enums::DataUsed::x_perip_categorical:

                _aggregation->set_value_to_be_aggregated(
                    _peripheral.categorical_col( ix_column_used ) );

                break;

            case enums::DataUsed::x_subfeature:

                assert( ix_column_used < _subfeatures.size() );

                _aggregation->set_value_to_be_aggregated(
                    _subfeatures[ix_column_used] );

                break;

            case enums::DataUsed::not_applicable:

                break;

            default:

                assert( !"Unknown enums::DataUsed in column_to_be_aggregated(...)!" );
        }

    // ---------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTree::fit(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const std::vector<containers::ColumnView<
        AUTOSQL_FLOAT,
        std::map<AUTOSQL_INT, AUTOSQL_INT>>> &_subfeatures,
    AUTOSQL_SAMPLE_CONTAINER::iterator _sample_container_begin,
    AUTOSQL_SAMPLE_CONTAINER::iterator _sample_container_end,
    optimizationcriteria::OptimizationCriterion *_optimization_criterion )
{
    // ------------------------------------------------------------

    impl()->input_.reset( new containers::Schema( _peripheral.to_schema() ) );

    impl()->output_.reset(
        new containers::Schema( _population.df().to_schema() ) );

    // ------------------------------------------------------------
    // Prepare the root, the aggregation and the optimization criterion

    debug_log( "fit: Preparing new candidate..." );

    root().reset( new DecisionTreeNode(
        true,   // _is_activated
        1,      // _depth
        impl()  // _tree
        ) );

    aggregation()->reset();

    optimization_criterion() = _optimization_criterion;

    aggregation()->set_optimization_criterion( optimization_criterion() );

    // ------------------------------------------------------------
    // Do the actual fitting (most of the time will be spent here)

    debug_log( "fit: Trying conditions..." );

    root()->fit_as_root(
        _population,
        _peripheral,
        _subfeatures,
        _sample_container_begin,
        _sample_container_end );

    // ------------------------------------------------------------
    // Clean up

    impl()->clear();

    // ------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTree::from_json_obj( const Poco::JSON::Object &_json_obj )
{
    assert( false && "ToDo" );

    // -----------------------------------

    /*  peripheral_name() =
          JSON::get_value<std::string>( _json_obj, "peripheral_name_" );

      population_name() =
          JSON::get_value<std::string>( _json_obj, "population_name_" );

      join_keys_perip_name() =
          JSON::get_value<std::string>( _json_obj, "join_keys_perip_name_" );

      join_keys_popul_name() =
          JSON::get_value<std::string>( _json_obj, "join_keys_popul_name_" );

      time_stamps_perip_name() =
          JSON::get_value<std::string>( _json_obj, "time_stamps_perip_name_" );

      time_stamps_popul_name() =
          JSON::get_value<std::string>( _json_obj, "time_stamps_popul_name_" );

      if ( _json_obj.has( "upper_time_stamps_" ) )
          {
              upper_time_stamps_name() =
                  JSON::get_value<std::string>( _json_obj, "upper_time_stamps_"
      );
          }
      else
          {
              upper_time_stamps_name() = "";
          }

      // -----------------------------------

      impl_.x_perip_categorical_colnames_.reset( new std::vector<std::string>()
      );

      x_perip_categorical_colnames() = JSON::array_to_vector<std::string>(
          JSON::get_array( _json_obj, "x_perip_categorical_colnames_" ) );

      // --

      impl_.x_perip_numerical_colnames_.reset( new std::vector<std::string>() );

      x_perip_numerical_colnames() = JSON::array_to_vector<std::string>(
          JSON::get_array( _json_obj, "x_perip_numerical_colnames_" ) );

      // --

      impl_.x_perip_discrete_colnames_.reset( new std::vector<std::string>() );

      x_perip_discrete_colnames() = JSON::array_to_vector<std::string>(
          JSON::get_array( _json_obj, "x_perip_discrete_colnames_" ) );
      // --

      impl_.x_popul_categorical_colnames_.reset( new std::vector<std::string>()
      );

      x_popul_categorical_colnames() = JSON::array_to_vector<std::string>(
          JSON::get_array( _json_obj, "x_popul_categorical_colnames_" ) );

      // --

      impl_.x_popul_numerical_colnames_.reset( new std::vector<std::string>() );

      x_popul_numerical_colnames() = JSON::array_to_vector<std::string>(
          JSON::get_array( _json_obj, "x_popul_numerical_colnames_" ) );

      // --

      impl_.x_popul_discrete_colnames_.reset( new std::vector<std::string>() );

      x_popul_discrete_colnames() = JSON::array_to_vector<std::string>(
          JSON::get_array( _json_obj, "x_popul_discrete_colnames_" ) );*/

    // -----------------------------------

    column_to_be_aggregated().ix_column_used =
        JSON::get_value<size_t>( _json_obj, "column_" );

    column_to_be_aggregated().data_used =
        JSON::int_to_data_used( JSON::get_value<int>( _json_obj, "data_" ) );

    column_to_be_aggregated().ix_perip_used =
        JSON::get_value<size_t>( _json_obj, "input_" );

    root().reset( new DecisionTreeNode(
        true,   // _is_activated
        1,      // _depth
        impl()  // _tree
        ) );

    root()->from_json_obj( *JSON::get_object( _json_obj, "conditions_" ) );

    // -----------------------------------

    impl()->same_units_.same_units_categorical_ =
        std::make_shared<AUTOSQL_SAME_UNITS_CONTAINER>(
            JSON::json_arr_to_same_units(
                *JSON::get_array( _json_obj, "same_units_categorical_" ) ) );

    impl()->same_units_.same_units_discrete_ =
        std::make_shared<AUTOSQL_SAME_UNITS_CONTAINER>(
            JSON::json_arr_to_same_units(
                *JSON::get_array( _json_obj, "same_units_discrete_" ) ) );

    impl()->same_units_.same_units_numerical_ =
        std::make_shared<AUTOSQL_SAME_UNITS_CONTAINER>(
            JSON::json_arr_to_same_units(
                *JSON::get_array( _json_obj, "same_units_numerical_" ) ) );

    // -----------------------------------

    const auto agg = JSON::get_value<std::string>( _json_obj, "aggregation_" );

    assert( agg != "" );

    impl_.aggregation_type_ = agg;

    aggregation_ptr() = make_aggregation();

    // -----------------------------------

    const auto subtrees_arr = JSON::get_array( _json_obj, "subfeatures_" );

    subtrees().clear();

    for ( size_t i = 0; i < subtrees_arr->size(); ++i )
        {
            subtrees().push_back( DecisionTree(
                *subtrees_arr->getObject( static_cast<unsigned int>( i ) ) ) );
        }

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

std::string DecisionTree::select_statement(
    const std::string &_feature_num ) const
{
    std::string select;

    if ( aggregation()->type() == "COUNT DISTINCT" )
        {
            select.append( "COUNT( DISTINCT " );
        }
    else if ( aggregation()->type() == "COUNT MINUS COUNT DISTINCT" )
        {
            select.append( "COUNT( * ) - COUNT( DISTINCT " );
        }
    else
        {
            select.append( aggregation()->type() );

            select.append( "( " );
        }

    select.append( impl()->get_colname(
        _feature_num,
        column_to_be_aggregated().data_used,
        column_to_be_aggregated().ix_column_used ) );

    select.append( " )" );

    return select;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DecisionTree::to_json_obj()
{
    // -----------------------------------

    Poco::JSON::Object obj;

    // -----------------------------------

    assert( false && "ToDo" );

    /*obj.set( "peripheral_name_", peripheral_name() );
    obj.set( "population_name_", population_name() );

    obj.set( "join_keys_perip_name_", join_keys_perip_name() );
    obj.set( "join_keys_popul_name_", join_keys_popul_name() );

    obj.set( "time_stamps_perip_name_", time_stamps_perip_name() );
    obj.set( "time_stamps_popul_name_", time_stamps_popul_name() );

    if ( upper_time_stamps_name() != "" )
        {
            obj.set( "upper_time_stamps_", upper_time_stamps_name() );
        }

    // -----------------------------------

    obj.set( "x_perip_categorical_colnames_", x_perip_categorical_colnames() );

    obj.set( "x_perip_numerical_colnames_", x_perip_numerical_colnames() );

    obj.set( "x_perip_discrete_colnames_", x_perip_discrete_colnames() );

    obj.set( "x_popul_categorical_colnames_", x_popul_categorical_colnames() );

    obj.set( "x_popul_numerical_colnames_", x_popul_numerical_colnames() );

    obj.set( "x_popul_discrete_colnames_", x_popul_discrete_colnames() );*/

    // -----------------------------------

    obj.set( "aggregation_", aggregation()->type() );

    obj.set( "column_", column_to_be_aggregated().ix_column_used );

    obj.set( "conditions_", root()->to_json_obj() );

    obj.set(
        "data_",
        JSON::data_used_to_int( column_to_be_aggregated().data_used ) );

    obj.set( "input_", ix_perip_used() );

    // -----------------------------------

    obj.set(
        "same_units_categorical_",
        JSON::same_units_to_json_arr( impl()->same_units_categorical() ) );

    obj.set(
        "same_units_discrete_",
        JSON::same_units_to_json_arr( impl()->same_units_discrete() ) );

    obj.set(
        "same_units_numerical_",
        JSON::same_units_to_json_arr( impl()->same_units_numerical() ) );

    // -----------------------------------

    Poco::JSON::Array subtrees_arr;

    for ( auto &subtree : subtrees() )
        {
            subtrees_arr.add( subtree.to_json_obj() );
        }

    obj.set( "subfeatures_", subtrees_arr );

    // -----------------------------------

    return obj;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DecisionTree::to_monitor(
    const std::string &_feature_num, const bool _use_timestamps ) const
{
    // -------------------------------------------------------------------

    Poco::JSON::Object obj;

    // -------------------------------------------------------------------

    obj.set( "aggregation_", select_statement( _feature_num ) );

    obj.set( "join_keys_popul_", output().join_keys_name() );

    obj.set( "time_stamps_popul_", output().time_stamps_name() );

    obj.set( "join_keys_perip_", input().join_keys_name() );

    obj.set( "time_stamps_perip_", input().time_stamps_name() );

    if ( input().num_time_stamps() == 2 )
        {
            obj.set( "upper_time_stamps_", input().upper_time_stamps_name() );
        }

    obj.set( "population_", output().name() );

    obj.set( "peripheral_", input().name() );

    // -------------------------------------------------------------------

    Poco::JSON::Array node;

    Poco::JSON::Array conditions;

    root()->to_monitor( _feature_num, node, conditions );

    obj.set( "conditions_", conditions );

    // -------------------------------------------------------------------

    return obj;
}

// ----------------------------------------------------------------------------

std::string DecisionTree::to_sql(
    const std::string _feature_num, const bool _use_timestamps ) const
{
    std::stringstream sql;

    // -------------------------------------------------------------------

    for ( size_t i = 0; i < subtrees().size(); ++i )
        {
            sql << subtrees()[i].to_sql(
                _feature_num + "_" + std::to_string( i + 1 ), _use_timestamps );
        }

    // -------------------------------------------------------------------

    sql << "CREATE TABLE FEATURE_" << _feature_num << " AS" << std::endl;

    // -------------------------------------------------------------------

    sql << "SELECT ";

    sql << select_statement( _feature_num );

    sql << " AS feature_" << _feature_num << "," << std::endl;

    sql << "       t1." << output().join_keys_name() << "," << std::endl;

    sql << "       t1." << output().time_stamps_name() << std::endl;

    // -------------------------------------------------------------------

    sql << "FROM (" << std::endl;

    sql << "     SELECT *," << std::endl;

    sql << "            ROW_NUMBER() OVER ( ORDER BY "
        << output().join_keys_name() << ", " << output().time_stamps_name()
        << " ASC ) AS rownum" << std::endl;

    sql << "     FROM " << output().name() << std::endl;

    sql << ") t1" << std::endl;

    sql << "LEFT JOIN " << input().name() << " t2" << std::endl;

    sql << "ON t1." << output().join_keys_name() << " = t2."
        << input().join_keys_name() << std::endl;

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

    if ( _use_timestamps )
        {
            if ( conditions.size() > 0 )
                {
                    sql << ") AND ";
                }
            else
                {
                    sql << "WHERE ";
                }

            sql << "t2." << input().time_stamps_name() << " <= t1."
                << output().time_stamps_name() << std::endl;

            if ( input().num_time_stamps() == 2 )
                {
                    sql << "AND ( t2." << input().upper_time_stamps_name()
                        << " > t1." << output().time_stamps_name() << " OR t2."
                        << input().upper_time_stamps_name() << " IS NULL )"
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

    sql << "GROUP BY t1.rownum," << std::endl;

    sql << "         t1." << output().join_keys_name() << "," << std::endl;

    sql << "         t1." << output().time_stamps_name() << ";" << std::endl
        << std::endl
        << std::endl;

    // -------------------------------------------------------------------

    return sql.str();
}

// ----------------------------------------------------------------------------

std::vector<AUTOSQL_FLOAT> DecisionTree::transform(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const containers::Optional<TableHolder> &_subtable,
    const bool _use_timestamps,
    aggregations::AbstractAggregation *_aggregation ) const
{
    // ------------------------------------------------------
    // Prepare the aggregation

    _aggregation->reset();

    // ------------------------------------------------------
    // Build predictions for subfeatures, if applicable.

    const auto predictions = SubtreeHelper::make_predictions(
        _subtable, _use_timestamps, subtrees() );

    const auto subfeatures =
        SubtreeHelper::make_subfeatures( _subtable, predictions );

    // ------------------------------------------------------
    // This is put in a loop to avoid the sample containers
    // taking up too much memory

    for ( size_t ix_x_popul = 0; ix_x_popul < _population.nrows();
          ++ix_x_popul )
        {
            // ------------------------------------------------------
            // Create matches and match pointers.

            debug_log( "transform: Create sample containers..." );

            AUTOSQL_SAMPLES samples;

            utils::Matchmaker::make_matches(
                _population,
                _peripheral,
                _use_timestamps,
                ix_x_popul,
                &samples );

            auto match_ptrs = utils::Matchmaker::make_pointers( &samples );

            // ------------------------------------------------------

            create_value_to_be_aggregated(
                _population,
                _peripheral,
                subfeatures,
                match_ptrs,
                _aggregation );

            // ------------------------------------------------------
            // Separate null values, tell the aggregation where the samples
            // begin and end and sort the samples, if necessary

            debug_log( "transform: Set begin, end..." );

            auto null_values_dist =
                std::distance( samples.begin(), samples.begin() );

            if ( aggregation_type() != "COUNT" )
                {
                    auto null_values_separator =
                        _aggregation->separate_null_values( samples );

                    null_values_dist =
                        std::distance( samples.begin(), null_values_separator );

                    _aggregation->set_samples_begin_end(
                        samples.data() + null_values_dist,
                        samples.data() + samples.size() );

                    if ( _aggregation->needs_sorting() )
                        {
                            _aggregation->sort_samples(
                                null_values_separator, samples.end() );
                        }

                    // Because keep on generating samples and sample_container,
                    // we do not have to explicitly sort the sample containers!
                }
            else
                {
                    _aggregation->set_samples_begin_end(
                        samples.data(), samples.data() + samples.size() );
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
                subfeatures,
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
}  // namespace autosql
