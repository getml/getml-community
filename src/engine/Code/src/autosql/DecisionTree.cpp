#include "decisiontrees/decisiontrees.hpp"

namespace autosql
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

DecisionTree::DecisionTree( const Poco::JSON::Object &_json_obj )
{
    debug_message( "Feature: Normal constructor..." );

    from_json_obj( _json_obj );

#ifdef AUTOSQL_PARALLEL
    impl_.comm_ = nullptr;
#endif  // AUTOSQL_PARALLEL
};

// ----------------------------------------------------------------------------

DecisionTree::DecisionTree(
    const std::string &_agg,
    const AUTOSQL_INT _ix_column_used,
    const DataUsed _data_used,
    const AUTOSQL_INT _ix_perip_used,
    const descriptors::SameUnits &_same_units,
    std::mt19937 &_random_number_generator,
    containers::Optional<aggregations::AggregationImpl> &_aggregation_impl )
{
    set_same_units( _same_units );

    column_to_be_aggregated().ix_column_used = _ix_column_used;

    column_to_be_aggregated().data_used = _data_used;

    column_to_be_aggregated().ix_perip_used = _ix_perip_used;

    assert( _agg != "" );

    aggregation_ptr() = parse_aggregation( _agg );

    impl_.aggregation_type_ = _agg;

#ifdef AUTOSQL_PARALLEL
    impl_.comm_ = nullptr;
#endif  // AUTOSQL_PARALLEL

    impl()->random_number_generator_ = &_random_number_generator;

    set_aggregation_impl( _aggregation_impl );
}

// ----------------------------------------------------------------------------

DecisionTree::DecisionTree( const DecisionTree &_other )
    : impl_( _other.impl_ ),
      root_( _other.root_ ),
      subtrees_( _other.subtrees() )
{
    debug_message( "Feature: Copy constructor..." );

    assert( _other.impl_.aggregation_type_ != "" );

    aggregation_ptr() = parse_aggregation( _other.impl_.aggregation_type_ );

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
    debug_message( "Feature: Move constructor..." );

    if ( root() )
        {
            root().get()->set_tree( impl() );
        }
}

// ----------------------------------------------------------------------------

void DecisionTree::create_value_to_be_aggregated(
    TableHolder &_table_holder, AUTOSQL_SAMPLE_CONTAINER &_sample_container )
{
    // ------------------------------------------------------------

    auto &peripheral_table = _table_holder.peripheral_tables[ix_perip_used()];

    auto &population_table = _table_holder.main_table;

    // ------------------------------------------------------------------------

    assert(
        peripheral_table.time_stamps().nrows() ==
        peripheral_table.categorical().nrows() );

    assert(
        peripheral_table.time_stamps().nrows() ==
        peripheral_table.discrete().nrows() );

    assert(
        peripheral_table.time_stamps().nrows() ==
        peripheral_table.numerical().nrows() );

    // ------------------------------------------------------------
    // Create subfeatures, if necessary

    if ( subtrees().size() > 0 )
        {
            assert( _table_holder.subtables[ix_perip_used()] );

            auto &subtable = *_table_holder.subtables[ix_perip_used()];

            auto population_indices =
                SampleContainer::create_population_indices(
                    subtable.main_table.df().nrows(), _sample_container );

            subtable.main_table.set_indices( population_indices );

            transform_subtrees(
                *_table_holder.subtables[ix_perip_used()], true );
        }

    // ------------------------------------------------------------------------

    const AUTOSQL_INT ix_column_used = column_to_be_aggregated().ix_column_used;

    switch ( column_to_be_aggregated().data_used )
        {
            case DataUsed::x_perip_numerical:

                aggregation()->set_value_to_be_aggregated(
                    peripheral_table.numerical(), ix_column_used );

                break;

            case DataUsed::x_perip_discrete:

                aggregation()->set_value_to_be_aggregated(
                    peripheral_table.discrete(), ix_column_used );

                break;

            case DataUsed::time_stamps_diff:

                aggregation()->set_value_to_be_aggregated(
                    peripheral_table.time_stamps(), 0 );

                aggregation()->set_value_to_be_compared(
                    population_table.time_stamps_column( ix_perip_used() ) );

                break;

            case DataUsed::same_unit_numerical:

                assert(
                    static_cast<AUTOSQL_INT>(
                        impl()->same_units_numerical().size() ) >
                    ix_column_used );

                {
                    const DataUsed data_used1 =
                        std::get<0>(
                            impl()->same_units_numerical()[ix_column_used] )
                            .data_used;

                    const DataUsed data_used2 =
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

                    if ( data_used1 == DataUsed::x_perip_numerical )
                        {
                            assert(
                                peripheral_table.numerical().ncols() >
                                ix_column_used1 );

                            aggregation()->set_value_to_be_aggregated(
                                peripheral_table.numerical(), ix_column_used1 );
                        }
                    else
                        {
                            assert( !"Unknown data_used1 in set_value_to_be_aggregated(...)!" );
                        }

                    if ( data_used2 == DataUsed::x_popul_numerical )
                        {
                            assert(
                                population_table.df().numerical().ncols() >
                                ix_column_used2 );

                            aggregation()->set_value_to_be_compared(
                                population_table.numerical_column(
                                    ix_column_used2 ) );
                        }
                    else if ( data_used2 == DataUsed::x_perip_numerical )
                        {
                            assert(
                                peripheral_table.numerical().ncols() >
                                ix_column_used2 );

                            auto view = containers::ColumnView<
                                AUTOSQL_FLOAT,
                                std::vector<AUTOSQL_INT>>(
                                peripheral_table.numerical(), ix_column_used2 );

                            aggregation()->set_value_to_be_compared( view );
                        }
                    else
                        {
                            assert( !"Unknown data_used2 in set_value_to_be_compared(...)!" );
                        }
                }

                break;

            case DataUsed::same_unit_discrete:

                assert(
                    static_cast<AUTOSQL_INT>(
                        impl()->same_units_discrete().size() ) >
                    ix_column_used );

                {
                    const DataUsed data_used1 =
                        std::get<0>(
                            impl()->same_units_discrete()[ix_column_used] )
                            .data_used;

                    const DataUsed data_used2 =
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

                    if ( data_used1 == DataUsed::x_perip_discrete )
                        {
                            assert(
                                peripheral_table.discrete().ncols() >
                                ix_column_used1 );

                            aggregation()->set_value_to_be_aggregated(
                                peripheral_table.discrete(), ix_column_used1 );
                        }
                    else
                        {
                            assert( !"Unknown data_used1 in set_value_to_be_aggregated(...)!" );
                        }

                    if ( data_used2 == DataUsed::x_popul_discrete )
                        {
                            assert(
                                population_table.df().discrete().ncols() >
                                ix_column_used2 );

                            aggregation()->set_value_to_be_compared(
                                population_table.discrete_column(
                                    ix_column_used2 ) );
                        }
                    else if ( data_used2 == DataUsed::x_perip_discrete )
                        {
                            assert(
                                peripheral_table.discrete().ncols() >
                                ix_column_used2 );

                            auto view = containers::ColumnView<
                                AUTOSQL_FLOAT,
                                std::vector<AUTOSQL_INT>>(
                                peripheral_table.discrete(), ix_column_used2 );

                            aggregation()->set_value_to_be_compared( view );
                        }
                    else
                        {
                            assert( !"Unknown data_used2 in set_value_to_be_compared(...)!" );
                        }
                }

                break;

            case DataUsed::x_perip_categorical:

                aggregation()->set_value_to_be_aggregated(
                    peripheral_table.categorical(), ix_column_used );

                break;

            case DataUsed::x_subfeature:

                assert( subtrees().size() > 0 );

                aggregation()->set_value_to_be_aggregated(
                    impl()->subfeatures().column_view( ix_column_used ) );

                break;

            case DataUsed::not_applicable:

                break;

            default:

                assert( !"Unknown DataUsed in column_to_be_aggregated(...)!" );
        }

    // ---------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTree::source_importances(
    descriptors::SourceImportances &_importances )
{
    // ----------------------------------------------------------------
    // Calculate aggregation importances

    impl_.source_importances(
        column_to_be_aggregated().data_used,
        column_to_be_aggregated().ix_column_used,
        1.0,
        _importances.aggregation_imp_ );

    // ----------------------------------------------------------------
    // Calculate the condition importances

    root()->source_importances( 1.0, _importances );

    // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTree::fit(
    AUTOSQL_SAMPLE_CONTAINER::iterator _sample_container_begin,
    AUTOSQL_SAMPLE_CONTAINER::iterator _sample_container_end,
    TableHolder &_table_holder,
    optimizationcriteria::OptimizationCriterion *_optimization_criterion,
    bool _allow_sets,
    AUTOSQL_INT _max_length,
    AUTOSQL_INT _min_num_samples,
    AUTOSQL_FLOAT _grid_factor,
    AUTOSQL_FLOAT _regularization,
    AUTOSQL_FLOAT _share_conditions,
    bool _use_timestamps )
{
    // ------------------------------------------------------------

    auto &peripheral_table = _table_holder.peripheral_tables[ix_perip_used()];

    auto &population_table = _table_holder.main_table;

    // ------------------------------------------------------------

    assert(
        peripheral_table.categorical().nrows() ==
        peripheral_table.numerical().nrows() );

    assert(
        peripheral_table.categorical().nrows() ==
        peripheral_table.discrete().nrows() );

    assert(
        peripheral_table.categorical().nrows() ==
        peripheral_table.time_stamps().nrows() );

    assert( peripheral_table.time_stamps().ncols() == 1 );

    // ------------------------------------------------------------

    assert(
        population_table.df().categorical().nrows() ==
        population_table.df().numerical().nrows() );

    assert(
        population_table.df().categorical().nrows() ==
        population_table.df().discrete().nrows() );

    assert(
        population_table.df().categorical().nrows() ==
        population_table.df().time_stamps( ix_perip_used() ).nrows() );

    assert( population_table.df().time_stamps( ix_perip_used() ).ncols() == 1 );

    // ------------------------------------------------------------

    peripheral() = peripheral_table;
    population() = population_table;

    // ------------------------------------------------------------

    peripheral_name() = peripheral_table.name();
    population_name() = population_table.df().name();

    // ------------------------------------------------------------

    join_keys_perip_name() = peripheral().join_key().colname( 0 );

    join_keys_popul_name() =
        population().df().join_key( ix_perip_used() ).colname( 0 );

    time_stamps_perip_name() = peripheral().time_stamps().colname( 0 );

    time_stamps_popul_name() =
        population().df().time_stamps( ix_perip_used() ).colname( 0 );

    if ( peripheral().upper_time_stamps() != nullptr )
        {
            upper_time_stamps_name() =
                peripheral().upper_time_stamps()->colname( 0 );
        }

    impl_.x_perip_categorical_colnames_ = peripheral().categorical().colnames();
    impl_.x_perip_numerical_colnames_ = peripheral().numerical().colnames();
    impl_.x_perip_discrete_colnames_ = peripheral().discrete().colnames();

    impl_.x_popul_categorical_colnames_ =
        population().df().categorical().colnames();
    impl_.x_popul_numerical_colnames_ =
        population().df().numerical().colnames();
    impl_.x_popul_discrete_colnames_ = population().df().discrete().colnames();

    allow_sets() = _allow_sets;
    max_length() = _max_length;
    min_num_samples() = _min_num_samples;
    grid_factor() = _grid_factor;
    regularization() = _regularization;
    share_conditions() = _share_conditions;

    // ------------------------------------------------------------
    // Prepare the root, the aggregation and the optimization criterion

    debug_message( "fit: Preparing new candidate..." );

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

    debug_message( "fit: Trying conditions..." );

    root()->fit_as_root( _sample_container_begin, _sample_container_end );

    // ------------------------------------------------------------
    // Clean up

    impl()->clear();

    // ------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTree::from_json_obj( const Poco::JSON::Object &_json_obj )
{
    // -----------------------------------

    peripheral_name() =
        _json_obj.AUTOSQL_GET_VALUE<std::string>( "peripheral_name_" );

    population_name() =
        _json_obj.AUTOSQL_GET_VALUE<std::string>( "population_name_" );

    join_keys_perip_name() =
        _json_obj.AUTOSQL_GET_VALUE<std::string>( "join_keys_perip_name_" );

    join_keys_popul_name() =
        _json_obj.AUTOSQL_GET_VALUE<std::string>( "join_keys_popul_name_" );

    time_stamps_perip_name() =
        _json_obj.AUTOSQL_GET_VALUE<std::string>( "time_stamps_perip_name_" );

    time_stamps_popul_name() =
        _json_obj.AUTOSQL_GET_VALUE<std::string>( "time_stamps_popul_name_" );

    if ( _json_obj.has( "upper_time_stamps_" ) )
        {
            upper_time_stamps_name() =
                _json_obj.AUTOSQL_GET_VALUE<std::string>( "upper_time_stamps_" );
        }
    else
        {
            upper_time_stamps_name() = "";
        }

    // -----------------------------------

    impl_.x_perip_categorical_colnames_.reset( new std::vector<std::string>() );

    x_perip_categorical_colnames() = JSON::array_to_vector<std::string>(
        _json_obj.AUTOSQL_GET_ARRAY( "x_perip_categorical_colnames_" ) );

    // --

    impl_.x_perip_numerical_colnames_.reset( new std::vector<std::string>() );

    x_perip_numerical_colnames() = JSON::array_to_vector<std::string>(
        _json_obj.AUTOSQL_GET_ARRAY( "x_perip_numerical_colnames_" ) );

    // --

    impl_.x_perip_discrete_colnames_.reset( new std::vector<std::string>() );

    x_perip_discrete_colnames() = JSON::array_to_vector<std::string>(
        _json_obj.AUTOSQL_GET_ARRAY( "x_perip_discrete_colnames_" ) );
    // --

    impl_.x_popul_categorical_colnames_.reset( new std::vector<std::string>() );

    x_popul_categorical_colnames() = JSON::array_to_vector<std::string>(
        _json_obj.AUTOSQL_GET_ARRAY( "x_popul_categorical_colnames_" ) );

    // --

    impl_.x_popul_numerical_colnames_.reset( new std::vector<std::string>() );

    x_popul_numerical_colnames() = JSON::array_to_vector<std::string>(
        _json_obj.AUTOSQL_GET_ARRAY( "x_popul_numerical_colnames_" ) );

    // --

    impl_.x_popul_discrete_colnames_.reset( new std::vector<std::string>() );

    x_popul_discrete_colnames() = JSON::array_to_vector<std::string>(
        _json_obj.AUTOSQL_GET_ARRAY( "x_popul_discrete_colnames_" ) );

    // -----------------------------------

    column_to_be_aggregated().ix_column_used =
        _json_obj.AUTOSQL_GET( "column_" );

    column_to_be_aggregated().data_used =
        JSON::int_to_data_used( _json_obj.AUTOSQL_GET( "data_" ) );

    column_to_be_aggregated().ix_perip_used = _json_obj.AUTOSQL_GET( "input_" );

    root().reset( new DecisionTreeNode(
        true,   // _is_activated
        1,      // _depth
        impl()  // _tree
        ) );

    root()->from_json_obj( *_json_obj.AUTOSQL_GET_OBJECT( "conditions_" ) );

    // -----------------------------------

    impl()->same_units_.same_units_categorical =
        std::make_shared<AUTOSQL_SAME_UNITS_CONTAINER>(
            JSON::json_arr_to_same_units(
                *_json_obj.AUTOSQL_GET_ARRAY( "same_units_categorical_" ) ) );

    impl()->same_units_.same_units_discrete =
        std::make_shared<AUTOSQL_SAME_UNITS_CONTAINER>(
            JSON::json_arr_to_same_units(
                *_json_obj.AUTOSQL_GET_ARRAY( "same_units_discrete_" ) ) );

    impl()->same_units_.same_units_numerical =
        std::make_shared<AUTOSQL_SAME_UNITS_CONTAINER>(
            JSON::json_arr_to_same_units(
                *_json_obj.AUTOSQL_GET_ARRAY( "same_units_numerical_" ) ) );

    // -----------------------------------

    std::string agg = _json_obj.AUTOSQL_GET( "aggregation_" );

    assert( agg != "" );

    aggregation_ptr() = parse_aggregation( agg );

    impl_.aggregation_type_ = agg;

    // -----------------------------------

    const auto subtrees_arr = _json_obj.AUTOSQL_GET_ARRAY( "subfeatures_" );

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
    debug_message( "Feature: Copy assignment constructor..." );

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
    debug_message( "Feature: Move assignment constructor..." );

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

std::shared_ptr<aggregations::AbstractAggregation> DecisionTree::parse_aggregation(
    const std::string &_aggregation )
{
    if ( _aggregation == "AVG" )
        {
            return make_aggregation<aggregations::AggregationType::Avg>();
        }

    else if ( _aggregation == "COUNT" )
        {
            return make_aggregation<aggregations::AggregationType::Count>();
        }

    else if ( _aggregation == "COUNT DISTINCT" )
        {
            return make_aggregation<
                aggregations::AggregationType::CountDistinct>();
        }

    else if ( _aggregation == "COUNT MINUS COUNT DISTINCT" )
        {
            return make_aggregation<
                aggregations::AggregationType::CountMinusCountDistinct>();
        }

    else if ( _aggregation == "MAX" )
        {
            return make_aggregation<aggregations::AggregationType::Max>();
        }

    else if ( _aggregation == "MEDIAN" )
        {
            return make_aggregation<aggregations::AggregationType::Median>();
        }

    else if ( _aggregation == "MIN" )
        {
            return make_aggregation<aggregations::AggregationType::Min>();
        }

    else if ( _aggregation == "SKEWNESS" )
        {
            return make_aggregation<aggregations::AggregationType::Skewness>();
        }

    else if ( _aggregation == "STDDEV" )
        {
            return make_aggregation<aggregations::AggregationType::Stddev>();
        }

    else if ( _aggregation == "SUM" )
        {
            return make_aggregation<aggregations::AggregationType::Sum>();
        }

    else if ( _aggregation == "VAR" )
        {
            return make_aggregation<aggregations::AggregationType::Var>();
        }

    else
        {
            std::string warning_message = "Aggregation of type '";
            warning_message.append( _aggregation );
            warning_message.append( "' not known!" );

            throw std::invalid_argument( warning_message );
        }
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

    obj.set( "peripheral_name_", peripheral_name() );
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

    obj.set( "x_popul_discrete_colnames_", x_popul_discrete_colnames() );

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

    obj.set( "join_keys_popul_", join_keys_popul_name() );

    obj.set( "time_stamps_popul_", time_stamps_popul_name() );

    obj.set( "join_keys_perip_", join_keys_perip_name() );

    obj.set( "time_stamps_perip_", time_stamps_perip_name() );

    if ( upper_time_stamps_name() != "" )
        {
            obj.set( "upper_time_stamps_", upper_time_stamps_name() );
        }

    obj.set( "population_", population_name() );

    obj.set( "peripheral_", peripheral_name() );

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

    sql << "       t1." << join_keys_popul_name() << "," << std::endl;

    sql << "       t1." << time_stamps_popul_name() << std::endl;

    // -------------------------------------------------------------------

    sql << "FROM (" << std::endl;

    sql << "     SELECT *," << std::endl;

    sql << "            ROW_NUMBER() OVER ( ORDER BY " << join_keys_popul_name()
        << ", " << time_stamps_popul_name() << " ASC ) AS rownum" << std::endl;

    sql << "     FROM " << population_name() << std::endl;

    sql << ") t1" << std::endl;

    sql << "LEFT JOIN " << peripheral_name() << " t2" << std::endl;

    sql << "ON t1." << join_keys_popul_name() << " = t2."
        << join_keys_perip_name() << std::endl;

    // -------------------------------------------------------------------

    std::vector<std::string> conditions;

    root()->to_sql( _feature_num, conditions, "" );

    for ( AUTOSQL_SIZE i = 0; i < conditions.size(); ++i )
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

            sql << "t2." << time_stamps_perip_name() << " <= t1."
                << time_stamps_popul_name() << std::endl;

            if ( upper_time_stamps_name() != "" )
                {
                    sql << "AND ( t2." << upper_time_stamps_name() << " > t1."
                        << time_stamps_popul_name() << " OR t2."
                        << upper_time_stamps_name() << " IS NULL )"
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

    sql << "         t1." << join_keys_popul_name() << "," << std::endl;

    sql << "         t1." << time_stamps_popul_name() << ";" << std::endl
        << std::endl
        << std::endl;

    // -------------------------------------------------------------------

    return sql.str();
}

// ----------------------------------------------------------------------------

containers::Matrix<AUTOSQL_FLOAT> DecisionTree::transform(
    TableHolder &_table_holder, bool _use_timestamps )
{
    // ------------------------------------------------------------

    auto &peripheral_table = _table_holder.peripheral_tables[ix_perip_used()];

    auto &population_table = _table_holder.main_table;

    // ------------------------------------------------------------

    assert(
        peripheral_table.categorical().nrows() ==
        peripheral_table.numerical().nrows() );

    assert(
        peripheral_table.categorical().nrows() ==
        peripheral_table.discrete().nrows() );

    assert(
        peripheral_table.categorical().nrows() ==
        peripheral_table.time_stamps().nrows() );

    assert( peripheral_table.time_stamps().ncols() == 1 );

    // ------------------------------------------------------------

    assert(
        population_table.df().categorical().nrows() ==
        population_table.df().numerical().nrows() );

    assert(
        population_table.df().categorical().nrows() ==
        population_table.df().discrete().nrows() );

    assert(
        population_table.df().categorical().nrows() ==
        population_table.df().time_stamps( ix_perip_used() ).nrows() );

    assert( population_table.df().time_stamps( ix_perip_used() ).ncols() == 1 );

    // ------------------------------------------------------------

    peripheral() = peripheral_table;

    population() = population_table;

    // ------------------------------------------------------
    // Prepare the aggregation

    aggregation()->reset();

    // ------------------------------------------------------
    // This is put in a loop to avoid the sample containers
    // taking up too much memory

    for ( AUTOSQL_INT ix_x_popul = 0; ix_x_popul < population().nrows();
          ++ix_x_popul )
        {
            // ------------------------------------------------------
            // Create samples and sample containers

            debug_message( "transform: Create sample containers..." );

            AUTOSQL_SAMPLES samples;

            AUTOSQL_SAMPLE_CONTAINER sample_container;

            SampleContainer::create_samples(
                ix_x_popul,
                _use_timestamps,
                *peripheral().index(),
                peripheral().join_key(),
                population().join_key( ix_x_popul, ix_perip_used() ),
                peripheral().time_stamps(),
                peripheral().upper_time_stamps(),
                population().time_stamp( ix_x_popul, ix_perip_used() ),
                samples );

            SampleContainer::create_sample_container(
                samples, sample_container );

            create_value_to_be_aggregated( _table_holder, sample_container );

            // ------------------------------------------------------
            // Separate null values, tell the aggregation where the samples
            // begin and end and sort the samples, if necessary

            debug_message( "transform: Set begin, end..." );

            auto null_values_dist =
                std::distance( samples.begin(), samples.begin() );

            if ( aggregation_type() != "COUNT" )
                {
                    auto null_values_separator =
                        separate_null_values( samples );

                    null_values_dist =
                        std::distance( samples.begin(), null_values_separator );

                    aggregation()->set_samples_begin_end(
                        samples.data() + null_values_dist,
                        samples.data() + samples.size() );

                    if ( aggregation()->needs_sorting() )
                        {
                            sort_samples(
                                null_values_separator, samples.end() );
                        }

                    // Because keep on generating samples and sample_container,
                    // we do not have to explicitly sort the sample containers!
                }
            else
                {
                    aggregation()->set_samples_begin_end(
                        samples.data(), samples.data() + samples.size() );
                }

            // ------------------------------------------------------
            // Do the actual transformation

            debug_message( "transform: Activate..." );

            aggregation()->activate_all(
                false,
                sample_container.begin() + null_values_dist,
                sample_container.end() );

            debug_message( "transform: Do actual transformation..." );

            root()->transform(
                sample_container.begin() + null_values_dist,
                sample_container.end() );

            // ------------------------------------------------------
            // Some aggregations, such as min and max contain additional
            // containers. If we do not clear them, they will use up too much
            // memory. For other aggregations, this does nothing at all.

            debug_message( "transform: Clear extras..." );

            aggregation()->clear_extras();
        }

    auto yhat = aggregation()->yhat();

    // ------------------------------------------------------
    // Clean up

    impl()->clear();

    // ------------------------------------------------------

    return yhat;
}

// ----------------------------------------------------------------------------

void DecisionTree::transform_subtrees(
    TableHolder &_table_holder, bool _use_timestamps )
{
    containers::Optional<aggregations::AggregationImpl> aggregation_impl(
        new aggregations::AggregationImpl( _table_holder.main_table.nrows() ) );

    for ( auto &tree : subtrees() )
        {
            tree.set_aggregation_impl( aggregation_impl );
        }

    auto subfeatures =
        containers::Matrix<AUTOSQL_FLOAT>( 0, _table_holder.main_table.nrows() );

    for ( auto &tree : subtrees() )
        {
            auto new_feature =
                tree.transform( _table_holder, _use_timestamps ).transpose();

            subfeatures.append( new_feature );
        }

    auto output_map = SampleContainer::create_output_map(
        _table_holder.main_table.get_indices() );

    impl()->subfeatures() =
        containers::MatrixView<AUTOSQL_FLOAT, std::map<AUTOSQL_INT, AUTOSQL_INT>>(
            subfeatures.transpose(), output_map );
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace autosql
