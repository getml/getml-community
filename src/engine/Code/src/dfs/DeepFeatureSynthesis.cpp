#include "dfs/algorithm/algorithm.hpp"

namespace dfs
{
namespace algorithm
{
// ----------------------------------------------------------------------------

DeepFeatureSynthesis::DeepFeatureSynthesis(
    const std::shared_ptr<const Hyperparameters> &_hyperparameters,
    const std::shared_ptr<const std::vector<std::string>> &_peripheral,
    const std::shared_ptr<const containers::Placeholder> &_placeholder,
    const std::shared_ptr<const std::vector<containers::Placeholder>>
        &_peripheral_schema,
    const std::shared_ptr<const containers::Placeholder> &_population_schema )
    : allow_http_( false ),
      comm_( nullptr ),
      hyperparameters_( _hyperparameters ),
      peripheral_( _peripheral ),
      peripheral_schema_( _peripheral_schema ),
      placeholder_( _placeholder ),
      population_schema_( _population_schema )

{
    if ( placeholder_ )
        {
            placeholder().check_data_model( peripheral(), true );
        }
}

// ----------------------------------------------------------------------------

DeepFeatureSynthesis::DeepFeatureSynthesis( const Poco::JSON::Object &_obj )
    : hyperparameters_( std::make_shared<const Hyperparameters>(
          *jsonutils::JSON::get_object( _obj, "hyperparameters_" ) ) ),
      peripheral_(
          _obj.has( "peripheral_" )
              ? std::make_shared<const std::vector<std::string>>(
                    jsonutils::JSON::array_to_vector<std::string>(
                        jsonutils::JSON::get_array( _obj, "peripheral_" ) ) )
              : nullptr ),
      placeholder_(
          _obj.has( "placeholder_" )
              ? std::make_shared<const containers::Placeholder>(
                    *jsonutils::JSON::get_object( _obj, "placeholder_" ) )
              : nullptr )
{
    // ------------------------------------------------------------------------

    if ( _obj.has( "population_schema_" ) )
        {
            population_schema_.reset( new containers::Placeholder(
                *jsonutils::JSON::get_object( _obj, "population_schema_" ) ) );
        }

    // ------------------------------------------------------------------------

    if ( _obj.has( "peripheral_schema_" ) )
        {
            auto vec =
                jsonutils::JSON::get_type_vector<containers::Placeholder>(
                    _obj, "peripheral_schema_" );

            peripheral_schema_ =
                std::make_shared<std::vector<containers::Placeholder>>( vec );
        }

    // ------------------------------------------------------------------------

    if ( _obj.has( "main_table_schemas_" ) )
        {
            auto vec =
                jsonutils::JSON::get_type_vector<containers::Placeholder>(
                    _obj, "main_table_schemas_" );

            main_table_schemas_ =
                std::make_shared<std::vector<containers::Placeholder>>( vec );
        }

    // ------------------------------------------------------------------------

    if ( _obj.has( "peripheral_table_schemas_" ) )
        {
            auto vec =
                jsonutils::JSON::get_type_vector<containers::Placeholder>(
                    _obj, "peripheral_table_schemas_" );

            peripheral_table_schemas_ =
                std::make_shared<std::vector<containers::Placeholder>>( vec );
        }

    // ------------------------------------------------------------------------

    allow_http() = _obj.has( "allow_http_" )
                       ? jsonutils::JSON::get_value<bool>( _obj, "allow_http_" )
                       : false;

    // ------------------------------------------------------------------------

    if ( _obj.has( "features_" ) )
        {
            auto vec =
                jsonutils::JSON::get_type_vector<containers::AbstractFeature>(
                    _obj, "features_" );

            abstract_features_ =
                std::make_shared<std::vector<containers::AbstractFeature>>(
                    vec );
        }

    // ------------------------------------------------------------------------

    if ( _obj.has( "subfeatures_" ) )
        {
            auto subfeatures_arr =
                jsonutils::JSON::get_array( _obj, "subfeatures_" );

            auto subfeatures = std::make_shared<
                std::vector<std::optional<DeepFeatureSynthesis>>>(
                subfeatures_arr->size() );

            for ( size_t i = 0; i < subfeatures_arr->size(); ++i )
                {
                    auto obj = subfeatures_arr->getObject(
                        static_cast<unsigned int>( i ) );

                    if ( obj )
                        {
                            subfeatures->at( i ) = DeepFeatureSynthesis( *obj );
                        }
                }

            subfeatures_ = subfeatures;
        }

    // ------------------------------------------------------------------------

    if ( placeholder_ )
        {
            placeholder().check_data_model( peripheral(), true );
        }

    // ------------------------------------------------------------------------
}

// ---------------------------------------------------------------------------

void DeepFeatureSynthesis::build_row(
    const TableHolder &_table_holder,
    const std::vector<containers::Features> &_subfeatures,
    const std::vector<size_t> &_index,
    const std::vector<std::function<bool( const containers::Match & )>>
        &_condition_functions,
    const size_t _rownum,
    containers::Features *_features ) const
{
    assert_true( _condition_functions.size() == _index.size() );

    assert_true( _features->size() == _index.size() );

    const auto all_matches = make_matches( _table_holder, _rownum );

    assert_true(
        all_matches.size() == _table_holder.peripheral_tables_.size() );

    assert_true(
        _table_holder.main_tables_.size() ==
        _table_holder.peripheral_tables_.size() );

    assert_true(
        _subfeatures.size() == _table_holder.peripheral_tables_.size() );

    for ( size_t i = 0; i < _index.size(); ++i )
        {
            const auto ix = _index.at( i );

            assert_true( ix < abstract_features().size() );

            const auto &abstract_feature = abstract_features().at( ix );

            assert_true(
                abstract_feature.peripheral_ <
                _table_holder.peripheral_tables_.size() );

            const auto population =
                _table_holder.main_tables_.at( abstract_feature.peripheral_ )
                    .df();

            const auto peripheral = _table_holder.peripheral_tables_.at(
                abstract_feature.peripheral_ );

            const auto subf = _subfeatures.at( abstract_feature.peripheral_ );

            const auto &matches =
                all_matches.at( abstract_feature.peripheral_ );

            assert_true( _features->at( i ) );

            assert_true( _rownum < _features->at( i )->size() );

            const auto &condition_function = _condition_functions.at( i );

            const auto value = Aggregator::apply_aggregation(
                population,
                peripheral,
                subf,
                matches,
                condition_function,
                abstract_feature );

            _features->at( i )->at( _rownum ) =
                ( std::isnan( value ) || std::isinf( value ) ) ? 0.0 : value;
        }
}

// ----------------------------------------------------------------------------

void DeepFeatureSynthesis::build_rows(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::vector<containers::Features> &_subfeatures,
    const std::vector<size_t> &_index,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const size_t _thread_num,
    std::atomic<size_t> *_num_completed,
    containers::Features *_features ) const
{
    const auto rownums = make_rownums( _thread_num, _population.nrows() );

    const auto population_view =
        containers::DataFrameView( _population, rownums );

    const auto table_holder = TableHolder(
        placeholder(), population_view, _peripheral, peripheral() );

    constexpr size_t log_iter = 5000;

    const auto condition_functions = ConditionParser::make_condition_functions(
        table_holder, _subfeatures, _index, abstract_features() );

    for ( size_t i = 0; i < rownums->size(); ++i )
        {
            build_row(
                table_holder,
                _subfeatures,
                _index,
                condition_functions,
                rownums->at( i ),
                _features );

            if ( i % log_iter == 0 && i != 0 )
                {
                    _num_completed->fetch_add( log_iter );

                    if ( _thread_num == 0 )
                        {
                            log_progress(
                                _logger,
                                _population.nrows(),
                                _num_completed->load() );
                        }
                }
        }

    _num_completed->fetch_add( rownums->size() % log_iter );
}

// ----------------------------------------------------------------------------

std::vector<containers::Features> DeepFeatureSynthesis::build_subfeatures(
    const std::vector<containers::DataFrame> &_peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger ) const
{
    assert_true( placeholder().joined_tables_.size() == subfeatures().size() );

    std::vector<containers::Features> features;

    for ( size_t i = 0; i < subfeatures().size(); ++i )
        {
            if ( !subfeatures().at( i ) )
                {
                    features.push_back( containers::Features() );
                    continue;
                }

            const auto joined_table = placeholder().joined_tables_.at( i );

            const auto population =
                find_peripheral( _peripheral, joined_table.name_ );

            const auto f = subfeatures().at( i )->transform(
                population, _peripheral, std::nullopt, _logger );

            features.push_back( f );
        }

    return features;
}

// ----------------------------------------------------------------------------

std::map<helpers::ColumnDescription, Float>
DeepFeatureSynthesis::column_importances(
    const std::vector<Float> &_importance_factors ) const
{
    auto importances = helpers::ImportanceMaker();

    auto subimportance_factors = init_subimportance_factors();

    for ( size_t i = 0; i < _importance_factors.size(); ++i )
        {
            const auto pairs = infer_importance(
                i, _importance_factors.at( i ), &subimportance_factors );

            for ( const auto &p : pairs )
                {
                    importances.add_to_importances( p.first, p.second );
                }
        }

    for ( size_t i = 0; i < subfeatures().size(); ++i )
        {
            if ( subfeatures().at( i ) )
                {
                    const auto subimportances =
                        subfeatures().at( i )->column_importances(
                            subimportance_factors.at( i ) );

                    for ( const auto &p : subimportances )
                        {
                            importances.add_to_importances( p.first, p.second );
                        }
                }
        }

    return importances.importances();
}

// ----------------------------------------------------------------------------

void DeepFeatureSynthesis::extract_schemas(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral )
{
    population_schema_ = std::make_shared<const containers::Placeholder>(
        _population.to_schema() );

    auto peripheral_schema =
        std::make_shared<std::vector<containers::Placeholder>>();

    for ( auto &df : _peripheral )
        {
            peripheral_schema->push_back( df.to_schema() );
        }

    peripheral_schema_ = peripheral_schema;
}

// ----------------------------------------------------------------------------

void DeepFeatureSynthesis::extract_schemas( const TableHolder &_table_holder )
{
    assert_true(
        _table_holder.main_tables_.size() ==
        _table_holder.peripheral_tables_.size() );

    const auto main_table_schemas =
        std::make_shared<std::vector<containers::Placeholder>>();

    const auto peripheral_table_schemas =
        std::make_shared<std::vector<containers::Placeholder>>();

    for ( size_t i = 0; i < _table_holder.main_tables_.size(); ++i )
        {
            main_table_schemas->push_back(
                _table_holder.main_tables_.at( i ).df().to_schema() );

            peripheral_table_schemas->push_back(
                _table_holder.peripheral_tables_.at( i ).to_schema() );
        }

    main_table_schemas_ = main_table_schemas;

    peripheral_table_schemas_ = peripheral_table_schemas;
}

// ----------------------------------------------------------------------------

containers::DataFrame DeepFeatureSynthesis::find_peripheral(
    const std::vector<containers::DataFrame> &_peripheral,
    const std::string &_name ) const
{
    if ( _peripheral.size() != peripheral().size() )
        {
            throw std::invalid_argument(
                "The number of peripheral tables does not match the number of "
                "peripheral placeholders." );
        }

    for ( size_t i = 0; i < peripheral().size(); ++i )
        {
            if ( peripheral().at( i ) == _name )
                {
                    return _peripheral.at( i );
                }
        }

    throw std::invalid_argument(
        "Placeholder named '" + _name + "' not found." );

    return _peripheral.at( 0 );
}

// ----------------------------------------------------------------------------

void DeepFeatureSynthesis::fit(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger )
{
    extract_schemas( _population, _peripheral );

    subfeatures_ = fit_subfeatures( _peripheral, _logger );

    if ( _logger )
        {
            _logger->log( "DeepFeatureSynthesis: Training features..." );
        }

    const auto abstract_features =
        std::make_shared<std::vector<containers::AbstractFeature>>();

    const auto rownums =
        std::make_shared<std::vector<size_t>>( _population.nrows() );

    std::iota( rownums->begin(), rownums->end(), 0 );

    const auto population_view =
        containers::DataFrameView( _population, rownums );

    const auto table_holder = TableHolder(
        placeholder(), population_view, _peripheral, peripheral() );

    assert_true(
        table_holder.main_tables_.size() ==
        table_holder.peripheral_tables_.size() );

    extract_schemas( table_holder );

    const auto conditions = make_conditions( table_holder );

    for ( size_t i = 0; i < table_holder.main_tables_.size(); ++i )
        {
            fit_on_peripheral(
                table_holder.main_tables_.at( i ).df(),
                table_holder.peripheral_tables_.at( i ),
                i,
                conditions,
                abstract_features );
        }

    abstract_features_ = abstract_features;

    if ( _logger )
        {
            _logger->log( "Trained features. Progress: 100\%." );
        }
}

// ----------------------------------------------------------------------------

void DeepFeatureSynthesis::fit_on_categoricals(
    const containers::DataFrame &_peripheral,
    const size_t _peripheral_ix,
    const std::vector<containers::Condition> &_conditions,
    std::shared_ptr<std::vector<containers::AbstractFeature>>
        _abstract_features ) const
{
    assert_true( _abstract_features );

    for ( size_t input_col = 0; input_col < _peripheral.num_categoricals();
          ++input_col )
        {
            for ( const auto agg : hyperparameters().aggregations_ )
                {
                    if ( !is_categorical( agg ) )
                        {
                            continue;
                        }

                    if ( _peripheral.categorical_unit( input_col )
                             .find( "comparison only" ) != std::string::npos )
                        {
                            continue;
                        }

                    _abstract_features->push_back( containers::AbstractFeature(
                        enums::Parser<enums::Aggregation>::parse( agg ),
                        _conditions,
                        enums::DataUsed::categorical,
                        input_col,
                        _peripheral_ix ) );
                }
        }
}

// ----------------------------------------------------------------------------

void DeepFeatureSynthesis::fit_on_discretes(
    const containers::DataFrame &_peripheral,
    const size_t _peripheral_ix,
    const std::vector<containers::Condition> &_conditions,
    std::shared_ptr<std::vector<containers::AbstractFeature>>
        _abstract_features ) const
{
    assert_true( _abstract_features );

    for ( size_t input_col = 0; input_col < _peripheral.num_discretes();
          ++input_col )
        {
            for ( const auto agg : hyperparameters().aggregations_ )
                {
                    if ( !is_numerical( agg ) )
                        {
                            continue;
                        }

                    if ( _peripheral.discrete_unit( input_col )
                             .find( "comparison only" ) != std::string::npos )
                        {
                            continue;
                        }

                    _abstract_features->push_back( containers::AbstractFeature(
                        enums::Parser<enums::Aggregation>::parse( agg ),
                        _conditions,
                        enums::DataUsed::discrete,
                        input_col,
                        _peripheral_ix ) );
                }
        }
}

// ----------------------------------------------------------------------------

void DeepFeatureSynthesis::fit_on_numericals(
    const containers::DataFrame &_peripheral,
    const size_t _peripheral_ix,
    const std::vector<containers::Condition> &_conditions,
    std::shared_ptr<std::vector<containers::AbstractFeature>>
        _abstract_features ) const
{
    assert_true( _abstract_features );

    for ( size_t input_col = 0; input_col < _peripheral.num_numericals();
          ++input_col )
        {
            for ( const auto agg : hyperparameters().aggregations_ )
                {
                    if ( !is_numerical( agg ) )
                        {
                            continue;
                        }

                    if ( _peripheral.numerical_unit( input_col )
                             .find( "comparison only" ) != std::string::npos )
                        {
                            continue;
                        }

                    _abstract_features->push_back( containers::AbstractFeature(
                        enums::Parser<enums::Aggregation>::parse( agg ),
                        _conditions,
                        enums::DataUsed::numerical,
                        input_col,
                        _peripheral_ix ) );
                }
        }
}

// ----------------------------------------------------------------------------

void DeepFeatureSynthesis::fit_on_same_units_categorical(
    const containers::DataFrame &_population,
    const containers::DataFrame &_peripheral,
    const size_t _peripheral_ix,
    const std::vector<containers::Condition> &_conditions,
    std::shared_ptr<std::vector<containers::AbstractFeature>>
        _abstract_features ) const
{
    assert_true( _abstract_features );

    for ( size_t output_col = 0; output_col < _population.num_categoricals();
          ++output_col )
        {
            for ( size_t input_col = 0;
                  input_col < _peripheral.num_categoricals();
                  ++input_col )
                {
                    for ( const auto agg : hyperparameters().aggregations_ )
                        {
                            const bool same_unit =
                                _population.categorical_unit( output_col ) !=
                                    "" &&
                                _population.categorical_unit( output_col ) ==
                                    _peripheral.categorical_unit( input_col );

                            if ( !same_unit )
                                {
                                    continue;
                                }

                            if ( !is_numerical( agg ) )
                                {
                                    continue;
                                }

                            _abstract_features->push_back(
                                containers::AbstractFeature(
                                    enums::Parser<enums::Aggregation>::parse(
                                        agg ),
                                    _conditions,
                                    enums::DataUsed::same_units_categorical,
                                    input_col,
                                    output_col,
                                    _peripheral_ix ) );
                        }
                }
        }
}

// ----------------------------------------------------------------------------

void DeepFeatureSynthesis::fit_on_same_units_discrete(
    const containers::DataFrame &_population,
    const containers::DataFrame &_peripheral,
    const size_t _peripheral_ix,
    const std::vector<containers::Condition> &_conditions,
    std::shared_ptr<std::vector<containers::AbstractFeature>>
        _abstract_features ) const
{
    assert_true( _abstract_features );

    for ( size_t output_col = 0; output_col < _population.num_discretes();
          ++output_col )
        {
            for ( size_t input_col = 0; input_col < _peripheral.num_discretes();
                  ++input_col )
                {
                    for ( const auto agg : hyperparameters().aggregations_ )
                        {
                            const bool same_unit =
                                _population.discrete_unit( output_col ) != "" &&
                                _population.discrete_unit( output_col ) ==
                                    _peripheral.discrete_unit( input_col );

                            if ( !same_unit )
                                {
                                    continue;
                                }

                            if ( !is_numerical( agg ) )
                                {
                                    continue;
                                }

                            const auto data_used =
                                is_ts(
                                    _population.discrete_name( output_col ),
                                    _population.discrete_unit( output_col ) )
                                    ? enums::DataUsed::same_units_discrete_ts
                                    : enums::DataUsed::same_units_discrete;

                            _abstract_features->push_back(
                                containers::AbstractFeature(
                                    enums::Parser<enums::Aggregation>::parse(
                                        agg ),
                                    _conditions,
                                    data_used,
                                    input_col,
                                    output_col,
                                    _peripheral_ix ) );
                        }
                }
        }
}

// ----------------------------------------------------------------------------

void DeepFeatureSynthesis::fit_on_same_units_numerical(
    const containers::DataFrame &_population,
    const containers::DataFrame &_peripheral,
    const size_t _peripheral_ix,
    const std::vector<containers::Condition> &_conditions,
    std::shared_ptr<std::vector<containers::AbstractFeature>>
        _abstract_features ) const
{
    assert_true( _abstract_features );

    for ( size_t output_col = 0; output_col < _population.num_numericals();
          ++output_col )
        {
            for ( size_t input_col = 0;
                  input_col < _peripheral.num_numericals();
                  ++input_col )
                {
                    for ( const auto agg : hyperparameters().aggregations_ )
                        {
                            const bool same_unit =
                                _population.numerical_unit( output_col ) !=
                                    "" &&
                                _population.numerical_unit( output_col ) ==
                                    _peripheral.numerical_unit( input_col );

                            if ( !same_unit )
                                {
                                    continue;
                                }

                            if ( !is_numerical( agg ) )
                                {
                                    continue;
                                }

                            const auto data_used =
                                is_ts(
                                    _population.numerical_name( output_col ),
                                    _population.numerical_unit( output_col ) )
                                    ? enums::DataUsed::same_units_numerical_ts
                                    : enums::DataUsed::same_units_numerical;

                            _abstract_features->push_back(
                                containers::AbstractFeature(
                                    enums::Parser<enums::Aggregation>::parse(
                                        agg ),
                                    _conditions,
                                    data_used,
                                    input_col,
                                    output_col,
                                    _peripheral_ix ) );
                        }
                }
        }
}

// ----------------------------------------------------------------------------

void DeepFeatureSynthesis::fit_on_subfeatures(
    const size_t _peripheral_ix,
    const std::vector<containers::Condition> &_conditions,
    std::shared_ptr<std::vector<containers::AbstractFeature>>
        _abstract_features ) const
{
    assert_true( _abstract_features );

    assert_true( _peripheral_ix < subfeatures().size() );

    if ( !subfeatures().at( _peripheral_ix ) )
        {
            return;
        }

    for ( size_t input_col = 0;
          input_col < subfeatures().at( _peripheral_ix )->num_features();
          ++input_col )
        {
            for ( const auto agg : hyperparameters().aggregations_ )
                {
                    if ( !is_numerical( agg ) )
                        {
                            continue;
                        }

                    _abstract_features->push_back( containers::AbstractFeature(
                        enums::Parser<enums::Aggregation>::parse( agg ),
                        _conditions,
                        enums::DataUsed::subfeatures,
                        input_col,
                        _peripheral_ix ) );
                }
        }
}

// ----------------------------------------------------------------------------

void DeepFeatureSynthesis::fit_on_peripheral(
    const containers::DataFrame &_population,
    const containers::DataFrame &_peripheral,
    const size_t _peripheral_ix,
    const std::vector<std::vector<containers::Condition>> &_conditions,
    std::shared_ptr<std::vector<containers::AbstractFeature>>
        _abstract_features ) const
{
    const auto condition_filter = make_condition_filter( _peripheral_ix );

    auto filtered_conditions =
        _conditions | std::views::filter( condition_filter );

    for ( const auto &cond : filtered_conditions )
        {
            fit_on_categoricals(
                _peripheral, _peripheral_ix, cond, _abstract_features );

            fit_on_discretes(
                _peripheral, _peripheral_ix, cond, _abstract_features );

            fit_on_numericals(
                _peripheral, _peripheral_ix, cond, _abstract_features );

            fit_on_same_units_categorical(
                _population,
                _peripheral,
                _peripheral_ix,
                cond,
                _abstract_features );

            fit_on_same_units_discrete(
                _population,
                _peripheral,
                _peripheral_ix,
                cond,
                _abstract_features );

            fit_on_same_units_numerical(
                _population,
                _peripheral,
                _peripheral_ix,
                cond,
                _abstract_features );

            fit_on_subfeatures( _peripheral_ix, cond, _abstract_features );

            if ( has_count() )
                {
                    _abstract_features->push_back( containers::AbstractFeature(
                        enums::Aggregation::count,
                        cond,
                        enums::DataUsed::not_applicable,
                        0,
                        _peripheral_ix ) );
                }
        }
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::vector<std::optional<DeepFeatureSynthesis>>>
DeepFeatureSynthesis::fit_subfeatures(
    const std::vector<containers::DataFrame> &_peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger ) const
{
    const auto subfeatures =
        std::make_shared<std::vector<std::optional<DeepFeatureSynthesis>>>();

    for ( size_t i = 0; i < placeholder().joined_tables_.size(); ++i )
        {
            const auto &joined_table = placeholder().joined_tables_.at( i );

            if ( joined_table.joined_tables_.size() == 0 )
                {
                    subfeatures->push_back( std::nullopt );
                    continue;
                }

            subfeatures->push_back( std::make_optional<DeepFeatureSynthesis>(
                hyperparameters_,
                peripheral_,
                std::make_shared<const containers::Placeholder>(
                    joined_table ) ) );

            const auto population =
                find_peripheral( _peripheral, joined_table.name_ );

            subfeatures->back()->fit( population, _peripheral, _logger );
        }

    return subfeatures;
}

// ----------------------------------------------------------------------------

size_t DeepFeatureSynthesis::get_num_threads() const
{
    const auto num_threads = hyperparameters().num_threads_;

    if ( num_threads <= 0 )
        {
            return std::max(
                static_cast<size_t>( 2 ),
                static_cast<size_t>( std::thread::hardware_concurrency() ) /
                    2 );
        }

    return static_cast<size_t>( num_threads );
}

// ----------------------------------------------------------------------------

std::vector<size_t> DeepFeatureSynthesis::infer_index(
    const std::optional<std::vector<size_t>> &_index ) const
{
    if ( _index )
        {
            return *_index;
        }

    std::vector<size_t> index;

    index = std::vector<size_t>( num_features() );

    for ( size_t i = 0; i < index.size(); ++i )
        {
            index[i] = i;
        }

    return index;
}

// ----------------------------------------------------------------------------

std::vector<std::pair<helpers::ColumnDescription, Float>>
DeepFeatureSynthesis::infer_importance(
    const size_t _feature_num,
    const Float _importance_factor,
    std::vector<std::vector<Float>> *_subimportance_factors ) const
{
    assert_true( _feature_num < abstract_features().size() );

    const auto abstract_feature = abstract_features().at( _feature_num );

    assert_true(
        _subimportance_factors->size() == peripheral_table_schemas().size() );

    assert_true(
        abstract_feature.peripheral_ < peripheral_table_schemas().size() );

    const auto &population =
        main_table_schemas().at( abstract_feature.peripheral_ );

    const auto &peripheral =
        peripheral_table_schemas().at( abstract_feature.peripheral_ );

    switch ( abstract_feature.data_used_ )
        {
            case enums::DataUsed::categorical:
                {
                    const auto col_desc = helpers::ColumnDescription(
                        helpers::ColumnDescription::PERIPHERAL,
                        peripheral.name(),
                        peripheral.categorical_name(
                            abstract_feature.input_col_ ) );

                    return { std::make_pair( col_desc, _importance_factor ) };
                }

            case enums::DataUsed::discrete:
                {
                    const auto col_desc = helpers::ColumnDescription(
                        helpers::ColumnDescription::PERIPHERAL,
                        peripheral.name(),
                        peripheral.discrete_name(
                            abstract_feature.input_col_ ) );

                    return { std::make_pair( col_desc, _importance_factor ) };
                }

            case enums::DataUsed::not_applicable:
                return std::vector<
                    std::pair<helpers::ColumnDescription, Float>>();

            case enums::DataUsed::numerical:
                {
                    const auto col_desc = helpers::ColumnDescription(
                        helpers::ColumnDescription::PERIPHERAL,
                        peripheral.name(),
                        peripheral.numerical_name(
                            abstract_feature.input_col_ ) );

                    return { std::make_pair( col_desc, _importance_factor ) };
                }

            case enums::DataUsed::same_units_categorical:
                {
                    const auto col_desc1 = helpers::ColumnDescription(
                        helpers::ColumnDescription::PERIPHERAL,
                        peripheral.name(),
                        peripheral.categorical_name(
                            abstract_feature.input_col_ ) );

                    const auto col_desc2 = helpers::ColumnDescription(
                        helpers::ColumnDescription::POPULATION,
                        population.name(),
                        population.categorical_name(
                            abstract_feature.output_col_ ) );

                    return {
                        std::make_pair( col_desc1, _importance_factor * 0.5 ),
                        std::make_pair( col_desc2, _importance_factor * 0.5 ) };
                }

            case enums::DataUsed::same_units_discrete:
            case enums::DataUsed::same_units_discrete_ts:
                {
                    const auto col_desc1 = helpers::ColumnDescription(
                        helpers::ColumnDescription::PERIPHERAL,
                        peripheral.name(),
                        peripheral.discrete_name(
                            abstract_feature.input_col_ ) );

                    const auto col_desc2 = helpers::ColumnDescription(
                        helpers::ColumnDescription::POPULATION,
                        population.name(),
                        population.discrete_name(
                            abstract_feature.output_col_ ) );

                    return {
                        std::make_pair( col_desc1, _importance_factor * 0.5 ),
                        std::make_pair( col_desc2, _importance_factor * 0.5 ) };
                }

            case enums::DataUsed::same_units_numerical:
            case enums::DataUsed::same_units_numerical_ts:
                {
                    const auto col_desc1 = helpers::ColumnDescription(
                        helpers::ColumnDescription::PERIPHERAL,
                        peripheral.name(),
                        peripheral.numerical_name(
                            abstract_feature.input_col_ ) );

                    const auto col_desc2 = helpers::ColumnDescription(
                        helpers::ColumnDescription::POPULATION,
                        population.name(),
                        population.numerical_name(
                            abstract_feature.output_col_ ) );

                    return {
                        std::make_pair( col_desc1, _importance_factor * 0.5 ),
                        std::make_pair( col_desc2, _importance_factor * 0.5 ) };
                }

            case enums::DataUsed::subfeatures:
                {
                    assert_true(
                        abstract_feature.input_col_ <
                        _subimportance_factors
                            ->at( abstract_feature.peripheral_ )
                            .size() );

                    _subimportance_factors->at( abstract_feature.peripheral_ )
                        .at( abstract_feature.input_col_ ) +=
                        _importance_factor;

                    return std::vector<
                        std::pair<helpers::ColumnDescription, Float>>();
                }

            default:
                assert_true( false && "Unknown data used" );

                const auto col_desc = helpers::ColumnDescription( "", "", "" );

                return { std::make_pair( col_desc, 0.0 ) };
        }
}

// ----------------------------------------------------------------------------

std::vector<std::vector<Float>>
DeepFeatureSynthesis::init_subimportance_factors() const
{
    const auto make_factors =
        []( const std::optional<DeepFeatureSynthesis> &sub )
        -> std::vector<Float> {
        if ( !sub )
            {
                return std::vector<Float>();
            }
        return std::vector<Float>( sub->num_features() );
    };

    const auto range = subfeatures() | std::views::transform( make_factors );

    return std::vector<std::vector<Float>>( range.begin(), range.end() );
}

// ----------------------------------------------------------------------------

bool DeepFeatureSynthesis::is_categorical( const std::string &_agg ) const
{
    const auto agg = enums::Parser<enums::Aggregation>::parse( _agg );

    switch ( agg )
        {
            case enums::Aggregation::count_distinct:
            case enums::Aggregation::count_minus_count_distinct:
                return true;

            default:
                return false;
        }
}

// ----------------------------------------------------------------------------

bool DeepFeatureSynthesis::is_numerical( const std::string &_agg ) const
{
    const auto agg = enums::Parser<enums::Aggregation>::parse( _agg );

    switch ( agg )
        {
            case enums::Aggregation::avg:
            case enums::Aggregation::max:
            case enums::Aggregation::median:
            case enums::Aggregation::min:
            case enums::Aggregation::stddev:
            case enums::Aggregation::sum:
            case enums::Aggregation::var:
                return true;

            default:
                return false;
        }
}

// ----------------------------------------------------------------------------

std::vector<std::vector<containers::Match>> DeepFeatureSynthesis::make_matches(
    const TableHolder &_table_holder, const size_t _rownum ) const
{
    // ------------------------------------------------------------

    const auto make_match = []( const size_t ix_input,
                                const size_t ix_output ) {
        return containers::Match{ ix_input, ix_output };
    };

    // ------------------------------------------------------------

    assert_true(
        _table_holder.main_tables_.size() ==
        _table_holder.peripheral_tables_.size() );

    // ------------------------------------------------------------

    auto all_matches = std::vector<std::vector<containers::Match>>();

    for ( size_t i = 0; i < _table_holder.main_tables_.size(); ++i )
        {
            const auto population = _table_holder.main_tables_.at( i ).df();

            const auto peripheral = _table_holder.peripheral_tables_.at( i );

            auto matches = std::vector<containers::Match>();

            helpers::Matchmaker<
                containers::DataFrame,
                containers::Match,
                decltype( make_match )>::
                make_matches(
                    population,
                    peripheral,
                    true,  // _use_timestamps
                    _rownum,
                    make_match,
                    &matches );

            all_matches.push_back( matches );
        }

    // ------------------------------------------------------------

    return all_matches;
}

// ----------------------------------------------------------------------------

void DeepFeatureSynthesis::save( const std::string &_fname ) const
{
    std::ofstream fs( _fname, std::ofstream::out );

    Poco::JSON::Stringifier::stringify( to_json_obj(), fs );

    fs.close();
}

// ----------------------------------------------------------------------------

containers::Features DeepFeatureSynthesis::init_features(
    const size_t _nrows, const size_t _ncols ) const
{
    auto features = containers::Features();

    for ( size_t col = 0; col < _ncols; ++col )
        {
            const auto new_feature =
                std::make_shared<std::vector<Float>>( _nrows );

            features.push_back( new_feature );
        }

    return features;
}

// ----------------------------------------------------------------------------

void DeepFeatureSynthesis::log_progress(
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const size_t _nrows,
    const size_t _num_completed ) const
{
    if ( !_logger )
        {
            return;
        }

    const auto num_completed_str = std::to_string( _num_completed );

    const auto progress = std::to_string( ( _num_completed * 100 ) / _nrows );

    _logger->log(
        "Built " + num_completed_str + " rows. Progress: " + progress + "\%." );
}

// ----------------------------------------------------------------------------

std::vector<std::vector<containers::Condition>>
DeepFeatureSynthesis::make_conditions( const TableHolder &_table_holder ) const
{
    auto conditions = std::vector<std::vector<containers::Condition>>();

    conditions.push_back( {} );

    assert_true(
        _table_holder.main_tables_.size() ==
        _table_holder.peripheral_tables_.size() );

    for ( size_t i = 0; i < _table_holder.main_tables_.size(); ++i )
        {
            const auto &population = _table_holder.main_tables_.at( i ).df();

            const auto &peripheral = _table_holder.peripheral_tables_.at( i );

            make_same_units_categorical_conditions(
                population, peripheral, i, &conditions );
        }

    return conditions;
}

// ----------------------------------------------------------------------------

void DeepFeatureSynthesis::make_same_units_categorical_conditions(
    const containers::DataFrame &_population,
    const containers::DataFrame &_peripheral,
    const size_t _peripheral_ix,
    std::vector<std::vector<containers::Condition>> *_conditions ) const
{
    for ( size_t output_col = 0; output_col < _population.num_categoricals();
          ++output_col )
        {
            for ( size_t input_col = 0;
                  input_col < _peripheral.num_categoricals();
                  ++input_col )
                {
                    const bool same_unit =
                        _population.categorical_unit( output_col ) != "" &&
                        _population.categorical_unit( output_col ) ==
                            _peripheral.categorical_unit( input_col );

                    if ( !same_unit )
                        {
                            continue;
                        }

                    _conditions->push_back( { containers::Condition(
                        enums::DataUsed::same_units_categorical,
                        input_col,
                        output_col,
                        _peripheral_ix ) } );
                }
        }
}

// ----------------------------------------------------------------------------

std::shared_ptr<std::vector<size_t>> DeepFeatureSynthesis::make_rownums(
    const size_t _thread_num, const size_t _nrows ) const
{
    const size_t num_threads = get_num_threads();

    assert_true( _thread_num < num_threads );

    const size_t rows_per_thread = _nrows / num_threads;

    const size_t begin = _thread_num * rows_per_thread;

    const size_t end = ( _thread_num < num_threads - 1 )
                           ? ( _thread_num + 1 ) * rows_per_thread
                           : _nrows;

    const auto rownums = std::make_shared<std::vector<size_t>>( end - begin );

    std::iota( rownums->begin(), rownums->end(), begin );

    return rownums;
}

// ----------------------------------------------------------------------------

void DeepFeatureSynthesis::spawn_threads(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::vector<containers::Features> &_subfeatures,
    const std::vector<size_t> &_index,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    containers::Features *_features ) const
{
    // ------------------------------------------------------------------------

    auto num_completed = std::atomic<size_t>( 0 );

    // ------------------------------------------------------------------------

    const auto execute_task = [this,
                               _population,
                               _peripheral,
                               _subfeatures,
                               _index,
                               _logger,
                               &num_completed,
                               _features]( const size_t thread_num ) {
        try
            {
                build_rows(
                    _population,
                    _peripheral,
                    _subfeatures,
                    _index,
                    _logger,
                    thread_num,
                    &num_completed,
                    _features );
            }
        catch ( std::exception &e )
            {
                if ( thread_num == 0 )
                    {
                        throw std::runtime_error( e.what() );
                    }
            }
    };

    // ------------------------------------------------------------------------

    const size_t num_threads = get_num_threads();

    std::vector<std::thread> threads;

    for ( size_t thread_num = 1; thread_num < num_threads; ++thread_num )
        {
            threads.push_back( std::thread( execute_task, thread_num ) );
        }

    // ------------------------------------------------------------------------

    try
        {
            execute_task( 0 );
        }
    catch ( std::exception &e )
        {
            for ( auto &thr : threads )
                {
                    thr.join();
                }

            throw std::runtime_error( e.what() );
        }

    // ------------------------------------------------------------------------

    for ( auto &thr : threads )
        {
            thr.join();
        }

    // ------------------------------------------------------------------------

    log_progress( _logger, _population.nrows(), num_completed.load() );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DeepFeatureSynthesis::subfeatures_to_sql(
    const std::shared_ptr<const std::vector<strings::String>> &_categories,
    const std::string &_feature_prefix,
    const size_t _offset,
    std::vector<std::string> *_sql ) const
{
    for ( size_t i = 0; i < subfeatures().size(); ++i )
        {
            if ( subfeatures().at( i ) )
                {
                    const auto sub = subfeatures().at( i )->to_sql(
                        _categories,
                        _feature_prefix + std::to_string( i + 1 ) + "_",
                        0,
                        true );

                    _sql->insert( _sql->end(), sub.begin(), sub.end() );
                }
        }
}

// ----------------------------------------------------------------------------

containers::Features DeepFeatureSynthesis::transform(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::optional<std::vector<size_t>> &_index,
    const std::shared_ptr<const logging::AbstractLogger> _logger ) const
{
    if ( _population.nrows() == 0 )
        {
            throw std::runtime_error(
                "Population table needs to contain at least some data!" );
        }

    const auto index = infer_index( _index );

    const auto subfeatures = build_subfeatures( _peripheral, _logger );

    auto features = init_features( _population.nrows(), index.size() );

    _logger->log( "DeepFeatureSynthesis: Building features..." );

    spawn_threads(
        _population, _peripheral, subfeatures, index, _logger, &features );

    return features;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DeepFeatureSynthesis::to_json_obj(
    const bool _schema_only ) const
{
    // ------------------------------------------------------------------------

    Poco::JSON::Object obj;

    // ------------------------------------------------------------------------

    obj.set( "type_", "DFSModel" );

    // ------------------------------------------------------------------------

    obj.set( "hyperparameters_", hyperparameters().to_json_obj() );

    // ------------------------------------------------------------------------

    if ( peripheral_ )
        {
            obj.set(
                "peripheral_",
                jsonutils::JSON::vector_to_array_ptr( peripheral() ) );
        }

    // ------------------------------------------------------------------------

    if ( placeholder_ )
        {
            obj.set( "placeholder_", placeholder().to_json_obj() );
        }

    // ----------------------------------------

    if ( population_schema_ )
        {
            obj.set( "population_schema_", population_schema().to_json_obj() );
        }

    // ----------------------------------------

    if ( peripheral_schema_ )
        {
            auto arr = jsonutils::JSON::vector_to_object_array_ptr(
                peripheral_schema() );

            obj.set( "peripheral_schema_", arr );
        }

    // ----------------------------------------

    if ( _schema_only )
        {
            return obj;
        }

    // ----------------------------------------

    if ( main_table_schemas_ )
        {
            auto arr = jsonutils::JSON::vector_to_object_array_ptr(
                main_table_schemas() );

            obj.set( "main_table_schemas_", arr );
        }

    // ----------------------------------------

    if ( peripheral_table_schemas_ )
        {
            auto arr = jsonutils::JSON::vector_to_object_array_ptr(
                peripheral_table_schemas() );

            obj.set( "peripheral_table_schemas_", arr );
        }

    // ----------------------------------------

    obj.set( "allow_http_", allow_http() );

    // ----------------------------------------

    if ( abstract_features_ )
        {
            Poco::JSON::Array::Ptr features_arr( new Poco::JSON::Array() );

            for ( const auto &feature : abstract_features() )
                {
                    features_arr->add( feature.to_json_obj() );
                }

            obj.set( "features_", features_arr );
        }

    // ----------------------------------------

    if ( subfeatures_ )
        {
            Poco::JSON::Array::Ptr subfeatures_arr( new Poco::JSON::Array() );

            for ( const auto &sub : subfeatures() )
                {
                    if ( sub )
                        {
                            auto obj = Poco::JSON::Object::Ptr(
                                new Poco::JSON::Object( sub->to_json_obj() ) );
                            subfeatures_arr->add( obj );
                        }
                    else
                        {
                            subfeatures_arr->add( Poco::Dynamic::Var() );
                        }
                }

            obj.set( "subfeatures_", subfeatures_arr );
        }

    // ----------------------------------------

    return obj;
}

// ----------------------------------------------------------------------------

std::vector<std::string> DeepFeatureSynthesis::to_sql(
    const std::shared_ptr<const std::vector<strings::String>> &_categories,
    const std::string &_feature_prefix,
    const size_t _offset,
    const bool _subfeatures ) const
{
    assert_true( _categories );

    assert_true(
        main_table_schemas().size() == peripheral_table_schemas().size() );

    std::vector<std::string> sql;

    if ( _subfeatures )
        {
            subfeatures_to_sql( _categories, _feature_prefix, _offset, &sql );
        }

    for ( size_t i = 0; i < abstract_features().size(); ++i )
        {
            const auto abstract_feature = abstract_features().at( i );

            assert_true(
                abstract_feature.peripheral_ <
                peripheral_table_schemas().size() );

            const auto input_schema =
                peripheral_table_schemas().at( abstract_feature.peripheral_ );

            const auto output_schema =
                main_table_schemas().at( abstract_feature.peripheral_ );

            sql.push_back( abstract_feature.to_sql(
                *_categories,
                _feature_prefix,
                std::to_string( _offset + i + 1 ),
                input_schema,
                output_schema ) );
        }

    return sql;
}

// ----------------------------------------------------------------------------
}  // namespace algorithm
}  // namespace dfs
