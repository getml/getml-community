#include "fastprop/algorithm/algorithm.hpp"

namespace fastprop
{
namespace algorithm
{
// ----------------------------------------------------------------------------

FastProp::FastProp(
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

FastProp::FastProp( const Poco::JSON::Object &_obj )
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

    if ( _obj.has( "mappings_" ) )
        {
            const auto ptr = jsonutils::JSON::get_object( _obj, "mappings_" );
            assert_true( ptr );
            mappings_ = std::make_shared<helpers::MappingContainer>( *ptr );
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

    if ( _obj.has( "vocabulary_" ) )
        {
            vocabulary_ = std::make_shared<helpers::VocabularyContainer>(
                *jsonutils::JSON::get_object( _obj, "vocabulary_" ) );
        }

    // ------------------------------------------------------------------------

    if ( _obj.has( "subfeatures_" ) )
        {
            auto subfeatures_arr =
                jsonutils::JSON::get_array( _obj, "subfeatures_" );

            auto subfeatures =
                std::make_shared<std::vector<std::optional<FastProp>>>(
                    subfeatures_arr->size() );

            for ( size_t i = 0; i < subfeatures_arr->size(); ++i )
                {
                    auto obj = subfeatures_arr->getObject(
                        static_cast<unsigned int>( i ) );

                    if ( obj )
                        {
                            subfeatures->at( i ) = FastProp( *obj );
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

void FastProp::build_row(
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
        all_matches.size() == _table_holder.peripheral_tables().size() );

    assert_true(
        _table_holder.main_tables().size() ==
        _table_holder.peripheral_tables().size() );

    assert_true(
        _subfeatures.size() <= _table_holder.peripheral_tables().size() );

    for ( size_t i = 0; i < _index.size(); ++i )
        {
            const auto ix = _index.at( i );

            assert_true( ix < abstract_features().size() );

            const auto &abstract_feature = abstract_features().at( ix );

            assert_true(
                abstract_feature.peripheral_ <
                _table_holder.peripheral_tables().size() );

            const auto &population = _table_holder.main_tables()
                                         .at( abstract_feature.peripheral_ )
                                         .df();

            const auto &peripheral = _table_holder.peripheral_tables().at(
                abstract_feature.peripheral_ );

            const auto subf =
                abstract_feature.peripheral_ < _subfeatures.size()
                    ? _subfeatures.at( abstract_feature.peripheral_ )
                    : std::optional<containers::Features>();

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

            ( *( *_features )[i] )[_rownum] =
                ( std::isnan( value ) || std::isinf( value ) ) ? 0.0 : value;
        }
}

// ----------------------------------------------------------------------------

void FastProp::build_rows(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::vector<containers::Features> &_subfeatures,
    const std::optional<helpers::WordIndexContainer> &_word_indices,
    const std::optional<const helpers::MappedContainer> &_mapped,
    const std::vector<size_t> &_index,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::shared_ptr<std::vector<size_t>> &_rownums,
    const size_t _thread_num,
    std::atomic<size_t> *_num_completed,
    containers::Features *_features ) const
{
    const auto rownums =
        make_rownums( _thread_num, _population.nrows(), _rownums );

    const auto population_view =
        containers::DataFrameView( _population, rownums );

    const auto table_holder = TableHolder(
        placeholder(),
        population_view,
        _peripheral,
        peripheral(),
        std::nullopt,
        _word_indices,
        _mapped );

    constexpr size_t log_iter = 5000;

    const auto condition_functions = ConditionParser::make_condition_functions(
        table_holder, _index, abstract_features() );

    const auto nrows = _rownums ? _rownums->size() : _population.nrows();

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
                                _logger, nrows, _num_completed->load() );
                        }
                }
        }

    _num_completed->fetch_add( nrows % log_iter );
}

// ----------------------------------------------------------------------------

std::vector<containers::Features> FastProp::build_subfeatures(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::vector<size_t> &_index,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::shared_ptr<std::vector<size_t>> &_rownums,
    const std::optional<const helpers::MappedContainer> &_mapped ) const
{
    assert_true( placeholder().joined_tables_.size() <= subfeatures().size() );

    assert_true(
        !_mapped || placeholder().joined_tables_.size() <= _mapped->size() );

    std::vector<containers::Features> features;

    for ( size_t i = 0; i < subfeatures().size(); ++i )
        {
            if ( !subfeatures().at( i ) )
                {
                    features.push_back( containers::Features() );
                    continue;
                }

            assert_true( i < placeholder().joined_tables_.size() );

            const auto joined_table = placeholder().joined_tables_.at( i );

            const auto new_population =
                find_peripheral( _peripheral, joined_table.name_ );

            const auto subfeature_index = make_subfeature_index( i, _index );

            const auto subfeature_rownums = make_subfeature_rownums(
                _rownums, _population, new_population, i );

            assert_true( !_mapped || _mapped->subcontainers( i ) );

            const auto mapped =
                _mapped ? std::make_optional<const helpers::MappedContainer>(
                              *_mapped->subcontainers( i ) )
                        : static_cast<
                              std::optional<const helpers::MappedContainer>>(
                              std::nullopt );

            const auto f = subfeatures().at( i )->transform(
                new_population,
                _peripheral,
                subfeature_index,
                _logger,
                subfeature_rownums,
                mapped,
                true );

            const auto f_expanded = expand_subfeatures(
                f, subfeature_index, subfeatures().at( i )->num_features() );

            features.push_back( f_expanded );
        }

    return features;
}

// ----------------------------------------------------------------------------

std::vector<Float> FastProp::calc_r_squared(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::shared_ptr<std::vector<size_t>> &_rownums ) const
{
    assert_true( _rownums );

    auto r_squared = std::vector<Float>();

    constexpr size_t batch_size = 100;

    for ( size_t begin = 0; begin < abstract_features().size();
          begin += batch_size )
        {
            const auto end =
                std::min( abstract_features().size(), begin + batch_size );

            const auto index =
                stl::make::vector<size_t>( std::views::iota( begin, end ) );

            const auto features = transform(
                _population,
                _peripheral,
                index,
                nullptr,
                _rownums,
                std::nullopt,
                false );

            const auto r = RSquared::calculate(
                _population.targets_, features, *_rownums );

            r_squared.insert( r_squared.end(), r.begin(), r.end() );

            if ( _logger )
                {
                    const auto progress = std::to_string(
                        ( end * 100 ) / abstract_features().size() );

                    _logger->log(
                        "Built " + std::to_string( end ) +
                        " features. Progress: " + progress + "\%." );
                }
        }

    return r_squared;
}

// ----------------------------------------------------------------------------

Float FastProp::calc_threshold( const std::vector<Float> &_r_squared ) const
{
    auto r_squared = _r_squared;

    std::ranges::sort( r_squared, std::ranges::greater() );

    assert_true( r_squared.size() > hyperparameters().num_features_ );

    return r_squared.at( hyperparameters().num_features_ );
}

// ----------------------------------------------------------------------------

std::map<helpers::ColumnDescription, Float> FastProp::column_importances(
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

typename FastProp::Vocabulary FastProp::expand_vocabulary(
    const Vocabulary &_vocabulary ) const
{
    const auto find_vocabulary =
        [this, &_vocabulary](
            const containers::Placeholder &_joined_table ) -> VocabForDf {
        const auto ix = find_peripheral_ix( _joined_table.name_ );
        assert_true( ix < _vocabulary.size() );
        return _vocabulary.at( ix );
    };

    auto range =
        placeholder().joined_tables_ | std::views::transform( find_vocabulary );

    return stl::make::vector<VocabForDf>( range );
}

// ----------------------------------------------------------------------------

containers::Features FastProp::expand_subfeatures(
    const containers::Features &_subfeatures,
    const std::vector<size_t> &_subfeature_index,
    const size_t _num_subfeatures ) const
{
    assert_true( _subfeatures.size() == _subfeature_index.size() );

    auto expanded_subfeatures = containers::Features( _num_subfeatures );

    for ( size_t i = 0; i < _subfeatures.size(); ++i )
        {
            const auto ix = _subfeature_index.at( i );

            assert_true( ix < expanded_subfeatures.size() );

            expanded_subfeatures.at( ix ) = _subfeatures.at( i );
        }

    return expanded_subfeatures;
}

// ----------------------------------------------------------------------------

void FastProp::extract_schemas(
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

void FastProp::extract_schemas( const TableHolder &_table_holder )
{
    assert_true(
        _table_holder.main_tables().size() ==
        _table_holder.peripheral_tables().size() );

    const auto main_table_schemas =
        std::make_shared<std::vector<containers::Placeholder>>();

    const auto peripheral_table_schemas =
        std::make_shared<std::vector<containers::Placeholder>>();

    for ( size_t i = 0; i < _table_holder.main_tables().size(); ++i )
        {
            main_table_schemas->push_back(
                _table_holder.main_tables().at( i ).df().to_schema() );

            peripheral_table_schemas->push_back(
                _table_holder.peripheral_tables().at( i ).to_schema() );
        }

    main_table_schemas_ = main_table_schemas;

    peripheral_table_schemas_ = peripheral_table_schemas;
}

// ----------------------------------------------------------------------------

std::vector<Int> FastProp::find_most_frequent_categories(
    const containers::Column<Int> &_col ) const
{
    std::map<Int, size_t> frequencies;

    for ( size_t i = 0; i < _col.nrows_; ++i )
        {
            const auto val = _col[i];

            const auto it = frequencies.find( val );

            if ( it == frequencies.end() )
                {
                    frequencies[val] = 1;
                }
            else
                {
                    it->second++;
                }
        }

    using Pair = std::pair<Int, size_t>;

    const auto sort_by_second = []( const Pair p1, const Pair p2 ) {
        return p1.second > p2.second;
    };

    auto pairs = std::vector<Pair>( frequencies.begin(), frequencies.end() );

    std::ranges::sort( pairs, sort_by_second );

    const auto get_first = []( const Pair p ) -> Int { return p.first; };

    const auto is_not_null = []( const Int val ) -> bool { return val >= 0; };

    const auto range = pairs | std::views::transform( get_first ) |
                       std::views::filter( is_not_null ) |
                       std::views::take( hyperparameters().n_most_frequent_ );

    return stl::make::vector<Int>( range );
}

// ----------------------------------------------------------------------------

containers::DataFrame FastProp::find_peripheral(
    const std::vector<containers::DataFrame> &_peripheral,
    const std::string &_name ) const
{
    if ( _peripheral.size() < peripheral().size() )
        {
            throw std::invalid_argument(
                "The number of peripheral tables does not match the number "
                "of "
                "peripheral placeholders." );
        }

    const auto ix = find_peripheral_ix( _name );

    assert_true( ix < _peripheral.size() );

    return _peripheral.at( ix );
}

// ----------------------------------------------------------------------------

size_t FastProp::find_peripheral_ix( const std::string &_name ) const
{
    for ( size_t i = 0; i < peripheral().size(); ++i )
        {
            if ( peripheral().at( i ) == _name )
                {
                    return i;
                }
        }

    throw std::invalid_argument(
        "Placeholder named '" + _name + "' not found." );

    return 0;
}

// ----------------------------------------------------------------------------

void FastProp::fit(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::optional<helpers::MappedContainer> _mapped,
    const bool _as_subfeatures )
{
    assert_true( !_as_subfeatures || _mapped );

    extract_schemas( _population, _peripheral );

    const auto [population_table, peripheral_tables, word_indices] =
        handle_text_fields(
            _population, _peripheral, _logger, _as_subfeatures );

    const auto mapped = handle_mappings(
        population_table, peripheral_tables, _mapped, word_indices, _logger );

    const auto rownums = sample_from_population( population_table.nrows() );

    const auto population_view =
        containers::DataFrameView( population_table, rownums );

    const auto table_holder = TableHolder(
        placeholder(),
        population_view,
        peripheral_tables,
        peripheral(),
        std::nullopt,
        word_indices,
        mapped );

    extract_schemas( table_holder );

    subfeatures_ =
        fit_subfeatures( table_holder, peripheral_tables, _logger, mapped );

    const auto conditions = make_conditions( table_holder );

    assert_true(
        table_holder.main_tables().size() ==
        table_holder.peripheral_tables().size() );

    assert_true(
        table_holder.main_tables().size() >=
        placeholder().joined_tables_.size() );

    const auto abstract_features =
        std::make_shared<std::vector<containers::AbstractFeature>>();

    for ( size_t i = 0; i < table_holder.main_tables().size(); ++i )
        {
            fit_on_peripheral(
                table_holder.main_tables().at( i ).df(),
                table_holder.peripheral_tables().at( i ),
                i,
                conditions,
                abstract_features );
        }

    abstract_features_ = abstract_features;

    if ( _logger && !_as_subfeatures )
        {
            const auto num_candidates =
                std::to_string( abstract_features->size() );

            const auto msg =
                "FastProp: Trying " + num_candidates + " features...";

            _logger->log( msg );
        }

    if ( !_as_subfeatures )
        {
            abstract_features_ = select_features(
                population_table, peripheral_tables, _logger, rownums );
        }
}

// ----------------------------------------------------------------------------

void FastProp::fit_on_categoricals(
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
            if ( _peripheral.categorical_unit( input_col )
                     .find( "comparison only" ) != std::string::npos )
                {
                    continue;
                }

            const auto condition_is_categorical =
                []( const containers::Condition &_cond ) {
                    return _cond.data_used_ == enums::DataUsed::categorical;
                };

            const auto any_condition_is_categorical = std::any_of(
                _conditions.begin(),
                _conditions.end(),
                condition_is_categorical );

            if ( any_condition_is_categorical )
                {
                    continue;
                }

            for ( const auto agg : hyperparameters().aggregations_ )
                {
                    if ( !is_categorical( agg ) )
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

void FastProp::fit_on_categoricals_by_categories(
    const containers::DataFrame &_population,
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
            if ( _peripheral.categorical_unit( input_col )
                     .find( "comparison only" ) != std::string::npos )
                {
                    continue;
                }

            const auto condition_is_categorical =
                []( const containers::Condition &_cond ) {
                    return _cond.data_used_ == enums::DataUsed::categorical;
                };

            const auto any_condition_is_categorical = std::any_of(
                _conditions.begin(),
                _conditions.end(),
                condition_is_categorical );

            if ( any_condition_is_categorical )
                {
                    continue;
                }

            const auto most_frequent = find_most_frequent_categories(
                _peripheral.categorical_col( input_col ) );

            for ( const auto categorical_value : most_frequent )
                {
                    for ( const auto agg : hyperparameters().aggregations_ )
                        {
                            if ( !is_numerical( agg ) )
                                {
                                    continue;
                                }

                            if ( skip_first_last(
                                     agg, _population, _peripheral ) )
                                {
                                    continue;
                                }

                            _abstract_features->push_back(
                                containers::AbstractFeature(
                                    enums::Parser<enums::Aggregation>::parse(
                                        agg ),
                                    _conditions,
                                    input_col,
                                    _peripheral_ix,
                                    enums::DataUsed::categorical,
                                    categorical_value ) );
                        }
                }
        }
}

// ----------------------------------------------------------------------------

void FastProp::fit_on_discretes(
    const containers::DataFrame &_population,
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

                    if ( skip_first_last( agg, _population, _peripheral ) )
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

void FastProp::fit_on_numericals(
    const containers::DataFrame &_population,
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

                    if ( skip_first_last( agg, _population, _peripheral ) )
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

void FastProp::fit_on_same_units_categorical(
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
                    const bool same_unit =
                        _population.categorical_unit( output_col ) != "" &&
                        _population.categorical_unit( output_col ) ==
                            _peripheral.categorical_unit( input_col );

                    if ( !same_unit )
                        {
                            continue;
                        }

                    for ( const auto agg : hyperparameters().aggregations_ )
                        {
                            if ( !is_numerical( agg ) )
                                {
                                    continue;
                                }

                            if ( skip_first_last(
                                     agg, _population, _peripheral ) )
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

void FastProp::fit_on_same_units_discrete(
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

                            if ( skip_first_last(
                                     agg, _population, _peripheral ) )
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

void FastProp::fit_on_same_units_numerical(
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

                            if ( skip_first_last(
                                     agg, _population, _peripheral ) )
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

void FastProp::fit_on_subfeatures(
    const containers::DataFrame &_population,
    const containers::DataFrame &_peripheral,
    const size_t _peripheral_ix,
    const std::vector<containers::Condition> &_conditions,
    std::shared_ptr<std::vector<containers::AbstractFeature>>
        _abstract_features ) const
{
    if ( _peripheral_ix >= subfeatures().size() )
        {
            return;
        }

    if ( !subfeatures().at( _peripheral_ix ) )
        {
            return;
        }

    assert_true( _abstract_features );

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

                    if ( skip_first_last( agg, _population, _peripheral ) )
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

void FastProp::fit_on_peripheral(
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

            fit_on_categoricals_by_categories(
                _population,
                _peripheral,
                _peripheral_ix,
                cond,
                _abstract_features );

            fit_on_discretes(
                _population,
                _peripheral,
                _peripheral_ix,
                cond,
                _abstract_features );

            fit_on_numericals(
                _population,
                _peripheral,
                _peripheral_ix,
                cond,
                _abstract_features );

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

            fit_on_subfeatures(
                _population,
                _peripheral,
                _peripheral_ix,
                cond,
                _abstract_features );

            if ( _peripheral.num_time_stamps() > 0 )
                {
                    _abstract_features->push_back( containers::AbstractFeature(
                        enums::Aggregation::avg_time_between,
                        cond,
                        enums::DataUsed::not_applicable,
                        0,
                        _peripheral_ix ) );
                }
        }

    if ( has_count() )
        {
            _abstract_features->push_back( containers::AbstractFeature(
                enums::Aggregation::count,
                {},
                enums::DataUsed::not_applicable,
                0,
                _peripheral_ix ) );
        }
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::vector<std::optional<FastProp>>>
FastProp::fit_subfeatures(
    const TableHolder &_table_holder,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::optional<const helpers::MappedContainer> &_mapped ) const
{
    assert_true(
        placeholder().joined_tables_.size() <=
        _table_holder.subtables().size() );

    assert_true(
        !_mapped || placeholder().joined_tables_.size() <= _mapped->size() );

    assert_msg(
        !_mapped || _table_holder.subtables().size() == _mapped->size(),
        "_table_holder.subtables().size(): " +
            std::to_string( _table_holder.subtables().size() ) +
            ", _mapped->size(): " + std::to_string( _mapped->size() ) );

    const auto subfeatures =
        std::make_shared<std::vector<std::optional<FastProp>>>();

    for ( size_t i = 0; i < placeholder().joined_tables_.size(); ++i )
        {
            const auto &joined_table = placeholder().joined_tables_.at( i );

            if ( !_table_holder.subtables().at( i ) )
                {
                    subfeatures->push_back( std::nullopt );
                    continue;
                }

            subfeatures->push_back( std::make_optional<FastProp>(
                hyperparameters_,
                peripheral_,
                std::make_shared<const containers::Placeholder>(
                    joined_table ) ) );

            const auto population =
                find_peripheral( _peripheral, joined_table.name_ );

            assert_true( !_mapped || _mapped->subcontainers( i ) );

            const auto mapped =
                _mapped ? std::make_optional<const helpers::MappedContainer>(
                              *_mapped->subcontainers( i ) )
                        : std::optional<const helpers::MappedContainer>();

            subfeatures->back()->fit(
                population, _peripheral, _logger, mapped, true );
        }

    return subfeatures;
}

// ----------------------------------------------------------------------------

size_t FastProp::get_num_threads() const
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

std::optional<helpers::MappedContainer> FastProp::handle_mappings(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::optional<helpers::MappedContainer> _mapped,
    const helpers::WordIndexContainer &_word_indices,
    const std::shared_ptr<const logging::AbstractLogger> _logger )
{
    if ( _mapped )
        {
            return *_mapped;
        }

    if ( hyperparameters().min_freq_ == 0 )
        {
            return std::nullopt;
        }

    mappings_ = helpers::MappingContainerMaker::fit(
        hyperparameters().min_freq_,
        placeholder(),
        _population,
        _peripheral,
        peripheral(),
        _word_indices,
        _logger );

    return helpers::MappingContainerMaker::transform(
        mappings_,
        placeholder(),
        _population,
        _peripheral,
        peripheral(),
        _word_indices,
        _logger );
}

// ----------------------------------------------------------------------------

std::tuple<
    containers::DataFrame,
    std::vector<containers::DataFrame>,
    helpers::WordIndexContainer>
FastProp::handle_text_fields(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const bool _as_subfeatures )
{
    const bool split_text_fields =
        infer_split_text_fields( _peripheral, _as_subfeatures );

    const auto [population, peripheral] =
        split_text_fields ? helpers::TextFieldSplitter::split_text_fields(
                                _population, _peripheral, _logger )
                          : std::make_pair( _population, _peripheral );

    const auto has_text_fields =
        []( const containers::DataFrame &_df ) -> bool {
        return _df.num_text() > 0;
    };

    const bool any_text_fields =
        has_text_fields( _population ) ||
        std::any_of( _peripheral.begin(), _peripheral.end(), has_text_fields );

    if ( any_text_fields ) _logger->log( "Indexing text fields..." );

    vocabulary_ = std::make_shared<const helpers::VocabularyContainer>(
        hyperparameters().min_df_,
        hyperparameters().vocab_size_,
        population,
        peripheral );

    if ( any_text_fields ) _logger->log( "Progress: 50%." );

    const auto word_indices =
        helpers::WordIndexContainer( population, peripheral, *vocabulary_ );

    if ( any_text_fields ) _logger->log( "Progress: 100%." );

    return std::make_tuple( population, peripheral, word_indices );
}

// ----------------------------------------------------------------------------

std::vector<size_t> FastProp::infer_index(
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
FastProp::infer_importance(
    const size_t _feature_num,
    const Float _importance_factor,
    std::vector<std::vector<Float>> *_subimportance_factors ) const
{
    assert_true( _feature_num < abstract_features().size() );

    const auto abstract_feature = abstract_features().at( _feature_num );

    assert_true(
        _subimportance_factors->size() <= peripheral_table_schemas().size() );

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

            case enums::DataUsed::text:
                {
                    const auto col_desc = helpers::ColumnDescription(
                        helpers::ColumnDescription::PERIPHERAL,
                        peripheral.name(),
                        peripheral.text_name( abstract_feature.input_col_ ) );

                    return { std::make_pair( col_desc, _importance_factor ) };
                }

            default:
                assert_true( false && "Unknown data used" );

                const auto col_desc = helpers::ColumnDescription( "", "", "" );

                return { std::make_pair( col_desc, 0.0 ) };
        }
}

// ----------------------------------------------------------------------------

bool FastProp::infer_split_text_fields(
    const std::vector<containers::DataFrame> &_peripheral,
    const bool _as_subfeatures ) const
{
    const auto is_text_field = []( const containers::DataFrame &_df ) -> bool {
        return _df.name_.find( helpers::Macros::text_field() ) !=
               std::string::npos;
    };

    const bool split_text_fields =
        hyperparameters().split_text_fields_ && !_as_subfeatures &&
        std::none_of( _peripheral.begin(), _peripheral.end(), is_text_field );

    return split_text_fields;
}

// ----------------------------------------------------------------------------

std::vector<std::vector<Float>> FastProp::init_subimportance_factors() const
{
    const auto make_factors =
        []( const std::optional<FastProp> &sub ) -> std::vector<Float> {
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

bool FastProp::is_categorical( const std::string &_agg ) const
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

bool FastProp::is_numerical( const std::string &_agg ) const
{
    const auto agg = enums::Parser<enums::Aggregation>::parse( _agg );
    return ( agg != enums::Aggregation::count );
}

// ----------------------------------------------------------------------------

std::vector<std::vector<containers::Match>> FastProp::make_matches(
    const TableHolder &_table_holder, const size_t _rownum ) const
{
    // ------------------------------------------------------------

    const auto make_match = []( const size_t ix_input,
                                const size_t ix_output ) {
        return containers::Match{ ix_input, ix_output };
    };

    // ------------------------------------------------------------

    assert_true(
        _table_holder.main_tables().size() ==
        _table_holder.peripheral_tables().size() );

    // ------------------------------------------------------------

    auto all_matches = std::vector<std::vector<containers::Match>>();

    for ( size_t i = 0; i < _table_holder.main_tables().size(); ++i )
        {
            const auto population = _table_holder.main_tables().at( i ).df();

            const auto peripheral = _table_holder.peripheral_tables().at( i );

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

void FastProp::save( const std::string &_fname ) const
{
    std::ofstream fs( _fname, std::ofstream::out );

    Poco::JSON::Stringifier::stringify( to_json_obj(), fs );

    fs.close();
}

// ----------------------------------------------------------------------------

containers::Features FastProp::init_features(
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

void FastProp::log_progress(
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

std::vector<std::vector<containers::Condition>> FastProp::make_conditions(
    const TableHolder &_table_holder ) const
{
    auto conditions = std::vector<std::vector<containers::Condition>>();

    conditions.push_back( {} );

    assert_true(
        _table_holder.main_tables().size() ==
        _table_holder.peripheral_tables().size() );

    for ( size_t i = 0; i < _table_holder.main_tables().size(); ++i )
        {
            const auto &population = _table_holder.main_tables().at( i ).df();

            const auto &peripheral = _table_holder.peripheral_tables().at( i );

            make_categorical_conditions( peripheral, i, &conditions );

            make_same_units_categorical_conditions(
                population, peripheral, i, &conditions );
        }

    return conditions;
}

// ----------------------------------------------------------------------------

void FastProp::make_categorical_conditions(
    const containers::DataFrame &_peripheral,
    const size_t _peripheral_ix,
    std::vector<std::vector<containers::Condition>> *_conditions ) const
{
    if ( hyperparameters().n_most_frequent_ == 0 )
        {
            return;
        }

    for ( size_t input_col = 0; input_col < _peripheral.num_categoricals();
          ++input_col )
        {
            if ( _peripheral.categorical_unit( input_col )
                     .find( "comparison only" ) != std::string::npos )
                {
                    continue;
                }

            const auto most_frequent = find_most_frequent_categories(
                _peripheral.categorical_col( input_col ) );

            for ( const auto category_used : most_frequent )
                {
                    _conditions->push_back( { containers::Condition(
                        category_used,
                        enums::DataUsed::categorical,
                        input_col,
                        _peripheral_ix ) } );
                }
        }
}

// ----------------------------------------------------------------------------

void FastProp::make_same_units_categorical_conditions(
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

std::vector<size_t> FastProp::make_subfeature_index(
    const size_t _peripheral_ix, const std::vector<size_t> &_index ) const
{
    const auto get_feature = [this]( const size_t ix ) {
        assert_true( ix < abstract_features().size() );
        return abstract_features().at( ix );
    };

    const auto is_relevant_feature =
        [_peripheral_ix]( const containers::AbstractFeature &f ) {
            return f.data_used_ == enums::DataUsed::subfeatures &&
                   f.peripheral_ == _peripheral_ix;
        };

    const auto get_input_col = []( const containers::AbstractFeature &f ) {
        return f.input_col_;
    };

    const auto range = _index | std::views::transform( get_feature ) |
                       std::views::filter( is_relevant_feature ) |
                       std::views::transform( get_input_col );

    const auto s = stl::make::set<size_t>( range );

    return std::vector<size_t>( s.begin(), s.end() );
}

// ----------------------------------------------------------------------------

std::shared_ptr<std::vector<size_t>> FastProp::make_subfeature_rownums(
    const std::shared_ptr<std::vector<size_t>> &_rownums,
    const containers::DataFrame &_population,
    const containers::DataFrame &_peripheral,
    const size_t _ix ) const
{
    if ( !_rownums )
        {
            return nullptr;
        }

    assert_true( _ix < placeholder().join_keys_used_.size() );

    const auto population = _population.create_subview(
        "POPULATION",
        placeholder().join_keys_used_.at( _ix ),
        placeholder().time_stamps_used_.at( _ix ),
        "",
        false,
        {},
        {},
        {} );

    const auto peripheral = _peripheral.create_subview(
        "PERIPHERAL",
        placeholder().other_join_keys_used_.at( _ix ),
        placeholder().other_time_stamps_used_.at( _ix ),
        placeholder().upper_time_stamps_used_.at( _ix ),
        placeholder().allow_lagged_targets_.at( _ix ),
        {},
        {},
        {} );

    const auto get_ix_input = []( size_t _ix_input,
                                  size_t _ix_output ) -> size_t {
        return _ix_input;
    };

    std::set<size_t> unique_indices;

    std::vector<size_t> peripheral_indices;

    for ( const auto ix_output : *_rownums )
        {
            peripheral_indices.clear();

            helpers::Matchmaker<
                containers::DataFrame,
                size_t,
                decltype( get_ix_input )>::
                make_matches(
                    population,
                    peripheral,
                    true,
                    ix_output,
                    get_ix_input,
                    &peripheral_indices );

            unique_indices.insert(
                peripheral_indices.begin(), peripheral_indices.end() );
        }

    return std::make_shared<std::vector<size_t>>(
        unique_indices.begin(), unique_indices.end() );
}

// ----------------------------------------------------------------------------

std::shared_ptr<std::vector<size_t>> FastProp::make_rownums(
    const size_t _thread_num,
    const size_t _nrows,
    const std::shared_ptr<std::vector<size_t>> &_rownums ) const
{
    const size_t num_threads = get_num_threads();

    assert_true( _thread_num < num_threads );

    const auto nrows = _rownums ? _rownums->size() : _nrows;

    const size_t rows_per_thread = nrows / num_threads;

    const size_t begin = _thread_num * rows_per_thread;

    const size_t end = ( _thread_num < num_threads - 1 )
                           ? ( _thread_num + 1 ) * rows_per_thread
                           : nrows;

    if ( _rownums )
        {
            return std::make_shared<std::vector<size_t>>(
                _rownums->begin() + begin, _rownums->begin() + end );
        }

    const auto rownums = std::make_shared<std::vector<size_t>>( end - begin );

    std::iota( rownums->begin(), rownums->end(), begin );

    return rownums;
}

// ----------------------------------------------------------------------------

std::shared_ptr<std::vector<size_t>> FastProp::sample_from_population(
    const size_t _nrows ) const
{
    std::mt19937 rng;

    std::uniform_real_distribution<Float> dist( 0.0, 1.0 );

    const auto include = [this, &rng, &dist]( const size_t rownum ) -> bool {
        return dist( rng ) < hyperparameters().sampling_factor_;
    };

    auto iota = std::views::iota( static_cast<size_t>( 0 ), _nrows );

    auto range = iota | std::views::filter( include );

    return std::make_shared<std::vector<size_t>>(
        stl::make::vector<size_t>( range ) );
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::vector<containers::AbstractFeature>>
FastProp::select_features(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::shared_ptr<std::vector<size_t>> &_rownums ) const
{
    if ( abstract_features().size() <= hyperparameters().num_features_ )
        {
            if ( _logger )
                {
                    _logger->log( "Trained features. Progress: 100\%." );
                }

            return abstract_features_;
        }

    const auto r_squared =
        calc_r_squared( _population, _peripheral, _logger, _rownums );

    const auto threshold = calc_threshold( r_squared );

    const auto r_greater_threshold = [&r_squared,
                                      threshold]( const size_t ix ) {
        return r_squared.at( ix ) > threshold;
    };

    const auto get_feature = [this]( const size_t ix ) {
        return abstract_features().at( ix );
    };

    assert_true( r_squared.size() == abstract_features().size() );

    const auto iota =
        std::views::iota( static_cast<size_t>( 0 ), r_squared.size() );

    const auto range = iota | std::views::filter( r_greater_threshold ) |
                       std::views::transform( get_feature );

    return std::make_shared<std::vector<containers::AbstractFeature>>(
        stl::make::vector<containers::AbstractFeature>( range ) );
}

// ----------------------------------------------------------------------------

bool FastProp::skip_first_last(
    const std::string &_agg,
    const containers::DataFrame &_population,
    const containers::DataFrame &_peripheral ) const
{
    const auto agg = enums::Parser<enums::Aggregation>::parse( _agg );

    if ( !Aggregator::is_first_last( agg ) )
        {
            return false;
        }

    return (
        _population.num_time_stamps() == 0 ||
        _peripheral.num_time_stamps() == 0 );
}

// ----------------------------------------------------------------------------

void FastProp::spawn_threads(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::vector<containers::Features> &_subfeatures,
    const std::optional<helpers::WordIndexContainer> &_word_indices,
    const std::optional<const helpers::MappedContainer> &_mapped,
    const std::vector<size_t> &_index,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::shared_ptr<std::vector<size_t>> &_rownums,
    containers::Features *_features ) const
{
    // ------------------------------------------------------------------------

    auto num_completed = std::atomic<size_t>( 0 );

    // ------------------------------------------------------------------------

    const auto execute_task = [this,
                               _population,
                               _peripheral,
                               _subfeatures,
                               _word_indices,
                               _mapped,
                               _index,
                               _logger,
                               _rownums,
                               &num_completed,
                               _features]( const size_t thread_num ) {
        try
            {
                build_rows(
                    _population,
                    _peripheral,
                    _subfeatures,
                    _word_indices,
                    _mapped,
                    _index,
                    _logger,
                    _rownums,
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

    log_progress( _logger, 100, 100 );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void FastProp::subfeatures_to_sql(
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

containers::Features FastProp::transform(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::optional<std::vector<size_t>> &_index,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::shared_ptr<std::vector<size_t>> &_rownums,
    const std::optional<const helpers::MappedContainer> _mapped,
    const bool _as_subfeatures ) const
{
    if ( _population.nrows() == 0 )
        {
            throw std::runtime_error(
                "Population table needs to contain at least some data!" );
        }

    const auto index = infer_index( _index );

    const bool split_text_fields =
        infer_split_text_fields( _peripheral, _as_subfeatures );

    const auto [population_table, peripheral_tables] =
        split_text_fields ? helpers::TextFieldSplitter::split_text_fields(
                                _population, _peripheral, _logger )
                          : std::make_pair( _population, _peripheral );

    const auto word_indices =
        vocabulary_ ? std::make_optional<helpers::WordIndexContainer>(
                          population_table, peripheral_tables, *vocabulary_ )
                    : std::optional<helpers::WordIndexContainer>();

    const auto mapped = _mapped ? _mapped
                                : helpers::MappingContainerMaker::transform(
                                      mappings_,
                                      placeholder(),
                                      population_table,
                                      peripheral_tables,
                                      peripheral(),
                                      word_indices,
                                      _logger );

    const auto subfeatures = build_subfeatures(
        population_table, peripheral_tables, index, _logger, _rownums, mapped );

    if ( _logger )
        {
            const auto msg = _as_subfeatures
                                 ? "FastProp: Building subfeatures..."
                                 : "FastProp: Building features...";

            _logger->log( msg );
        }

    auto features = init_features( population_table.nrows(), index.size() );

    spawn_threads(
        population_table,
        peripheral_tables,
        subfeatures,
        word_indices,
        mapped,
        index,
        _logger,
        _rownums,
        &features );

    return features;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object FastProp::to_json_obj( const bool _schema_only ) const
{
    // ------------------------------------------------------------------------

    Poco::JSON::Object obj;

    // ------------------------------------------------------------------------

    obj.set( "type_", "FastPropModel" );

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

    if ( mappings_ )
        {
            obj.set( "mappings_", mappings_->to_json_obj() );
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

    if ( vocabulary_ )
        {
            obj.set( "vocabulary_", vocabulary_->to_json_obj() );
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

std::vector<std::string> FastProp::to_sql(
    const std::shared_ptr<const std::vector<strings::String>> &_categories,
    const std::string &_feature_prefix,
    const size_t _offset,
    const bool _subfeatures ) const
{
    assert_true( _categories );

    assert_true(
        main_table_schemas().size() == peripheral_table_schemas().size() );

    std::vector<std::string> sql;

    if ( _subfeatures && mappings_ )
        {
            sql.push_back(
                mappings_->to_sql( _categories, _feature_prefix, _offset ) );
        }

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
}  // namespace fastprop
