#include "helpers/helpers.hpp"

namespace helpers
{
// ----------------------------------------------------------------------------

std::pair<Int, std::vector<Float>> MappingContainerMaker::calc_agg_targets(
    const std::vector<MappingAggregation>& _aggregation_enums,
    const DataFrame& _data_frame,
    const RownumPair& _input )
{
    // -----------------------------------------------------------------------

    const auto& rownums = _input.second;

    // -----------------------------------------------------------------------

    const auto calc_aggs =
        [&_aggregation_enums,
         &rownums]( const Column<Float>& _target_col ) -> std::vector<Float> {
        const auto get_value = [&_target_col]( const size_t _i ) -> Float {
            return _target_col[_i];
        };

        const auto value_range = rownums | std::views::transform( get_value );

        const auto aggregate =
            [value_range]( const MappingAggregation& _agg ) -> Float {
            return MappingContainerMaker::aggregate(
                value_range.begin(), value_range.end(), _agg );
        };

        const auto aggregated_range =
            _aggregation_enums | std::views::transform( aggregate );

        return stl::collect::vector<Float>( aggregated_range );
    };

    // -----------------------------------------------------------------------

    const auto range =
        _data_frame.targets_ | std::views::transform( calc_aggs );

    const auto all = stl::collect::vector<std::vector<Float>>( range );

    const auto second = stl::collect::vector<Float>( all | std::views::join );

    // -----------------------------------------------------------------------

    return std::make_pair( _input.first, second );
}

// ----------------------------------------------------------------------------

size_t MappingContainerMaker::count_mappable_columns(
    const TableHolder& _table_holder )
{
    const auto get_count = []( const DataFrame& _df ) -> size_t {
        return _df.num_categoricals() + _df.num_discretes() + _df.num_text();
    };

    const auto get_subcount =
        []( const std::optional<TableHolder>& _subtable ) -> size_t {
        if ( !_subtable )
            {
                return 0;
            }
        return MappingContainerMaker::count_mappable_columns(
            _subtable.value() );
    };

    auto range1 =
        _table_holder.peripheral_tables() | std::views::transform( get_count );

    auto range2 =
        _table_holder.subtables() | std::views::transform( get_subcount );

    return std::accumulate( range1.begin(), range1.end(), 0 ) +
           std::accumulate( range2.begin(), range2.end(), 0 );
}

// ----------------------------------------------------------------------------

std::shared_ptr<const MappingContainer> MappingContainerMaker::fit(
    const std::shared_ptr<const std::vector<std::string>>& _aggregation,
    const std::vector<MappingAggregation>& _aggregation_enums,
    const size_t _min_freq,
    const Placeholder& _placeholder,
    const DataFrame& _population,
    const std::vector<DataFrame>& _peripheral,
    const std::vector<std::string>& _peripheral_names,
    const WordIndexContainer& _word_indices,
    const std::shared_ptr<const logging::AbstractLogger>& _logger )
{
    const auto dummy_rownums = std::make_shared<std::vector<size_t>>( 0 );

    const auto population_view = DataFrameView( _population, dummy_rownums );

    const auto table_holder = TableHolder(
        _placeholder,
        population_view,
        _peripheral,
        _peripheral_names,
        std::nullopt,
        _word_indices );

    const auto total = count_mappable_columns( table_holder );

    auto progress_logger =
        logging::ProgressLogger( "Fitting the mappings...", _logger, total );

    return fit_on_table_holder(
        _aggregation,
        _aggregation_enums,
        _min_freq,
        table_holder,
        std::vector<DataFrame>(),
        std::vector<DataFrame>(),
        &progress_logger );
}

// ----------------------------------------------------------------------------

typename MappingContainerMaker::MappingForDf
MappingContainerMaker::fit_on_categoricals(
    const std::vector<MappingAggregation>& _aggregation_enums,
    const size_t _min_freq,
    const std::vector<DataFrame>& _main_tables,
    const std::vector<DataFrame>& _peripheral_tables,
    logging::ProgressLogger* _progress_logger )
{
    assert_true( _main_tables.size() == _peripheral_tables.size() );

    assert_true( _main_tables.size() > 0 );

    const auto col_to_mapping =
        [&_aggregation_enums, _min_freq, &_main_tables, &_peripheral_tables](
            const Column<Int>& _col ) {
            const auto rownum_map =
                MappingContainerMaker::make_rownum_map_categorical( _col );
            return MappingContainerMaker::make_mapping(
                _aggregation_enums,
                _min_freq,
                rownum_map,
                _main_tables,
                _peripheral_tables );
        };

    const auto range = _peripheral_tables.back().categoricals_ |
                       std::views::transform( col_to_mapping );

    const auto vec = stl::collect::vector<MappingForDf::value_type>( range );

    _progress_logger->increment( vec.size() );

    return vec;
}

// ----------------------------------------------------------------------------

typename MappingContainerMaker::MappingForDf
MappingContainerMaker::fit_on_discretes(
    const std::vector<MappingAggregation>& _aggregation_enums,
    const size_t _min_freq,
    const std::vector<DataFrame>& _main_tables,
    const std::vector<DataFrame>& _peripheral_tables,
    logging::ProgressLogger* _progress_logger )
{
    assert_true( _main_tables.size() == _peripheral_tables.size() );

    assert_true( _main_tables.size() > 0 );

    const auto col_to_mapping =
        [&_aggregation_enums, _min_freq, &_main_tables, &_peripheral_tables](
            const Column<Float>& _col ) {
            const auto rownum_map =
                MappingContainerMaker::make_rownum_map_discrete( _col );
            return MappingContainerMaker::make_mapping(
                _aggregation_enums,
                _min_freq,
                rownum_map,
                _main_tables,
                _peripheral_tables );
        };

    const auto range = _peripheral_tables.back().discretes_ |
                       std::views::transform( col_to_mapping );

    const auto vec = stl::collect::vector<MappingForDf::value_type>( range );

    _progress_logger->increment( vec.size() );

    return vec;
}

// ----------------------------------------------------------------------------

typename MappingContainerMaker::MappingForDf MappingContainerMaker::fit_on_text(
    const std::vector<MappingAggregation>& _aggregation_enums,
    const size_t _min_freq,
    const std::vector<DataFrame>& _main_tables,
    const std::vector<DataFrame>& _peripheral_tables,
    logging::ProgressLogger* _progress_logger )
{
    assert_true( _main_tables.size() == _peripheral_tables.size() );

    assert_true( _main_tables.size() > 0 );

    assert_msg(
        _peripheral_tables.back().text_.size() ==
            _peripheral_tables.back().word_indices_.size(),
        "_peripheral_tables.back().text_.size(): " +
            std::to_string( _peripheral_tables.back().text_.size() ) +
            ", _peripheral_tables.back().word_indices_.size(): " +
            std::to_string( _peripheral_tables.back().word_indices_.size() ) );

    using WordIndex = typename DataFrame::WordIndices::value_type;

    const auto col_to_mapping =
        [&_aggregation_enums, _min_freq, &_main_tables, &_peripheral_tables](
            const WordIndex& _word_index ) {
            assert_true( _word_index );
            const auto rownum_map =
                MappingContainerMaker::make_rownum_map_text( *_word_index );
            return MappingContainerMaker::make_mapping(
                _aggregation_enums,
                _min_freq,
                rownum_map,
                _main_tables,
                _peripheral_tables );
        };

    const auto range = _peripheral_tables.back().word_indices_ |
                       std::views::transform( col_to_mapping );

    const auto vec = stl::collect::vector<MappingForDf::value_type>( range );

    _progress_logger->increment( vec.size() );

    return vec;
}

// ----------------------------------------------------------------------------

std::shared_ptr<const MappingContainer>
MappingContainerMaker::fit_on_table_holder(
    const std::shared_ptr<const std::vector<std::string>>& _aggregation,
    const std::vector<MappingAggregation>& _aggregation_enums,
    const size_t _min_freq,
    const TableHolder& _table_holder,
    const std::vector<DataFrame>& _main_tables,
    const std::vector<DataFrame>& _peripheral_tables,
    logging::ProgressLogger* _progress_logger )
{
    // -----------------------------------------------------------

    assert_true(
        _table_holder.main_tables().size() ==
        _table_holder.peripheral_tables().size() );

    assert_true(
        _table_holder.main_tables().size() ==
        _table_holder.subtables().size() );

    // -----------------------------------------------------------

    const auto append = []( const std::vector<DataFrame>& _vec,
                            const DataFrame& _df ) -> std::vector<DataFrame> {
        auto vec = _vec;
        vec.push_back( _df );
        return vec;
    };

    // -----------------------------------------------------------

    std::vector<MappingForDf> categorical;

    std::vector<Colnames> categorical_names;

    std::vector<MappingForDf> discrete;

    std::vector<Colnames> discrete_names;

    std::vector<std::shared_ptr<const MappingContainer>> subcontainers;

    std::vector<MappingForDf> text;

    std::vector<Colnames> text_names;

    auto table_names = std::make_shared<std::vector<std::string>>();

    for ( size_t i = 0; i < _table_holder.main_tables().size(); ++i )
        {
            const auto main_tables = append(
                _main_tables, _table_holder.main_tables().at( i ).df() );

            const auto peripheral_tables = append(
                _peripheral_tables, _table_holder.peripheral_tables().at( i ) );

            const auto categorical_mapping = fit_on_categoricals(
                _aggregation_enums,
                _min_freq,
                main_tables,
                peripheral_tables,
                _progress_logger );

            const auto discrete_mapping = fit_on_discretes(
                _aggregation_enums,
                _min_freq,
                main_tables,
                peripheral_tables,
                _progress_logger );

            const auto subcontainer =
                _table_holder.subtables().at( i )
                    ? fit_on_table_holder(
                          _aggregation,
                          _aggregation_enums,
                          _min_freq,
                          *_table_holder.subtables().at( i ),
                          main_tables,
                          peripheral_tables,
                          _progress_logger )
                    : std::shared_ptr<MappingContainer>();

            const auto text_mapping = fit_on_text(
                _aggregation_enums,
                _min_freq,
                main_tables,
                peripheral_tables,
                _progress_logger );

            categorical.push_back( categorical_mapping );

            categorical_names.push_back(
                extract_colnames( peripheral_tables.back().categoricals_ ) );

            discrete.push_back( discrete_mapping );

            discrete_names.push_back(
                extract_colnames( peripheral_tables.back().discretes_ ) );

            subcontainers.push_back( subcontainer );

            table_names->push_back( peripheral_tables.back().name_ );

            text.push_back( text_mapping );

            text_names.push_back(
                extract_colnames( peripheral_tables.back().text_ ) );
        }

    // -----------------------------------------------------------

    return std::make_shared<MappingContainer>(
        _aggregation,
        categorical,
        categorical_names,
        discrete,
        discrete_names,
        subcontainers,
        table_names,
        text,
        text_names );

    // -----------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<size_t> MappingContainerMaker::find_output_ix(
    const std::vector<size_t>& _input_ix,
    const DataFrame& _output_table,
    const DataFrame& _input_table )
{
    const auto time_stamp_in_range =
        []( const Float _time_stamp_input,
            const Float _upper_time_stamp,
            const Float _time_stamp_output ) -> bool {
        return (
            ( _time_stamp_input <= _time_stamp_output ) &&
            ( std::isnan( _upper_time_stamp ) ||
              _time_stamp_output < _upper_time_stamp ) );
    };

    std::set<size_t> unique;

    for ( const auto ix : _input_ix )
        {
            if ( !_output_table.has( _input_table.join_key( ix ) ) )
                {
                    continue;
                }

            const auto it = _output_table.find( _input_table.join_key( ix ) );

            const auto time_stamp_input = _input_table.time_stamp( ix );

            const auto upper_time_stamp = _input_table.upper_time_stamp( ix );

            for ( const auto ix_out : it->second )
                {
                    assert_true( ix_out >= 0 );
                    assert_true( ix_out < _output_table.nrows() );

                    const bool use_this = time_stamp_in_range(
                        time_stamp_input,
                        upper_time_stamp,
                        _output_table.time_stamp( ix_out ) );

                    if ( use_this )
                        {
                            unique.insert( static_cast<size_t>( ix_out ) );
                        }
                }
        }

    return std::vector<size_t>( unique.begin(), unique.end() );
}

// ----------------------------------------------------------------------------

size_t MappingContainerMaker::infer_num_targets( const MappingForDf& _mapping )
{
    const auto non_empty = []( const MappingForDf::value_type& m ) -> bool {
        assert_true( m );
        return ( m->size() > 0 );
    };

    const auto it = std::find_if( _mapping.begin(), _mapping.end(), non_empty );

    if ( it == _mapping.end() )
        {
            return 0;
        }

    return ( *it )->begin()->second.size();
}

// ----------------------------------------------------------------------------

std::string MappingContainerMaker::make_colname(
    const std::string& _name,
    const std::string& _feature_postfix,
    const std::vector<std::string>& _aggregation,
    const size_t _weight_num )
{
    const auto [agg, target_num] =
        infer_aggregation_target_num( _aggregation, _weight_num );

    return helpers::SQLGenerator::make_colname( _name ) + "__mapping_" +
           _feature_postfix + "target_" + std::to_string( target_num + 1 ) +
           "_" + helpers::SQLGenerator::to_lower( agg );
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::map<Int, std::vector<Float>>>
MappingContainerMaker::make_mapping(
    const std::vector<MappingAggregation>& _aggregation_enums,
    const size_t _min_freq,
    const std::map<Int, std::vector<size_t>>& _rownum_map,
    const std::vector<DataFrame>& _main_tables,
    const std::vector<DataFrame>& _peripheral_tables )
{
    assert_true( _main_tables.size() == _peripheral_tables.size() );

    assert_true( _main_tables.size() > 0 );

    const auto match_rownums =
        make_match_rownums( _main_tables, _peripheral_tables );

    const auto greater_than_min_freq =
        [_min_freq]( const RownumPair& _input ) -> bool {
        return _input.second.size() >= _min_freq;
    };

    const auto calc_agg = std::bind(
        MappingContainerMaker::calc_agg_targets,
        _aggregation_enums,
        _main_tables.at( 0 ),
        std::placeholders::_1 );

    auto range = _rownum_map | std::views::transform( match_rownums ) |
                 std::views::filter( greater_than_min_freq ) |
                 std::views::transform( calc_agg );

    return std::make_shared<const std::map<Int, std::vector<Float>>>(
        range.begin(), range.end() );
}

// ----------------------------------------------------------------------------

std::function<typename MappingContainerMaker::RownumPair(
    const typename MappingContainerMaker::RownumPair& )>
MappingContainerMaker::make_match_rownums(
    const std::vector<DataFrame>& _main_tables,
    const std::vector<DataFrame>& _peripheral_tables )
{
    return [&_main_tables,
            &_peripheral_tables]( const RownumPair& _input ) -> RownumPair {
        auto rownums = _input.second;

        for ( size_t i = 0; i < _main_tables.size(); ++i )
            {
                const auto ix = _main_tables.size() - 1 - i;

                rownums = MappingContainerMaker::find_output_ix(
                    rownums,
                    _main_tables.at( ix ),
                    _peripheral_tables.at( ix ) );
            }

        return std::make_pair( _input.first, rownums );
    };
}

// ----------------------------------------------------------------------------

std::map<Int, std::vector<size_t>>
MappingContainerMaker::make_rownum_map_categorical( const Column<Int>& _col )
{
    std::map<Int, std::vector<size_t>> rownum_map;

    for ( size_t i = 0; i < _col.nrows_; ++i )
        {
            const auto key = _col[i];

            if ( key < 0 )
                {
                    continue;
                }

            const auto it = rownum_map.find( key );

            if ( it == rownum_map.end() )
                {
                    rownum_map[key] = { i };
                }
            else
                {
                    it->second.push_back( i );
                }
        }

    return rownum_map;
}

// ----------------------------------------------------------------------------

std::map<Int, std::vector<size_t>>
MappingContainerMaker::make_rownum_map_discrete( const Column<Float>& _col )
{
    std::map<Int, std::vector<size_t>> rownum_map;

    for ( size_t i = 0; i < _col.nrows_; ++i )
        {
            if ( std::isnan( _col[i] ) || std::isinf( _col[i] ) )
                {
                    continue;
                }

            const auto key = static_cast<Int>( _col[i] );

            const auto it = rownum_map.find( key );

            if ( it == rownum_map.end() )
                {
                    rownum_map[key] = { i };
                }
            else
                {
                    it->second.push_back( i );
                }
        }

    return rownum_map;
}

// ----------------------------------------------------------------------------

std::map<Int, std::vector<size_t>> MappingContainerMaker::make_rownum_map_text(
    const textmining::WordIndex& _word_index )
{
    std::map<Int, std::vector<size_t>> rownum_map;

    for ( size_t i = 0; i < _word_index.nrows(); ++i )
        {
            const auto range = _word_index.range( i );

            const auto unique_words =
                std::set<Int>( range.begin(), range.end() );

            for ( const auto key : unique_words )
                {
                    const auto it = rownum_map.find( key );

                    if ( it == rownum_map.end() )
                        {
                            rownum_map[key] = { i };
                        }
                    else
                        {
                            it->second.push_back( i );
                        }
                }
        }

    return rownum_map;
}

// ----------------------------------------------------------------------------

MappingAggregation MappingContainerMaker::parse_aggregation(
    const std::string& _str )
{
    if ( _str == AVG )
        {
            return MappingAggregation::avg;
        }

    if ( _str == COUNT )
        {
            return MappingAggregation::count;
        }

    if ( _str == COUNT_ABOVE_MEAN )
        {
            return MappingAggregation::count_above_mean;
        }

    if ( _str == COUNT_BELOW_MEAN )
        {
            return MappingAggregation::count_below_mean;
        }

    if ( _str == COUNT_DISTINCT )
        {
            return MappingAggregation::count_distinct;
        }

    if ( _str == COUNT_MINUS_COUNT_DISTINCT )
        {
            return MappingAggregation::count_minus_count_distinct;
        }

    if ( _str == COUNT_DISTINCT_OVER_COUNT )
        {
            return MappingAggregation::count_distinct_over_count;
        }

    if ( _str == KURTOSIS )
        {
            return MappingAggregation::kurtosis;
        }

    if ( _str == MAX )
        {
            return MappingAggregation::max;
        }

    if ( _str == MEDIAN )
        {
            return MappingAggregation::median;
        }

    if ( _str == MIN )
        {
            return MappingAggregation::min;
        }

    if ( _str == MODE )
        {
            return MappingAggregation::mode;
        }

    if ( _str == NUM_MAX )
        {
            return MappingAggregation::num_max;
        }

    if ( _str == NUM_MIN )
        {
            return MappingAggregation::num_min;
        }

    if ( _str == Q1 )
        {
            return MappingAggregation::q1;
        }

    if ( _str == Q5 )
        {
            return MappingAggregation::q5;
        }

    if ( _str == Q10 )
        {
            return MappingAggregation::q10;
        }

    if ( _str == Q25 )
        {
            return MappingAggregation::q25;
        }

    if ( _str == Q75 )
        {
            return MappingAggregation::q75;
        }

    if ( _str == Q90 )
        {
            return MappingAggregation::q90;
        }

    if ( _str == Q95 )
        {
            return MappingAggregation::q95;
        }

    if ( _str == Q99 )
        {
            return MappingAggregation::q99;
        }

    if ( _str == SKEW )
        {
            return MappingAggregation::skew;
        }

    if ( _str == STDDEV )
        {
            return MappingAggregation::stddev;
        }

    if ( _str == SUM )
        {
            return MappingAggregation::sum;
        }

    if ( _str == VAR )
        {
            return MappingAggregation::var;
        }

    if ( _str == VARIATION_COEFFICIENT )
        {
            return MappingAggregation::variation_coefficient;
        }

    throw_unless( false, "Mapping: Unknown aggregation: '" + _str + "'" );

    return MappingAggregation::avg;
}

// ----------------------------------------------------------------------------

std::optional<const MappedContainer> MappingContainerMaker::transform(
    const std::shared_ptr<const MappingContainer>& _mapping,
    const Placeholder& _placeholder,
    const DataFrame& _population,
    const std::vector<DataFrame>& _peripheral,
    const std::vector<std::string>& _peripheral_names,
    const std::optional<WordIndexContainer>& _word_indices,
    const std::shared_ptr<const logging::AbstractLogger>& _logger )
{
    if ( !_mapping )
        {
            return std::nullopt;
        }

    const auto dummy_rownums = std::make_shared<std::vector<size_t>>( 0 );

    const auto population_view = DataFrameView( _population, dummy_rownums );

    const auto table_holder = TableHolder(
        _placeholder,
        population_view,
        _peripheral,
        _peripheral_names,
        std::nullopt,
        _word_indices );

    const auto total = count_mappable_columns( table_holder );

    auto progress_logger = logging::ProgressLogger(
        "Building the mapping columns...", _logger, total );

    const auto ptr =
        transform_table_holder( _mapping, table_holder, "", &progress_logger );

    assert_true( ptr );

    assert_msg(
        table_holder.subtables().size() == ptr->size(),
        "table_holder.subtables().size(): " +
            std::to_string( table_holder.subtables().size() ) +
            ", ptr->size(): " + std::to_string( ptr->size() ) );

    return *ptr;
}

// ----------------------------------------------------------------------------

typename MappingContainerMaker::MappedColumns
MappingContainerMaker::transform_categorical(
    const MappingForDf& _mapping,
    const std::vector<Column<Int>>& _categorical,
    const std::string& _feature_postfix,
    const std::vector<std::string>& _aggregation,
    logging::ProgressLogger* _progress_logger )
{
    // ------------------------------------------------------------------------

    assert_msg(
        _categorical.size() == _mapping.size(),
        "_categorical.size(): " + std::to_string( _categorical.size() ) +
            ", _mapping.size(): " + std::to_string( _mapping.size() ) );

    // ------------------------------------------------------------------------

    const auto map_to_weight =
        [&_mapping, &_categorical, &_feature_postfix, &_aggregation](
            const size_t _colnum, const size_t _weight_num ) -> Column<Float> {
        return MappingContainerMaker::transform_categorical_column(
            _mapping,
            _categorical,
            _feature_postfix,
            _aggregation,
            _colnum,
            _weight_num );
    };

    // ------------------------------------------------------------------------

    const auto transform_col = make_transform_col( map_to_weight, _mapping );

    // ------------------------------------------------------------------------

    const auto range =
        std::views::iota( static_cast<size_t>( 0 ), _mapping.size() ) |
        std::views::transform( transform_col );

    const auto all = stl::collect::vector<std::vector<Column<Float>>>( range );

    const auto mapped =
        stl::collect::vector<Column<Float>>( all | std::views::join );

    _progress_logger->increment( _mapping.size() );

    return mapped;
}

// ----------------------------------------------------------------------------

Column<Float> MappingContainerMaker::transform_categorical_column(
    const MappingForDf& _mapping,
    const std::vector<Column<Int>>& _categorical,
    const std::string& _feature_postfix,
    const std::vector<std::string>& _aggregation,
    const size_t _colnum,
    const size_t _weight_num )
{
    assert_true( _colnum < _mapping.size() );

    assert_true( _mapping.at( _colnum ) );

    const auto& mapping = *_mapping.at( _colnum );

    const auto map_to_value = [&mapping,
                               _weight_num]( const Int _key ) -> Float {
        const auto it = mapping.find( _key );

        if ( it == mapping.end() )
            {
                return NAN;
            }

        assert_true( _weight_num < it->second.size() );
        return it->second.at( _weight_num );
    };

    const auto& cat_col = _categorical.at( _colnum );

    const auto range = cat_col | std::views::transform( map_to_value );

    const auto data = std::make_shared<std::vector<Float>>(
        stl::collect::vector<Float>( range ) );

    const auto colname = make_colname(
        cat_col.name_, _feature_postfix, _aggregation, _weight_num );

    return Column<Float>( data, colname, "" );
}

// ----------------------------------------------------------------------------

typename MappingContainerMaker::MappedColumns
MappingContainerMaker::transform_discrete(
    const MappingForDf& _mapping,
    const std::vector<Column<Float>>& _discrete,
    const std::string& _feature_postfix,
    const std::vector<std::string>& _aggregation,
    logging::ProgressLogger* _progress_logger )
{
    // ------------------------------------------------------------------------

    assert_msg(
        _discrete.size() == _mapping.size(),
        "_discrete.size(): " + std::to_string( _discrete.size() ) +
            ", _mapping.size(): " + std::to_string( _mapping.size() ) );

    // ------------------------------------------------------------------------

    const auto map_to_weight =
        [&_mapping, &_discrete, &_feature_postfix, &_aggregation](
            const size_t _colnum, const size_t _weight_num ) -> Column<Float> {
        return MappingContainerMaker::transform_discrete_column(
            _mapping,
            _discrete,
            _feature_postfix,
            _aggregation,
            _colnum,
            _weight_num );
    };

    // ------------------------------------------------------------------------

    const auto transform_col = make_transform_col( map_to_weight, _mapping );

    // ------------------------------------------------------------------------

    const auto range =
        std::views::iota( static_cast<size_t>( 0 ), _mapping.size() ) |
        std::views::transform( transform_col );

    const auto all = stl::collect::vector<std::vector<Column<Float>>>( range );

    const auto mapped =
        stl::collect::vector<Column<Float>>( all | std::views::join );

    _progress_logger->increment( _mapping.size() );

    return mapped;
}

// ----------------------------------------------------------------------------

Column<Float> MappingContainerMaker::transform_discrete_column(
    const MappingForDf& _mapping,
    const std::vector<Column<Float>>& _discrete,
    const std::string& _feature_postfix,
    const std::vector<std::string>& _aggregation,
    const size_t _colnum,
    const size_t _weight_num )
{
    assert_true( _colnum < _mapping.size() );

    assert_true( _mapping.at( _colnum ) );

    const auto& mapping = *_mapping.at( _colnum );

    const auto map_to_value = [&mapping,
                               _weight_num]( const Float _key ) -> Float {
        const auto key = static_cast<Int>( _key );

        const auto it = mapping.find( key );

        if ( it == mapping.end() )
            {
                return NAN;
            }

        assert_true( _weight_num < it->second.size() );
        return it->second.at( _weight_num );
    };

    const auto& dis_col = _discrete.at( _colnum );

    const auto range = dis_col | std::views::transform( map_to_value );

    const auto data = std::make_shared<std::vector<Float>>(
        stl::collect::vector<Float>( range ) );

    const auto colname = make_colname(
        dis_col.name_, _feature_postfix, _aggregation, _weight_num );

    return Column<Float>( data, colname, "" );
}

// ----------------------------------------------------------------------------

typename MappingContainerMaker::MappedColumns
MappingContainerMaker::transform_text(
    const MappingForDf& _mapping,
    const std::vector<Column<strings::String>>& _text,
    const typename DataFrame::WordIndices& _word_indices,
    const std::string& _feature_postfix,
    const std::vector<std::string>& _aggregation,
    logging::ProgressLogger* _progress_logger )
{
    // ------------------------------------------------------------------------

    assert_msg(
        _text.size() == _word_indices.size(),
        "_text.size(): " + std::to_string( _text.size() ) +
            ", _word_index.size(): " + std::to_string( _word_indices.size() ) );

    assert_msg(
        _text.size() == _mapping.size(),
        "_text.size(): " + std::to_string( _text.size() ) +
            ", _mapping.size(): " + std::to_string( _mapping.size() ) );

    // ------------------------------------------------------------------------

    const auto map_to_weight =
        [&_mapping, &_text, &_word_indices, &_feature_postfix, &_aggregation](
            const size_t _colnum, const size_t _weight_num ) -> Column<Float> {
        return MappingContainerMaker::transform_text_column(
            _mapping,
            _text,
            _word_indices,
            _feature_postfix,
            _aggregation,
            _colnum,
            _weight_num );
    };

    // ------------------------------------------------------------------------

    const auto transform_col = make_transform_col( map_to_weight, _mapping );

    // ------------------------------------------------------------------------

    const auto range =
        std::views::iota( static_cast<size_t>( 0 ), _mapping.size() ) |
        std::views::transform( transform_col );

    const auto all = stl::collect::vector<std::vector<Column<Float>>>( range );

    const auto mapped =
        stl::collect::vector<Column<Float>>( all | std::views::join );

    _progress_logger->increment( _mapping.size() );

    return mapped;
}

// ----------------------------------------------------------------------------

Column<Float> MappingContainerMaker::transform_text_column(
    const MappingForDf& _mapping,
    const std::vector<Column<strings::String>>& _text,
    const typename DataFrame::WordIndices& _word_indices,
    const std::string& _feature_postfix,
    const std::vector<std::string>& _aggregation,
    const size_t _colnum,
    const size_t _weight_num )
{
    // ----------------------------------------------------------

    assert_true( _colnum < _mapping.size() );

    assert_true( _text.size() == _mapping.size() );

    assert_true( _mapping.at( _colnum ) );

    assert_true( _word_indices.at( _colnum ) );

    const auto& mapping = *_mapping.at( _colnum );

    // ----------------------------------------------------------

    const auto map_word = [&mapping, _weight_num]( const Int _word ) -> Float {
        const auto it = mapping.find( _word );

        if ( it == mapping.end() )
            {
                return NAN;
            }

        assert_true( _weight_num < it->second.size() );

        return it->second.at( _weight_num );
    };

    // ----------------------------------------------------------

    const auto map_word_range =
        [map_word]( const stl::Range<const Int*> _word_range ) -> Float {
        auto range = _word_range | std::views::transform( map_word );
        return helpers::Aggregations::avg( range.begin(), range.end() );
    };

    // ----------------------------------------------------------

    const auto& word_index = *_word_indices.at( _colnum );

    const auto get_range =
        [&word_index]( const size_t _i ) -> stl::Range<const Int*> {
        return word_index.range( _i );
    };

    // ----------------------------------------------------------

    const auto iota =
        std::views::iota( static_cast<size_t>( 0 ), word_index.nrows() );

    const auto range = iota | std::views::transform( get_range ) |
                       std::views::transform( map_word_range );

    const auto data = std::make_shared<std::vector<Float>>(
        stl::collect::vector<Float>( range ) );

    assert_msg(
        data->size() == _text.at( _colnum ).nrows_,
        "data->size(): " + std::to_string( data->size() ) +
            ", text.at( _colnum ).nrows_: " +
            std::to_string( _text.at( _colnum ).nrows_ ) );

    const auto colname = make_colname(
        _text.at( _colnum ).name_,
        _feature_postfix,
        _aggregation,
        _weight_num );

    return Column<Float>( data, colname, "" );
}

// ----------------------------------------------------------------------------

std::shared_ptr<const MappedContainer>
MappingContainerMaker::transform_table_holder(
    const std::shared_ptr<const MappingContainer>& _mapping,
    const TableHolder& _table_holder,
    const std::string& _feature_postfix,
    logging::ProgressLogger* _progress_logger )
{
    assert_true( _mapping );

    assert_true(
        _mapping->categorical().size() ==
        _table_holder.peripheral_tables().size() );

    assert_true(
        _mapping->categorical().size() == _mapping->subcontainers().size() );

    assert_true( _mapping->categorical().size() == _mapping->text().size() );

    assert_true(
        _mapping->categorical().size() == _table_holder.subtables().size() );

    const auto& aggregation = _mapping->aggregation();

    const auto make_feature_postfix =
        [&_feature_postfix]( const size_t _i ) -> std::string {
        return _feature_postfix + std::to_string( _i + 1 ) + "_";
    };

    const auto make_categorical =
        [&_mapping,
         &_table_holder,
         _progress_logger,
         make_feature_postfix,
         &aggregation]( const size_t _i ) -> MappedColumns {
        return MappingContainerMaker::transform_categorical(
            _mapping->categorical().at( _i ),
            _table_holder.peripheral_tables().at( _i ).categoricals_,
            make_feature_postfix( _i ),
            aggregation,
            _progress_logger );
    };

    const auto make_discrete =
        [&_mapping,
         &_table_holder,
         _progress_logger,
         make_feature_postfix,
         &aggregation]( const size_t _i ) -> MappedColumns {
        return MappingContainerMaker::transform_discrete(
            _mapping->discrete().at( _i ),
            _table_holder.peripheral_tables().at( _i ).discretes_,
            make_feature_postfix( _i ),
            aggregation,
            _progress_logger );
    };

    const auto make_subcontainer =
        [&_mapping, &_table_holder, _progress_logger, make_feature_postfix](
            const size_t _i ) -> std::shared_ptr<const MappedContainer> {
        if ( !_table_holder.subtables().at( _i ) )
            {
                return nullptr;
            }

        assert_true( _mapping->subcontainers().at( _i ) );

        return MappingContainerMaker::transform_table_holder(
            _mapping->subcontainers().at( _i ),
            *_table_holder.subtables().at( _i ),
            make_feature_postfix( _i ),
            _progress_logger );
    };

    const auto make_text = [&_mapping,
                            &_table_holder,
                            _progress_logger,
                            make_feature_postfix,
                            &aggregation]( const size_t _i ) -> MappedColumns {
        return MappingContainerMaker::transform_text(
            _mapping->text().at( _i ),
            _table_holder.peripheral_tables().at( _i ).text_,
            _table_holder.peripheral_tables().at( _i ).word_indices_,
            make_feature_postfix( _i ),
            aggregation,
            _progress_logger );
    };

    const auto iota = std::views::iota(
        static_cast<size_t>( 0 ), _mapping->categorical().size() );

    const auto categorical = stl::collect::vector<std::vector<Column<Float>>>(
        iota | std::views::transform( make_categorical ) );

    const auto discrete = stl::collect::vector<std::vector<Column<Float>>>(
        iota | std::views::transform( make_discrete ) );

    const auto subcontainers =
        stl::collect::vector<std::shared_ptr<const MappedContainer>>(
            iota | std::views::transform( make_subcontainer ) );

    const auto text = stl::collect::vector<std::vector<Column<Float>>>(
        iota | std::views::transform( make_text ) );

    const auto ptr = std::make_shared<MappedContainer>(
        categorical, discrete, subcontainers, text );

    assert_msg(
        _table_holder.subtables().size() == ptr->size(),
        "_table_holder.subtables().size(): " +
            std::to_string( _table_holder.subtables().size() ) +
            ", ptr->size(): " + std::to_string( ptr->size() ) );

    return ptr;
}

// ----------------------------------------------------------------------------
}  // namespace helpers
