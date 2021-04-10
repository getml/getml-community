#include "helpers/helpers.hpp"

namespace helpers
{
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
        _min_freq,
        table_holder,
        std::vector<DataFrame>(),
        std::vector<DataFrame>(),
        &progress_logger );
}

// ----------------------------------------------------------------------------

typename MappingContainerMaker::MappingForDf
MappingContainerMaker::fit_on_categoricals(
    const size_t _min_freq,
    const std::vector<DataFrame>& _main_tables,
    const std::vector<DataFrame>& _peripheral_tables,
    logging::ProgressLogger* _progress_logger )
{
    assert_true( _main_tables.size() == _peripheral_tables.size() );

    assert_true( _main_tables.size() > 0 );

    const auto col_to_mapping = [_min_freq, &_main_tables, &_peripheral_tables](
                                    const Column<Int>& _col ) {
        const auto rownum_map =
            MappingContainerMaker::make_rownum_map_categorical( _col );
        return MappingContainerMaker::make_mapping(
            _min_freq, rownum_map, _main_tables, _peripheral_tables );
    };

    const auto range = _peripheral_tables.back().categoricals_ |
                       std::views::transform( col_to_mapping );

    const auto vec = stl::make::vector<MappingForDf::value_type>( range );

    _progress_logger->increment( vec.size() );

    return vec;
}

// ----------------------------------------------------------------------------

typename MappingContainerMaker::MappingForDf
MappingContainerMaker::fit_on_discretes(
    const size_t _min_freq,
    const std::vector<DataFrame>& _main_tables,
    const std::vector<DataFrame>& _peripheral_tables,
    logging::ProgressLogger* _progress_logger )
{
    assert_true( _main_tables.size() == _peripheral_tables.size() );

    assert_true( _main_tables.size() > 0 );

    const auto col_to_mapping = [_min_freq, &_main_tables, &_peripheral_tables](
                                    const Column<Float>& _col ) {
        const auto rownum_map =
            MappingContainerMaker::make_rownum_map_discrete( _col );
        return MappingContainerMaker::make_mapping(
            _min_freq, rownum_map, _main_tables, _peripheral_tables );
    };

    const auto range = _peripheral_tables.back().discretes_ |
                       std::views::transform( col_to_mapping );

    const auto vec = stl::make::vector<MappingForDf::value_type>( range );

    _progress_logger->increment( vec.size() );

    return vec;
}

// ----------------------------------------------------------------------------

typename MappingContainerMaker::MappingForDf MappingContainerMaker::fit_on_text(
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

    const auto col_to_mapping = [_min_freq, &_main_tables, &_peripheral_tables](
                                    const WordIndex& _word_index ) {
        assert_true( _word_index );
        const auto rownum_map =
            MappingContainerMaker::make_rownum_map_text( *_word_index );
        return MappingContainerMaker::make_mapping(
            _min_freq, rownum_map, _main_tables, _peripheral_tables );
    };

    const auto range = _peripheral_tables.back().word_indices_ |
                       std::views::transform( col_to_mapping );

    const auto vec = stl::make::vector<MappingForDf::value_type>( range );

    _progress_logger->increment( vec.size() );

    return vec;
}

// ----------------------------------------------------------------------------

std::shared_ptr<const MappingContainer>
MappingContainerMaker::fit_on_table_holder(
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
                _min_freq, main_tables, peripheral_tables, _progress_logger );

            const auto discrete_mapping = fit_on_discretes(
                _min_freq, main_tables, peripheral_tables, _progress_logger );

            const auto subcontainer =
                _table_holder.subtables().at( i )
                    ? fit_on_table_holder(
                          _min_freq,
                          *_table_holder.subtables().at( i ),
                          main_tables,
                          peripheral_tables,
                          _progress_logger )
                    : std::shared_ptr<MappingContainer>();

            const auto text_mapping = fit_on_text(
                _min_freq, main_tables, peripheral_tables, _progress_logger );

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

std::function<typename MappingContainerMaker::ValueMap(
    const typename MappingContainerMaker::RownumMap& )>
MappingContainerMaker::make_calc_avg_targets(
    const std::vector<DataFrame>& _main_tables )
{
    return [&_main_tables]( const RownumMap& _input ) -> ValueMap {
        // -----------------------------------------------------------

        const auto& rownums = _input.second;

        // -----------------------------------------------------------

        const auto calc_avg =
            [&rownums]( const Column<Float>& _target_col ) -> Float {
            assert_true( rownums.size() > 0 );

            Float sum_target = 0.0;

            for ( const auto r : rownums )
                {
                    sum_target += _target_col[r];
                }

            return sum_target / static_cast<Float>( rownums.size() );
        };

        // -----------------------------------------------------------

        assert_true( _main_tables.size() > 0 );

        assert_true( _main_tables.at( 0 ).targets_.size() > 0 );

        const auto range =
            _main_tables.at( 0 ).targets_ | std::views::transform( calc_avg );

        return std::make_pair(
            _input.first, stl::make::vector<Float>( range ) );
    };
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::map<Int, std::vector<Float>>>
MappingContainerMaker::make_mapping(
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
        [_min_freq](
            const std::pair<Int, std::vector<size_t>>& _input ) -> bool {
        return _input.second.size() >= _min_freq;
    };

    const auto calc_avg_targets = make_calc_avg_targets( _main_tables );

    auto range = _rownum_map | std::views::transform( match_rownums ) |
                 std::views::filter( greater_than_min_freq ) |
                 std::views::transform( calc_avg_targets );

    return std::make_shared<const std::map<Int, std::vector<Float>>>(
        range.begin(), range.end() );
}

// ----------------------------------------------------------------------------

std::function<typename MappingContainerMaker::RownumMap(
    const typename MappingContainerMaker::RownumMap& )>
MappingContainerMaker::make_match_rownums(
    const std::vector<DataFrame>& _main_tables,
    const std::vector<DataFrame>& _peripheral_tables )
{
    return [&_main_tables,
            &_peripheral_tables]( const RownumMap& _input ) -> RownumMap {
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

    const auto ptr = transform_table_holder(
        _mapping, table_holder, "mapping_", &progress_logger );

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
    logging::ProgressLogger* _progress_logger )
{
    const auto num_targets = infer_num_targets( _mapping );

    if ( num_targets == 0 )
        {
            return MappedColumns();
        }

    assert_msg(
        _categorical.size() == _mapping.size(),
        "_categorical.size(): " + std::to_string( _categorical.size() ) +
            ", _mapping.size(): " + std::to_string( _mapping.size() ) );

    const auto transform_col =
        [num_targets, &_mapping, &_categorical, &_feature_postfix](
            const size_t i ) -> Column<Float> {
        const auto colnum = i / num_targets;
        const auto target_num = i % num_targets;
        const auto feature_postfix = _feature_postfix +
                                     std::to_string( colnum + 1 ) + "_" +
                                     std::to_string( target_num + 1 );
        return MappingContainerMaker::transform_categorical_column(
            _mapping,
            _categorical,
            feature_postfix,
            num_targets,
            colnum,
            target_num );
    };

    const auto range =
        std::views::iota(
            static_cast<size_t>( 0 ), _mapping.size() * num_targets ) |
        std::views::transform( transform_col );

    const auto mapped = stl::make::vector<Column<Float>>( range );

    assert_msg(
        _mapping.size() * num_targets == mapped.size(),
        "_mapping.size(): " + std::to_string( _mapping.size() ) +
            ", mapped.size(): " + std::to_string( mapped.size() ) +
            ", num_targets: " + std::to_string( num_targets ) );

    _progress_logger->increment( _mapping.size() );

    return mapped;
}

// ----------------------------------------------------------------------------

Column<Float> MappingContainerMaker::transform_categorical_column(
    const MappingForDf& _mapping,
    const std::vector<Column<Int>>& _categorical,
    const std::string& _feature_postfix,
    const size_t _num_targets,
    const size_t _colnum,
    const size_t _target_num )
{
    assert_true( _colnum < _mapping.size() );

    assert_true( _mapping.at( _colnum ) );

    const auto& mapping = *_mapping.at( _colnum );

    const auto& cat_col = _categorical.at( _colnum );

    auto vec = std::make_shared<std::vector<Float>>( cat_col.nrows_, NAN );

    for ( size_t i = 0; i < cat_col.nrows_; ++i )
        {
            const auto it = mapping.find( cat_col[i] );

            if ( it != mapping.end() )
                {
                    assert_true( it->second.size() == _num_targets );
                    vec->at( i ) = it->second.at( _target_num );
                }
        }

    return Column<Float>(
        vec, cat_col.name_ + "__" + _feature_postfix, vec->size(), "" );
}

// ----------------------------------------------------------------------------

typename MappingContainerMaker::MappedColumns
MappingContainerMaker::transform_discrete(
    const MappingForDf& _mapping,
    const std::vector<Column<Float>>& _discrete,
    const std::string& _feature_postfix,
    logging::ProgressLogger* _progress_logger )
{
    const auto num_targets = infer_num_targets( _mapping );

    if ( num_targets == 0 )
        {
            return MappedColumns();
        }

    assert_msg(
        _discrete.size() == _mapping.size(),
        "_discrete.size(): " + std::to_string( _discrete.size() ) +
            ", _mapping.size(): " + std::to_string( _mapping.size() ) );

    const auto transform_col =
        [num_targets, &_mapping, &_discrete, &_feature_postfix](
            const size_t i ) -> Column<Float> {
        const auto colnum = i / num_targets;
        const auto target_num = i % num_targets;
        const auto feature_postfix = _feature_postfix +
                                     std::to_string( colnum + 1 ) + "_" +
                                     std::to_string( target_num + 1 );
        return MappingContainerMaker::transform_discrete_column(
            _mapping,
            _discrete,
            feature_postfix,
            num_targets,
            colnum,
            target_num );
    };

    const auto range =
        std::views::iota(
            static_cast<size_t>( 0 ), _mapping.size() * num_targets ) |
        std::views::transform( transform_col );

    const auto mapped = stl::make::vector<Column<Float>>( range );

    assert_msg(
        _mapping.size() * num_targets == mapped.size(),
        "_mapping.size(): " + std::to_string( _mapping.size() ) +
            ", mapped.size(): " + std::to_string( mapped.size() ) +
            ", num_targets: " + std::to_string( num_targets ) );

    _progress_logger->increment( _mapping.size() );

    return mapped;
}

// ----------------------------------------------------------------------------

Column<Float> MappingContainerMaker::transform_discrete_column(
    const MappingForDf& _mapping,
    const std::vector<Column<Float>>& _discrete,
    const std::string& _feature_postfix,
    const size_t _num_targets,
    const size_t _colnum,
    const size_t _target_num )
{
    assert_true( _colnum < _mapping.size() );

    assert_true( _mapping.at( _colnum ) );

    const auto& mapping = *_mapping.at( _colnum );

    const auto& dis_col = _discrete.at( _colnum );

    auto vec = std::make_shared<std::vector<Float>>( dis_col.nrows_, NAN );

    for ( size_t i = 0; i < dis_col.nrows_; ++i )
        {
            const auto it = mapping.find( static_cast<Int>( dis_col[i] ) );

            if ( it != mapping.end() )
                {
                    assert_true( it->second.size() == _num_targets );
                    vec->at( i ) = it->second.at( _target_num );
                }
        }

    return Column<Float>(
        vec, dis_col.name_ + "__" + _feature_postfix, vec->size(), "" );
}

// ----------------------------------------------------------------------------

typename MappingContainerMaker::MappedColumns
MappingContainerMaker::transform_text(
    const MappingForDf& _mapping,
    const std::vector<Column<strings::String>>& _text,
    const typename DataFrame::WordIndices& _word_indices,
    const std::string& _feature_postfix,
    logging::ProgressLogger* _progress_logger )
{
    const auto num_targets = infer_num_targets( _mapping );

    if ( num_targets == 0 )
        {
            return MappedColumns();
        }

    assert_msg(
        _text.size() == _word_indices.size(),
        "_text.size(): " + std::to_string( _text.size() ) +
            ", _word_index.size(): " + std::to_string( _word_indices.size() ) );

    assert_msg(
        _text.size() == _mapping.size(),
        "_text.size(): " + std::to_string( _text.size() ) +
            ", _mapping.size(): " + std::to_string( _mapping.size() ) );

    const auto transform_col =
        [num_targets, &_mapping, &_text, &_word_indices, &_feature_postfix](
            const size_t i ) -> Column<Float> {
        const auto colnum = i / num_targets;
        const auto target_num = i % num_targets;
        const auto feature_postfix = _feature_postfix +
                                     std::to_string( colnum + 1 ) + "_" +
                                     std::to_string( target_num + 1 );
        return MappingContainerMaker::transform_text_column(
            _mapping,
            _text,
            _word_indices,
            feature_postfix,
            num_targets,
            colnum,
            target_num );
    };

    const auto range =
        std::views::iota(
            static_cast<size_t>( 0 ), _mapping.size() * num_targets ) |
        std::views::transform( transform_col );

    const auto mapped = stl::make::vector<Column<Float>>( range );

    assert_msg(
        _mapping.size() * num_targets == mapped.size(),
        "_mapping.size(): " + std::to_string( _mapping.size() ) +
            ", mapped.size(): " + std::to_string( mapped.size() ) +
            ", num_targets: " + std::to_string( num_targets ) );

    _progress_logger->increment( _mapping.size() );

    return mapped;
}

// ----------------------------------------------------------------------------

Column<Float> MappingContainerMaker::transform_text_column(
    const MappingForDf& _mapping,
    const std::vector<Column<strings::String>>& _text,
    const typename DataFrame::WordIndices& _word_indices,
    const std::string& _feature_postfix,
    const size_t _num_targets,
    const size_t _colnum,
    const size_t _target_num )
{
    assert_true( _colnum < _mapping.size() );

    assert_true( _mapping.at( _colnum ) );

    assert_true( _word_indices.at( _colnum ) );

    const auto& mapping = *_mapping.at( _colnum );

    const auto& word_index = *_word_indices.at( _colnum );

    auto vec = std::make_shared<std::vector<Float>>( word_index.nrows(), NAN );

    for ( size_t i = 0; i < word_index.nrows(); ++i )
        {
            const auto range = word_index.range( i );

            for ( const auto word : range )
                {
                    const auto it = mapping.find( word );

                    Float num_words = 0.0;

                    if ( it != mapping.end() )
                        {
                            assert_true( it->second.size() == _num_targets );

                            if ( num_words == 0.0 )
                                {
                                    vec->at( i ) = it->second.at( _target_num );
                                }
                            else
                                {
                                    vec->at( i ) +=
                                        it->second.at( _target_num );
                                }

                            ++num_words;
                        }

                    if ( num_words > 0.0 )
                        {
                            vec->at( i ) /= num_words;
                        }
                }
        }

    return Column<Float>(
        vec,
        _text.at( _colnum ).name_ + "__" + _feature_postfix,
        vec->size(),
        "" );
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

    std::vector<MappedColumns> categorical;

    std::vector<MappedColumns> discrete;

    std::vector<MappedColumns> text;

    std::vector<std::shared_ptr<const MappedContainer>> subcontainers;

    for ( size_t i = 0; i < _mapping->categorical().size(); ++i )
        {
            categorical.push_back( transform_categorical(
                _mapping->categorical().at( i ),
                _table_holder.peripheral_tables().at( i ).categoricals_,
                _feature_postfix,
                _progress_logger ) );

            discrete.push_back( transform_discrete(
                _mapping->discrete().at( i ),
                _table_holder.peripheral_tables().at( i ).discretes_,
                _feature_postfix,
                _progress_logger ) );

            if ( _table_holder.subtables().at( i ) )
                {
                    assert_true( _mapping->subcontainers().at( i ) );

                    const auto feature_postfix =
                        _feature_postfix + std::to_string( i + 1 ) + "_";

                    subcontainers.push_back( transform_table_holder(
                        _mapping->subcontainers().at( i ),
                        *_table_holder.subtables().at( i ),
                        feature_postfix,
                        _progress_logger ) );
                }
            else
                {
                    subcontainers.push_back( nullptr );
                }

            text.push_back( transform_text(
                _mapping->text().at( i ),
                _table_holder.peripheral_tables().at( i ).text_,
                _table_holder.peripheral_tables().at( i ).word_indices_,
                _feature_postfix,
                _progress_logger ) );
        }

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
