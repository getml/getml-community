#include "helpers/helpers.hpp"

namespace helpers
{
// ----------------------------------------------------------------------------

std::shared_ptr<const MappingContainer> MappingContainerMaker::fit(
    const size_t _min_df,
    const Placeholder& _placeholder,
    const DataFrame& _population,
    const std::vector<DataFrame>& _peripheral,
    const std::vector<std::string>& _peripheral_names,
    const WordIndexContainer& _word_indices )
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

    return fit_on_table_holder(
        _min_df,
        table_holder,
        std::vector<DataFrame>(),
        std::vector<DataFrame>() );
}

// ----------------------------------------------------------------------------

typename MappingContainerMaker::MappingForDf
MappingContainerMaker::fit_on_categoricals(
    const size_t _min_df,
    const std::vector<DataFrame>& _main_tables,
    const std::vector<DataFrame>& _peripheral_tables )
{
    assert_true( _main_tables.size() == _peripheral_tables.size() );

    assert_true( _main_tables.size() > 0 );

    const auto col_to_mapping = [_min_df, &_main_tables, &_peripheral_tables](
                                    const Column<Int>& _col ) {
        const auto rownum_map =
            MappingContainerMaker::make_rownum_map_categorical( _col );
        return MappingContainerMaker::make_mapping(
            _min_df, rownum_map, _main_tables, _peripheral_tables );
    };

    const auto range = _peripheral_tables.back().categoricals_ |
                       std::views::transform( col_to_mapping );

    return stl::make::vector<MappingForDf::value_type>( range );
}

// ----------------------------------------------------------------------------

typename MappingContainerMaker::MappingForDf MappingContainerMaker::fit_on_text(
    const size_t _min_df,
    const std::vector<DataFrame>& _main_tables,
    const std::vector<DataFrame>& _peripheral_tables )
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

    const auto col_to_mapping = [_min_df, &_main_tables, &_peripheral_tables](
                                    const WordIndex& _word_index ) {
        assert_true( _word_index );
        const auto rownum_map =
            MappingContainerMaker::make_rownum_map_text( *_word_index );
        return MappingContainerMaker::make_mapping(
            _min_df, rownum_map, _main_tables, _peripheral_tables );
    };

    const auto range = _peripheral_tables.back().word_indices_ |
                       std::views::transform( col_to_mapping );

    return stl::make::vector<MappingForDf::value_type>( range );
}

// ----------------------------------------------------------------------------

std::shared_ptr<const MappingContainer>
MappingContainerMaker::fit_on_table_holder(
    const size_t _min_df,
    const TableHolder& _table_holder,
    const std::vector<DataFrame>& _main_tables,
    const std::vector<DataFrame>& _peripheral_tables )
{
    // -----------------------------------------------------------

    assert_true(
        _table_holder.main_tables_.size() ==
        _table_holder.peripheral_tables_.size() );

    assert_true(
        _table_holder.main_tables_.size() == _table_holder.subtables_.size() );

    // -----------------------------------------------------------

    const auto append = []( const std::vector<DataFrame>& _vec,
                            const DataFrame& _df ) -> std::vector<DataFrame> {
        auto vec = _vec;
        vec.push_back( _df );
        return vec;
    };

    // -----------------------------------------------------------

    std::vector<MappingForDf> categorical;

    std::vector<std::shared_ptr<const MappingContainer>> subcontainers;

    std::vector<MappingForDf> text;

    for ( size_t i = 0; i < _table_holder.main_tables_.size(); ++i )
        {
            const auto main_tables =
                append( _main_tables, _table_holder.main_tables_.at( i ).df() );

            const auto peripheral_tables = append(
                _peripheral_tables, _table_holder.peripheral_tables_.at( i ) );

            const auto categorical_mapping =
                fit_on_categoricals( _min_df, main_tables, peripheral_tables );

            const auto subcontainer =
                _table_holder.subtables_.at( i )
                    ? fit_on_table_holder(
                          _min_df,
                          *_table_holder.subtables_.at( i ),
                          main_tables,
                          peripheral_tables )
                    : std::shared_ptr<MappingContainer>();

            const auto text_mapping =
                fit_on_text( _min_df, main_tables, peripheral_tables );

            categorical.push_back( categorical_mapping );

            subcontainers.push_back( subcontainer );

            text.push_back( text_mapping );
        }

    // -----------------------------------------------------------

    return std::make_shared<MappingContainer>(
        categorical, subcontainers, text );

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
    const auto non_empty = []( const MappingForDf::value_type& m ) {
        assert_true( m );
        return m->size() > 0;
    };

    const auto it = std::find_if( _mapping.begin(), _mapping.end(), non_empty );

    if ( it == _mapping.end() )
        {
            return 0;
        }

    return ( *it )->begin()->second.size();
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::map<Int, std::vector<Float>>>
MappingContainerMaker::make_mapping(
    const size_t _min_df,
    const std::map<Int, std::vector<size_t>>& _rownum_map,
    const std::vector<DataFrame>& _main_tables,
    const std::vector<DataFrame>& _peripheral_tables )
{
    // -----------------------------------------------------------

    assert_true( _main_tables.size() == _peripheral_tables.size() );

    assert_true( _main_tables.size() > 0 );

    // -----------------------------------------------------------

    const auto match_rownums =
        [&_main_tables, &_peripheral_tables](
            const std::pair<Int, std::vector<size_t>>& _input )
        -> std::pair<Int, std::vector<size_t>> {
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

    // -----------------------------------------------------------

    const auto greater_than_min_df =
        [_min_df]( const std::pair<Int, std::vector<size_t>>& _input ) -> bool {
        return _input.second.size() >= _min_df;
    };

    // -----------------------------------------------------------

    const auto calc_avg_targets =
        [&_main_tables]( const std::pair<Int, std::vector<size_t>>& _input )
        -> std::pair<Int, std::vector<Float>> {
        const auto& rownums = _input.second;

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

        const auto range =
            _main_tables.back().targets_ | std::views::transform( calc_avg );

        return std::make_pair(
            _input.first, stl::make::vector<Float>( range ) );
    };

    // -----------------------------------------------------------

    auto range = _rownum_map | std::views::transform( match_rownums ) |
                 std::views::filter( greater_than_min_df ) |
                 std::views::transform( calc_avg_targets );

    return std::make_shared<const std::map<Int, std::vector<Float>>>(
        range.begin(), range.end() );

    // -----------------------------------------------------------
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
    const WordIndexContainer& _word_indices )
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

    const auto ptr = transform_table_holder( _mapping, table_holder );

    assert_true( ptr );

    return *ptr;
}

// ----------------------------------------------------------------------------

typename MappingContainerMaker::MappedColumns
MappingContainerMaker::transform_categorical(
    const MappingForDf& _mapping, const std::vector<Column<Int>>& _categorical )
{
    const auto num_targets = infer_num_targets( _mapping );

    if ( num_targets == 0 )
        {
            return MappedColumns();
        }

    const auto transform_col = [num_targets, &_mapping, &_categorical](
                                   const size_t i ) -> Column<Float> {
        const auto colnum = i / num_targets;

        const auto target_num = i % num_targets;

        assert_true( colnum < _mapping.size() );

        assert_true( _mapping.at( colnum ) );

        const auto& mapping = *_mapping.at( colnum );

        const auto& cat_col = _categorical.at( colnum );

        auto vec = std::make_shared<std::vector<Float>>( cat_col.nrows_, NAN );

        for ( size_t i = 0; i < cat_col.nrows_; ++i )
            {
                const auto it = mapping.find( cat_col[i] );

                if ( it != mapping.end() )
                    {
                        assert_true( it->second.size() == num_targets );
                        vec->at( i ) = it->second.at( target_num );
                    }
            }

        return Column<Float>(
            vec,
            cat_col.name_ + "__mapping, target " +
                std::to_string( target_num + 1 ),
            vec->size(),
            "" );
    };

    assert_msg(
        _mapping.size() == _categorical.size(),
        "_mapping.size(): " + std::to_string( _mapping.size() ) +
            ", _categorical.size(): " + std::to_string( _categorical.size() ) );

    const auto range =
        std::views::iota(
            static_cast<size_t>( 0 ), _mapping.size() * num_targets ) |
        std::views::transform( transform_col );

    return stl::make::vector<Column<Float>>( range );
}

// ----------------------------------------------------------------------------

typename MappingContainerMaker::MappedColumns
MappingContainerMaker::transform_text(
    const MappingForDf& _mapping,
    const std::vector<Column<strings::String>>& _text,
    const typename DataFrame::WordIndices& _word_indices )
{
    const auto num_targets = infer_num_targets( _mapping );

    if ( num_targets == 0 )
        {
            return MappedColumns();
        }

    const auto transform_col = [num_targets, &_mapping, &_text, &_word_indices](
                                   const size_t i ) -> Column<Float> {
        const auto colnum = i / num_targets;

        const auto target_num = i % num_targets;

        assert_true( colnum < _mapping.size() );

        assert_true( _mapping.at( colnum ) );

        assert_true( _word_indices.at( colnum ) );

        const auto& mapping = *_mapping.at( colnum );

        const auto& word_index = *_word_indices.at( colnum );

        auto vec =
            std::make_shared<std::vector<Float>>( word_index.nrows(), NAN );

        for ( size_t i = 0; i < word_index.nrows(); ++i )
            {
                const auto range = word_index.range( i );

                for ( const auto word : range )
                    {
                        const auto it = mapping.find( word );

                        Float num_words = 0.0;

                        if ( it != mapping.end() )
                            {
                                assert_true( it->second.size() == num_targets );

                                ++num_words;

                                if ( std::isnan( vec->at( i ) ) )
                                    {
                                        vec->at( i ) =
                                            it->second.at( target_num );
                                    }
                                else
                                    {
                                        vec->at( i ) +=
                                            it->second.at( target_num );
                                    }
                            }

                        if ( num_words > 0.0 )
                            {
                                vec->at( i ) /= num_words;
                            }
                    }
            }

        return Column<Float>(
            vec,
            _text.at( i ).name_ + "__mapping, target " +
                std::to_string( target_num + 1 ),
            vec->size(),
            "" );
    };

    assert_msg(
        _text.size() == _word_indices.size(),
        "_text.size(): " + std::to_string( _text.size() ) +
            ", _word_index.size(): " + std::to_string( _word_indices.size() ) );

    assert_msg(
        _mapping.size() == _word_indices.size(),
        "_mapping.size(): " + std::to_string( _mapping.size() ) +
            ", _word_index.size(): " + std::to_string( _word_indices.size() ) );

    const auto range =
        std::views::iota(
            static_cast<size_t>( 0 ), _mapping.size() * num_targets ) |
        std::views::transform( transform_col );

    return stl::make::vector<Column<Float>>( range );
}

// ----------------------------------------------------------------------------

std::shared_ptr<const MappedContainer>
MappingContainerMaker::transform_table_holder(
    const std::shared_ptr<const MappingContainer>& _mapping,
    const TableHolder& _table_holder )
{
    assert_true( _mapping );

    assert_true(
        _mapping->categorical_.size() ==
        _table_holder.peripheral_tables_.size() );

    assert_true(
        _mapping->categorical_.size() == _mapping->subcontainers_.size() );

    assert_true( _mapping->categorical_.size() == _mapping->text_.size() );

    assert_true(
        _mapping->categorical_.size() == _table_holder.subtables_.size() );

    std::vector<MappedColumns> categorical;

    std::vector<MappedColumns> text;

    std::vector<std::shared_ptr<const MappedContainer>> subcontainers;

    for ( size_t i = 0; i < _mapping->categorical_.size(); ++i )
        {
            categorical.push_back( transform_categorical(
                _mapping->categorical_.at( i ),
                _table_holder.peripheral_tables_.at( i ).categoricals_ ) );

            if ( _table_holder.subtables_.at( i ) )
                {
                    assert_true( _mapping->subcontainers_.at( i ) );

                    subcontainers.push_back( transform_table_holder(
                        _mapping->subcontainers_.at( i ),
                        *_table_holder.subtables_.at( i ) ) );
                }
            else
                {
                    subcontainers.push_back( nullptr );
                }

            text.push_back( transform_text(
                _mapping->text_.at( i ),
                _table_holder.peripheral_tables_.at( i ).text_,
                _table_holder.peripheral_tables_.at( i ).word_indices_ ) );
        }

    return std::make_shared<MappedContainer>(
        categorical, subcontainers, text );
}

// ----------------------------------------------------------------------------
}  // namespace helpers
