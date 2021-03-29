#include "helpers/helpers.hpp"

namespace helpers
{
// ----------------------------------------------------------------------------

DataFrame TextFieldSplitter::add_rowid( const DataFrame& _df )
{
    auto range = std::views::iota(
        static_cast<Int>( 0 ), static_cast<Int>( _df.nrows() ) );

    const auto ptr =
        std::make_shared<std::vector<Int>>( stl::make::vector<Int>( range ) );

    const auto rowid =
        Column<Int>( ptr, helpers::Macros::rowid(), ptr->size(), "" );

    auto join_keys = _df.join_keys_;

    join_keys.push_back( rowid );

    auto indices = _df.indices_;

    indices.push_back( DataFrame::create_index( rowid ) );

    return DataFrame(
        _df.categoricals_,  // _categoricals
        _df.discretes_,     // _discretes
        indices,            // _indices
        join_keys,          // _join_keys
        _df.name_,          // _name
        _df.numericals_,    // _numericals
        _df.targets_,       // _targets
        _df.text_,          // _text
        _df.time_stamps_,   // _time_stamps
        _df.row_indices_,   // _row_indices
        _df.word_indices_   // _word_indices
    );
}

// ----------------------------------------------------------------------------

size_t TextFieldSplitter::count_text_fields(
    const DataFrame& _population_df,
    const std::vector<DataFrame>& _peripheral_dfs )
{
    auto num_text_fields = _population_df.num_text();

    for ( const auto& df : _peripheral_dfs )
        {
            num_text_fields += df.num_text();
        }

    return num_text_fields;
}

// ----------------------------------------------------------------------------

DataFrame TextFieldSplitter::make_new_df(
    const std::string& _df_name, const Column<strings::String>& _col )
{
    const auto [rownums, words] = split_text_fields_on_col( _col );

    return DataFrame(
        {},                                            // _categoricals
        {},                                            // _discretes
        { rownums },                                   // _join_keys
        _df_name + Macros::text_field() + _col.name_,  // _name
        {},                                            // _numericals
        {},                                            // _targets
        { words },                                     // _text
        {}                                             // _time_stamps
    );
}

// ----------------------------------------------------------------------------

DataFrame TextFieldSplitter::remove_text_fields( const DataFrame& _df )
{
    return DataFrame(
        _df.categoricals_,  // _categoricals
        _df.discretes_,     // _discretes
        _df.indices_,       // _indices
        _df.join_keys_,     // _join_keys
        _df.name_,          // _name
        _df.numericals_,    // _numericals
        _df.targets_,       // _targets
        {},                 // _text
        _df.time_stamps_,   // _time_stamps
        {},                 // _row_indices
        {}                  // _word_indices
    );
}

// ----------------------------------------------------------------------------

std::pair<DataFrame, std::vector<DataFrame>>
TextFieldSplitter::split_text_fields(
    const DataFrame& _population_df,
    const std::vector<DataFrame>& _peripheral_dfs,
    const std::shared_ptr<const logging::AbstractLogger>& _logger )
{
    const auto num_text_fields =
        count_text_fields( _population_df, _peripheral_dfs );

    if ( num_text_fields == 0 )
        {
            return std::make_pair( _population_df, _peripheral_dfs );
        }

    if ( _logger )
        {
            _logger->log( "Splitting text fields..." );
        }

    auto progress_logger = logging::ProgressLogger( _logger, num_text_fields );

    const auto modify_if_applicable = []( const DataFrame& _df ) -> DataFrame {
        return _df.num_text() == 0 ? _df
                                   : remove_text_fields( add_rowid( _df ) );
    };

    const auto population_df = modify_if_applicable( _population_df );

    auto range =
        _peripheral_dfs | std::views::transform( modify_if_applicable );

    auto peripheral_dfs = stl::make::vector<DataFrame>( range );

    split_text_fields_on_df(
        _population_df, &peripheral_dfs, &progress_logger );

    for ( const auto& df : _peripheral_dfs )
        {
            split_text_fields_on_df( df, &peripheral_dfs, &progress_logger );
        }

    return std::make_pair( population_df, peripheral_dfs );
}

// ----------------------------------------------------------------------------

std::pair<Column<Int>, Column<strings::String>>
TextFieldSplitter::split_text_fields_on_col(
    const Column<strings::String>& _col )
{
    const auto rownums_ptr = std::make_shared<std::vector<Int>>();

    const auto words_ptr = std::make_shared<std::vector<strings::String>>();

    for ( size_t i = 0; i < _col.nrows_; ++i )
        {
            const auto splitted =
                textmining::Vocabulary::split_text_field( _col[i] );

            for ( const auto& word : splitted )
                {
                    rownums_ptr->push_back( i );
                    words_ptr->push_back( strings::String( word ) );
                }
        }

    const auto rownums =
        Column<Int>( rownums_ptr, "rownums", rownums_ptr->size(), "" );

    const auto words =
        Column<strings::String>( words_ptr, "words", words_ptr->size(), "" );

    return std::make_pair( rownums, words );
}

// ----------------------------------------------------------------------------

void TextFieldSplitter::split_text_fields_on_df(
    const DataFrame& _df,
    std::vector<DataFrame>* _peripheral_dfs,
    logging::ProgressLogger* _progress_logger )
{
    const auto add_df =
        [&_df]( const Column<strings::String>& _col ) -> DataFrame {
        const auto df = make_new_df( _df.name(), _col );
        return df;
    };

    auto range = _df.text_ | std::views::transform( add_df );

    for ( auto df : range )
        {
            _peripheral_dfs->push_back( df );
            _progress_logger->increment();
        }
}

// ----------------------------------------------------------------------------
}  // namespace helpers
