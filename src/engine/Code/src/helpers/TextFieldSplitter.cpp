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

    const auto rowid = Column<Int>( ptr, helpers::Macros::rowid(), "" );

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

VocabularyContainer TextFieldSplitter::reverse(
    const VocabularyContainer& _vocab,
    const Placeholder& _population_schema,
    const std::vector<Placeholder>& _peripheral_schema )
{
    // -------------------------------------------------------------------------

    const auto add_num_text = []( const size_t _init,
                                  const Placeholder& _p ) -> size_t {
        return _init + _p.text_.size();
    };

    // -------------------------------------------------------------------------

    const auto accumulate = [_peripheral_schema,
                             add_num_text]( const Int _i ) -> size_t {
        assert_true( _i >= 0 );

        assert_true( static_cast<size_t>( _i ) <= _peripheral_schema.size() );

        return std::accumulate(
            _peripheral_schema.begin(),
            _peripheral_schema.begin() + _i,
            static_cast<size_t>( 0 ),
            add_num_text );
    };

    // -------------------------------------------------------------------------

    const auto infer_begin_end =
        [&_population_schema, &_peripheral_schema, accumulate](
            const Int _i ) -> std::pair<size_t, size_t> {
        const auto init =
            _peripheral_schema.size() + _population_schema.text_.size();

        if ( _i < 0 )
            {
                return std::make_pair( _peripheral_schema.size(), init );
            }

        const auto begin = init + accumulate( _i );

        const auto end = init + accumulate( _i + 1 );

        return std::make_pair( begin, end );
    };

    // -------------------------------------------------------------------------

    using VocabForDf = VocabularyContainer::VocabForDf;

    const auto extract_vocab = [&_vocab,
                                infer_begin_end]( const Int _i ) -> VocabForDf {
        const auto [begin, end] = infer_begin_end( _i );

        assert_true( begin < _vocab.peripheral().size() || begin == end );

        assert_true( end <= _vocab.peripheral().size() );

        VocabForDf result;

        for ( size_t i = begin; i < end; ++i )
            {
                assert_true( _vocab.peripheral().at( i ).size() == 1 );
                result.push_back( _vocab.peripheral().at( i ).at( 0 ) );
            }

        return result;
    };

    // -------------------------------------------------------------------------

    const auto population = extract_vocab( -1 );

    const auto size = static_cast<Int>( _peripheral_schema.size() );

    auto peripheral = std::vector<VocabForDf>();

    for ( Int i = 0; i < size; ++i )
        {
            peripheral.push_back( extract_vocab( i ) );
        }

    // -------------------------------------------------------------------------

    return VocabularyContainer( population, peripheral );

    // -------------------------------------------------------------------------
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

    auto progress_logger = logging::ProgressLogger(
        "Splitting text fields...", _logger, num_text_fields );

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

    const auto rownums = Column<Int>( rownums_ptr, "rownum", "" );

    const auto words = Column<strings::String>( words_ptr, "word", "" );

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
