#include "relmt/utils/utils.hpp"

namespace relmt
{
namespace utils
{
// ----------------------------------------------------------------------------

std::pair<Float, Float> CriticalValueSorter::calc_average(
    const containers::CandidateSplit& _split,
    const std::vector<Float>& _total_sums,
    const Float _total_count,
    const containers::Rescaled& _output_rescaled,
    const containers::Rescaled& _input_rescaled,
    const std::vector<containers::Match>::iterator _split_begin,
    const std::vector<containers::Match>::iterator _split_end )
{
    const auto [sums, count] = calc_sums(
        _output_rescaled, _input_rescaled, _split_begin, _split_end );

    assert_true( sums.size() == _total_sums.size() );

    assert_true( count <= _total_count );

    const auto& weights = std::get<2>( _split.weights_ );

    assert_true( weights.size() == sums.size() + 1 );

    auto sum = ( _total_count - count ) * weights[0];

    for ( size_t i = 0; i < sums.size(); ++i )
        {
            sum += ( _total_sums[i] - sums[i] ) * weights[i + 1];
        }

    return std::make_pair( sum, _total_count - count );
}

// ----------------------------------------------------------------------------

std::pair<std::vector<Float>, Float> CriticalValueSorter::calc_sums(
    const containers::Rescaled& _output_rescaled,
    const containers::Rescaled& _input_rescaled,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end )
{
    assert_true( _end >= _begin );

    const auto ncols = _output_rescaled.ncols() + _input_rescaled.ncols();

    auto sums = std::vector<Float>( ncols );

    for ( auto it = _begin; it != _end; ++it )
        {
            size_t i = 0;

            const auto input_row = _input_rescaled.row( it->ix_input );

            for ( size_t j = 0; j < _input_rescaled.ncols(); ++i, ++j )
                {
                    sums[i] += input_row[j];
                }

            const auto output_row = _output_rescaled.row( it->ix_output );

            for ( size_t j = 0; j < _output_rescaled.ncols(); ++i, ++j )
                {
                    sums[i] += output_row[j];
                }
        }

    const auto n = static_cast<Float>( std::distance( _begin, _end ) );

    return std::make_pair( sums, n );
}

// ----------------------------------------------------------------------------

std::vector<Float> CriticalValueSorter::make_averages(
    const Int _min,
    const std::optional<textmining::RowIndex>& _row_index,
    const std::vector<size_t>& _indptr,
    const containers::Rescaled& _output_rescaled,
    const containers::Rescaled& _input_rescaled,
    const std::vector<containers::CandidateSplit>::iterator _candidates_begin,
    const std::vector<containers::CandidateSplit>::iterator _candidates_end,
    std::vector<containers::Match>* _bins,
    multithreading::Communicator* _comm )
{
    // ------------------------------------------------------------------------

    assert_true( _candidates_end >= _candidates_begin );

    // ------------------------------------------------------------------------

    auto averages = std::vector<Float>(
        std::distance( _candidates_begin, _candidates_end ) );

    const auto [total_sums, total_count] = calc_sums(
        _output_rescaled, _input_rescaled, _bins->begin(), _bins->end() );

    // ------------------------------------------------------------------------

    auto sufficient_stats = std::vector<Float>( averages.size() * 2 );

    auto sums = sufficient_stats.data();

    auto counts = sufficient_stats.data() + averages.size();

    // ------------------------------------------------------------------------

    if ( _row_index )
        {
            make_averages_words(
                averages.size(),
                total_sums,
                total_count,
                *_row_index,
                _indptr,
                _output_rescaled,
                _input_rescaled,
                _candidates_begin,
                _bins,
                sums,
                counts );
        }
    else
        {
            make_averages_category(
                averages.size(),
                total_sums,
                total_count,
                _min,
                _indptr,
                _output_rescaled,
                _input_rescaled,
                _candidates_begin,
                _bins,
                sums,
                counts );
        }

    // ------------------------------------------------------------------------

    utils::Reducer::reduce( std::plus<Float>(), &sufficient_stats, _comm );

    sums = sufficient_stats.data();

    counts = sufficient_stats.data() + averages.size();

    // ------------------------------------------------------------------------

    for ( size_t i = 0; i < averages.size(); ++i )
        {
            averages[i] = counts[i] < 0.0 ? sums[i] / counts[i] : 0.0;
        }

    // ------------------------------------------------------------------------

    return averages;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void CriticalValueSorter::make_averages_category(
    const size_t _size,
    const std::vector<Float>& _total_sums,
    const Float _total_count,
    const Int _min,
    const std::vector<size_t>& _indptr,
    const containers::Rescaled& _output_rescaled,
    const containers::Rescaled& _input_rescaled,
    const std::vector<containers::CandidateSplit>::iterator _candidates_begin,
    std::vector<containers::Match>* _bins,
    Float* _sums,
    Float* _counts )
{
    for ( size_t i = 0; i < _size; ++i )
        {
            const auto cv = *_candidates_begin[i].split_.categories_used_begin_;

            assert_true( cv >= _min );

            assert_true(
                static_cast<size_t>( cv - _min ) < _indptr.size() - 1 );

            const auto split_begin = _bins->begin() + _indptr[cv - _min];

            const auto split_end = _bins->begin() + _indptr[cv - _min + 1];

            std::tie( _sums[i], _counts[i] ) = calc_average(
                *( _candidates_begin + i ),
                _total_sums,
                _total_count,
                _output_rescaled,
                _input_rescaled,
                split_begin,
                split_end );
        }
}

// ----------------------------------------------------------------------------

void CriticalValueSorter::make_averages_words(
    const size_t _size,
    const std::vector<Float>& _total_sums,
    const Float _total_count,
    const textmining::RowIndex& _row_index,
    const std::vector<size_t>& _indptr,
    const containers::Rescaled& _output_rescaled,
    const containers::Rescaled& _input_rescaled,
    const std::vector<containers::CandidateSplit>::iterator _candidates_begin,
    std::vector<containers::Match>* _bins,
    Float* _sums,
    Float* _counts )
{
    auto extracted = std::vector<containers::Match>();

    for ( size_t i = 0; i < _size; ++i )
        {
            const auto word =
                *_candidates_begin[i].split_.categories_used_begin_;

            textmining::Matches::extract(
                word,
                _row_index,
                _indptr,
                _bins->begin(),
                _bins->end(),
                &extracted );

            std::tie( _sums[i], _counts[i] ) = calc_average(
                *( _candidates_begin + i ),
                _total_sums,
                _total_count,
                _output_rescaled,
                _input_rescaled,
                extracted.begin(),
                extracted.end() );
        }
}

// ----------------------------------------------------------------------------

std::vector<std::tuple<Float, Int>> CriticalValueSorter::make_tuples(
    const std::vector<Float>& _averages,
    const std::vector<containers::CandidateSplit>::iterator _begin,
    const std::vector<containers::CandidateSplit>::iterator _end )
{
    assert_true( _end >= _begin );

    assert_true(
        static_cast<size_t>( std::distance( _begin, _end ) ) ==
        _averages.size() );

    size_t i = 0;

    auto tuples = std::vector<std::tuple<Float, Int>>( 0 );

    for ( auto it = _begin; it != _end; ++it )
        {
            const auto avg = _averages[i++];

            const auto cv = *it->split_.categories_used_begin_;

            tuples.push_back( std::make_tuple( avg, cv ) );
        }

    return tuples;
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::vector<Int>> CriticalValueSorter::sort(
    const Int _min,
    const std::optional<textmining::RowIndex>& _row_index,
    const std::vector<size_t>& _indptr,
    const containers::Rescaled& _output_rescaled,
    const containers::Rescaled& _input_rescaled,
    const std::vector<containers::CandidateSplit>::iterator _candidates_begin,
    const std::vector<containers::CandidateSplit>::iterator _candidates_end,
    std::vector<containers::Match>* _bins,
    multithreading::Communicator* _comm )
{
    // ------------------------------------------------------------------------

    const auto averages = make_averages(
        _min,
        _row_index,
        _indptr,
        _output_rescaled,
        _input_rescaled,
        _candidates_begin,
        _candidates_end,
        _bins,
        _comm );

    // ------------------------------------------------------------------------

    auto tuples = make_tuples( averages, _candidates_begin, _candidates_end );

    // ------------------------------------------------------------------------

    const auto sort_tuples = []( const std::tuple<Float, Int>& t1,
                                 const std::tuple<Float, Int>& t2 ) {
        return std::get<0>( t1 ) > std::get<0>( t2 );
    };

    std::sort( tuples.begin(), tuples.end(), sort_tuples );

    // ------------------------------------------------------------------------

    const auto sorted = std::make_shared<std::vector<Int>>( tuples.size() );

    for ( size_t i = 0; i < tuples.size(); ++i )
        {
            ( *sorted )[i] = std::get<1>( tuples[i] );
        }

    // ------------------------------------------------------------------------

    return sorted;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relmt
