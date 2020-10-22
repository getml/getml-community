#include "relmt/aggregations/aggregations.hpp"

namespace relmt
{
namespace aggregations
{
// ----------------------------------------------------------------------------

std::tuple<
    const std::vector<Float>*,
    const std::vector<Float>*,
    const std::vector<Float>*,
    const std::vector<Float>*>
IntermediateAggregationImpl::calc_etas(
    const bool _divide_by_count,
    const enums::Aggregation _agg,
    const std::vector<size_t>& _indices_current,
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta1_old,
    const std::vector<Float>& _eta2,
    const std::vector<Float>& _eta2_old )

{
    update_etas(
        _divide_by_count,
        _indices_current,
        _eta1,
        _eta1_old,
        _eta2,
        _eta2_old,
        &eta1_,
        &eta2_ );

    return std::make_tuple( &eta1_, &eta1_old_, &eta2_, &eta2_old_ );
}

// ----------------------------------------------------------------------------

std::pair<Float, containers::Weights> IntermediateAggregationImpl::calc_pair(
    const bool _divide_by_count,
    const enums::Aggregation _agg,
    const enums::Revert _revert,
    const enums::Update _update,
    const std::vector<Float>& _old_weights,
    const std::vector<size_t>& _indices,
    const std::vector<size_t>& _indices_current,
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta1_old,
    const std::vector<Float>& _eta2,
    const std::vector<Float>& _eta2_old )
{
    if ( ncols_ != _old_weights.size() )
        {
            resize( _old_weights.size() );
        }

    const auto [eta1, eta1_old, eta2, eta2_old] = calc_etas(
        _divide_by_count,
        _agg,
        _indices_current,
        _eta1,
        _eta1_old,
        _eta2,
        _eta2_old );

    const auto result = child_->calc_pair(
        _agg,
        _revert,
        _update,
        _old_weights,
        indices(),
        indices_current(),
        *eta1,
        *eta1_old,
        *eta2,
        *eta2_old );

    update_etas_old( _agg );

    return result;
}

// ----------------------------------------------------------------------------

std::vector<Float> IntermediateAggregationImpl::reduce_predictions(
    const bool _divide_by_count, const std::vector<Float>& _input_predictions )
{
    assert_true( eta1_.size() > 0 );

    auto counts = std::vector<Float>( agg_index().nrows() );

    auto predictions = std::vector<Float>( agg_index().nrows() );

    for ( size_t i = 0; i < _input_predictions.size(); ++i )
        {
            const auto indices = agg_index().transform( i );

            for ( const auto ix : indices )
                {
                    assert_true( ix >= 0 );
                    assert_true(
                        static_cast<size_t>( ix ) < predictions.size() );

                    predictions[ix] += _input_predictions[i];
                    ++counts[ix];
                }
        }

    if ( _divide_by_count )
        {
            for ( size_t i = 0; i < predictions.size(); ++i )
                {
                    if ( counts[i] > 0.0 )
                        {
                            predictions[i] /= counts[i];
                        }
                }
        }

    return predictions;
}

// ----------------------------------------------------------------------------

void IntermediateAggregationImpl::reset( const bool _reset_child )
{
    assert_true( eta1_.size() == ncols() * nrows() );
    assert_true( eta1_.size() == eta2_.size() );
    assert_true( eta1_.size() == eta1_old_.size() );
    assert_true( eta1_.size() == eta2_old_.size() );

    for ( auto i : indices_ )
        {
            assert_true( i < nrows() );

            const auto begin = i * ncols();
            const auto end = ( i + 1 ) * ncols();

            std::fill( eta1_.data() + begin, eta1_.data() + end, 0.0 );
            std::fill( eta1_old_.data() + begin, eta1_old_.data() + end, 0.0 );

            std::fill( eta2_.data() + begin, eta2_.data() + end, 0.0 );
            std::fill( eta2_old_.data() + begin, eta2_old_.data() + end, 0.0 );
        }

    indices_.clear();
    indices_current_.clear();

    assert_true( child_ );

    if ( _reset_child )
        {
            child_->reset();
        }
}

// ----------------------------------------------------------------------------

void IntermediateAggregationImpl::resize( const size_t _ncols )
{
    ncols_ = _ncols;

    eta1_ = std::vector<Float>( nrows() * ncols() );
    eta1_old_ = std::vector<Float>( nrows() * ncols() );

    eta2_ = std::vector<Float>( nrows() * ncols() );
    eta2_old_ = std::vector<Float>( nrows() * ncols() );
}

// ----------------------------------------------------------------------------

void IntermediateAggregationImpl::update_etas(
    const bool _divide_by_count,
    const std::vector<size_t>& _indices_current,
    const std::vector<Float>& _eta1_input,
    const std::vector<Float>& _eta1_input_old,
    const std::vector<Float>& _eta2_input,
    const std::vector<Float>& _eta2_input_old,
    std::vector<Float>* _eta1_output,
    std::vector<Float>* _eta2_output )
{
    indices_current_.clear();

    if ( _divide_by_count )
        {
            update_etas_divide_by_count(
                _indices_current,
                _eta1_input,
                _eta1_input_old,
                _eta2_input,
                _eta2_input_old,
                _eta1_output,
                _eta2_output );
        }
    else
        {
            update_etas_dont_divide(
                _indices_current,
                _eta1_input,
                _eta1_input_old,
                _eta2_input,
                _eta2_input_old,
                _eta1_output,
                _eta2_output );
        }
}

// ----------------------------------------------------------------------------

void IntermediateAggregationImpl::update_etas_divide_by_count(
    const std::vector<size_t>& _indices_current,
    const std::vector<Float>& _eta1_input,
    const std::vector<Float>& _eta1_input_old,
    const std::vector<Float>& _eta2_input,
    const std::vector<Float>& _eta2_input_old,
    std::vector<Float>* _eta1_output,
    std::vector<Float>* _eta2_output )
{
    assert_true( _eta1_output->size() == nrows() * ncols() );
    assert_true( _eta1_output->size() == _eta2_output->size() );

    assert_true( _eta1_input.size() == _eta2_input.size() );
    assert_true( _eta1_input.size() == _eta1_input_old.size() );
    assert_true( _eta1_input.size() == _eta2_input_old.size() );
    assert_true( _eta1_input.size() % ncols() == 0 );

    const auto nrows_input = _eta1_input.size() / ncols();

    for ( auto ix_input : _indices_current )
        {
            const auto output_indices = agg_index().transform( ix_input );

            for ( auto ix_output : output_indices )
                {
                    assert_true( ix_input < nrows_input );
                    assert_true( ix_output < nrows() );

                    const auto count = get_count( ix_output );

                    for ( size_t j = 0; j < ncols(); ++j )
                        {
                            const auto ix1 = ix_input * ncols() + j;
                            const auto ix2 = ix_output * ncols() + j;

                            ( *_eta1_output )[ix2] +=
                                ( _eta1_input[ix1] - _eta1_input_old[ix1] ) /
                                count;
                        }

                    for ( size_t j = 0; j < ncols(); ++j )
                        {
                            const auto ix1 = ix_input * ncols() + j;
                            const auto ix2 = ix_output * ncols() + j;

                            ( *_eta2_output )[ix2] +=
                                ( _eta2_input[ix1] - _eta2_input_old[ix1] ) /
                                count;
                        }

                    indices_.insert( ix_output );
                    indices_current_.insert( ix_output );
                }
        }
}

// ----------------------------------------------------------------------------

void IntermediateAggregationImpl::update_etas_dont_divide(
    const std::vector<size_t>& _indices_current,
    const std::vector<Float>& _eta1_input,
    const std::vector<Float>& _eta1_input_old,
    const std::vector<Float>& _eta2_input,
    const std::vector<Float>& _eta2_input_old,
    std::vector<Float>* _eta1_output,
    std::vector<Float>* _eta2_output )
{
    assert_true( _eta1_output->size() == nrows() * ncols() );
    assert_true( _eta1_output->size() == _eta2_output->size() );

    assert_true( _eta1_input.size() == _eta2_input.size() );
    assert_true( _eta1_input.size() == _eta1_input_old.size() );
    assert_true( _eta1_input.size() == _eta2_input_old.size() );
    assert_true( _eta1_input.size() % ncols() == 0 );

    const auto nrows_input = _eta1_input.size() / ncols();

    for ( auto ix_input : _indices_current )
        {
            const auto output_indices = agg_index().transform( ix_input );

            for ( auto ix_output : output_indices )
                {
                    assert_true( ix_input < nrows_input );
                    assert_true( ix_output < nrows() );

                    for ( size_t j = 0; j < ncols(); ++j )
                        {
                            const auto ix1 = ix_input * ncols() + j;
                            const auto ix2 = ix_output * ncols() + j;

                            ( *_eta1_output )[ix2] +=
                                ( _eta1_input[ix1] - _eta1_input_old[ix1] );
                        }

                    for ( size_t j = 0; j < ncols(); ++j )
                        {
                            const auto ix1 = ix_input * ncols() + j;
                            const auto ix2 = ix_output * ncols() + j;

                            ( *_eta2_output )[ix2] +=
                                ( _eta2_input[ix1] - _eta2_input_old[ix1] );
                        }

                    indices_.insert( ix_output );
                    indices_current_.insert( ix_output );
                }
        }
}

// ----------------------------------------------------------------------------

void IntermediateAggregationImpl::update_etas_old(
    const enums::Aggregation _agg )
{
    assert_true( eta1_.size() == ncols() * nrows() );
    assert_true( eta1_.size() == eta2_.size() );
    assert_true( eta1_.size() == eta1_old_.size() );
    assert_true( eta1_.size() == eta2_old_.size() );

    for ( auto i : indices_current_ )
        {
            assert_true( i < nrows() );

            for ( size_t j = 0; j < ncols(); ++j )
                {
                    const auto ix = i * ncols() + j;
                    eta1_old_[ix] = eta1_[ix];
                }

            for ( size_t j = 0; j < ncols(); ++j )
                {
                    const auto ix = i * ncols() + j;
                    eta2_old_[ix] = eta2_[ix];
                }
        }
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relmt
