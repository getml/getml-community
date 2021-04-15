#include "relboost/aggregations/aggregations.hpp"

namespace relboost
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
    if ( _agg == enums::Aggregation::avg_first_null )
        {
            update_etas(
                _divide_by_count,
                _indices_current,
                _eta1,
                _eta1_old,
                _eta2,
                _eta2_old,
                &eta2_1_null_,
                &w_fixed_2_ );

            return std::make_tuple(
                &eta2_1_null_,
                &eta2_1_null_old_,
                &w_fixed_2_,
                &w_fixed_2_old_ );
        }
    else if ( _agg == enums::Aggregation::avg_second_null )
        {
            update_etas(
                _divide_by_count,
                _indices_current,
                _eta1,
                _eta1_old,
                _eta2,
                _eta2_old,
                &eta1_2_null_,
                &w_fixed_1_ );

            return std::make_tuple(
                &eta1_2_null_,
                &eta1_2_null_old_,
                &w_fixed_1_,
                &w_fixed_1_old_ );
        }
    else
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
}

// ----------------------------------------------------------------------------

std::shared_ptr<std::vector<Float>>
IntermediateAggregationImpl::reduce_predictions(
    const bool _divide_by_count, const std::vector<Float>& _input_predictions )
{
    assert_true( eta1_.size() > 0 );

    auto counts = std::vector<Float>( agg_index().nrows() );

    const auto predictions =
        std::make_shared<std::vector<Float>>( agg_index().nrows() );

    for ( size_t i = 0; i < _input_predictions.size(); ++i )
        {
            const auto indices = agg_index().transform( i );

            for ( const auto ix : indices )
                {
                    assert_true( ix >= 0 );
                    assert_true(
                        static_cast<size_t>( ix ) < predictions->size() );

                    ( *predictions )[ix] += _input_predictions[i];
                    ++counts[ix];
                }
        }

    if ( _divide_by_count )
        {
            for ( size_t i = 0; i < predictions->size(); ++i )
                {
                    if ( counts[i] > 0.0 )
                        {
                            ( *predictions )[i] /= counts[i];
                        }
                }
        }

    return predictions;
}

// ----------------------------------------------------------------------------

void IntermediateAggregationImpl::reset( const bool _reset_child )
{
    for ( auto ix : indices_ )
        {
            assert_true( ix < eta1_.size() );

            eta1_[ix] = 0.0;
            eta1_2_null_[ix] = 0.0;
            eta1_2_null_old_[ix] = 0.0;
            eta1_old_[ix] = 0.0;
            eta2_[ix] = 0.0;
            eta2_1_null_[ix] = 0.0;
            eta2_1_null_old_[ix] = 0.0;
            eta2_old_[ix] = 0.0;
            w_fixed_1_[ix] = 0.0;
            w_fixed_1_old_[ix] = 0.0;
            w_fixed_2_[ix] = 0.0;
            w_fixed_2_old_[ix] = 0.0;
        }

#ifndef NDEBUG

    for ( size_t i = 0; i < eta1_.size(); ++i )
        {
            assert_true( eta1_[i] == 0.0 );
            assert_true( eta1_2_null_[i] == 0.0 );
            assert_true( eta1_2_null_old_[i] == 0.0 );
            assert_true( eta1_old_[i] == 0.0 );
            assert_true( eta2_[i] == 0.0 );
            assert_true( eta2_1_null_[i] == 0.0 );
            assert_true( eta2_1_null_old_[i] == 0.0 );
            assert_true( eta2_old_[i] == 0.0 );
            assert_true( w_fixed_1_[i] == 0.0 );
            assert_true( w_fixed_1_old_[i] == 0.0 );
            assert_true( w_fixed_2_[i] == 0.0 );
            assert_true( w_fixed_2_old_[i] == 0.0 );
        }

#endif

    indices_.clear();
    indices_current_.clear();

    assert_true( child_ );

    if ( _reset_child )
        {
            child_->reset();
        }
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
            for ( auto ix_input : _indices_current )
                {
                    const auto output_indices =
                        agg_index().transform( ix_input );

                    for ( auto ix_output : output_indices )
                        {
                            assert_true( ix_input < _eta1_input_old.size() );
                            assert_true( ix_output < _eta1_output->size() );

                            ( *_eta1_output )[ix_output] +=
                                ( _eta1_input[ix_input] -
                                  _eta1_input_old[ix_input] ) /
                                get_count( ix_output );

                            ( *_eta2_output )[ix_output] +=
                                ( _eta2_input[ix_input] -
                                  _eta2_input_old[ix_input] ) /
                                get_count( ix_output );

                            indices_.insert( ix_output );
                            indices_current_.insert( ix_output );
                        }
                }
        }
    else
        {
            for ( auto ix_input : _indices_current )
                {
                    const auto output_indices =
                        agg_index().transform( ix_input );

                    for ( auto ix_output : output_indices )
                        {
                            assert_true( ix_input < _eta1_input_old.size() );
                            assert_true( ix_output < _eta1_output->size() );

                            ( *_eta1_output )[ix_output] +=
                                _eta1_input[ix_input] -
                                _eta1_input_old[ix_input];

                            ( *_eta2_output )[ix_output] +=
                                _eta2_input[ix_input] -
                                _eta2_input_old[ix_input];

                            indices_.insert( ix_output );
                            indices_current_.insert( ix_output );
                        }
                }
        }
}

// ----------------------------------------------------------------------------

void IntermediateAggregationImpl::update_etas_old(
    const enums::Aggregation _agg )
{
    if ( _agg == enums::Aggregation::avg_first_null )
        {
            for ( auto ix : indices_current_ )
                {
                    assert_true( ix < eta1_.size() );
                    eta2_1_null_old_[ix] = eta2_1_null_[ix];
                    w_fixed_2_old_[ix] = w_fixed_2_[ix];
                }
        }
    else if ( _agg == enums::Aggregation::avg_second_null )
        {
            for ( auto ix : indices_current_ )
                {
                    assert_true( ix < eta1_.size() );
                    eta1_2_null_old_[ix] = eta1_2_null_[ix];
                    w_fixed_1_old_[ix] = w_fixed_1_[ix];
                }
        }
    else
        {
            for ( auto ix : indices_current_ )
                {
                    assert_true( ix < eta1_.size() );
                    eta1_old_[ix] = eta1_[ix];
                    eta2_old_[ix] = eta2_[ix];
                }
        }
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relboost
