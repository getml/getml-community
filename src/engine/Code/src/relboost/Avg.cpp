#include "relboost/aggregations/aggregations.hpp"

namespace relboost
{
namespace aggregations
{
// ----------------------------------------------------------------------------

void Avg::activate(
    const containers::IntSet::Iterator _begin,
    const containers::IntSet::Iterator _end )
{
    for ( auto it = _begin; it != _end; ++it )
        {
            const auto ix = *it;

            assert_true( count_committed_[ix] >= 0.0 );
            assert_true( count1_[ix] >= 0.0 );
            assert_true( count2_[ix] >= 0.0 );

            if ( count_committed_[ix] + count1_[ix] == 0.0 )
                {
                    eta1_2_null_[ix] = w_fixed_1_[ix] = 0.0;
                }
            else
                {
                    assert_true( !std::isnan( w_fixed_committed_[ix] ) );
                    assert_true( count_committed_[ix] + count1_[ix] > 0.5 );

                    eta1_2_null_[ix] =
                        count1_[ix] / ( count_committed_[ix] + count1_[ix] );

                    w_fixed_1_[ix] = w_fixed_committed_[ix] *
                                     count_committed_[ix] /
                                     ( count_committed_[ix] + count1_[ix] );

                    assert_true( !std::isnan( eta1_2_null_[ix] ) );
                    assert_true( !std::isnan( w_fixed_1_[ix] ) );
                }

            if ( count_committed_[ix] + count2_[ix] == 0.0 )
                {
                    eta2_1_null_[ix] = w_fixed_2_[ix] = 0.0;
                }
            else
                {
                    assert_true( !std::isnan( w_fixed_committed_[ix] ) );
                    assert_true( count_committed_[ix] + count2_[ix] > 0.5 );

                    eta2_1_null_[ix] =
                        count2_[ix] / ( count_committed_[ix] + count2_[ix] );

                    w_fixed_2_[ix] = w_fixed_committed_[ix] *
                                     count_committed_[ix] /
                                     ( count_committed_[ix] + count2_[ix] );

                    assert_true( !std::isnan( eta2_1_null_[ix] ) );
                    assert_true( !std::isnan( w_fixed_2_[ix] ) );
                }
        }
}

// ----------------------------------------------------------------------------

void Avg::calc_all(
    const enums::Revert _revert,
    const Float _old_weight,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _split_begin,
    const std::vector<containers::Match>::iterator _split_end,
    const std::vector<containers::Match>::iterator _end )
{
    // ------------------------------------------------------------------------

    assert_true( eta1_.size() == eta2_.size() );
    assert_true( eta1_.size() == eta_old_.size() );
    assert_true( eta1_.size() == count_committed_.size() );
    assert_true( eta1_.size() == w_fixed_1_.size() );
    assert_true( eta1_.size() == w_fixed_2_.size() );

    assert_true( indices_.size() == 0 );
    assert_true( indices_current_.size() == 0 );

    update_ = enums::Update::calc_all;

    // ------------------------------------------------------------------------

#ifndef NDEBUG

    for ( auto val : count1_ )
        {
            assert_true( val == 0.0 );
        }

    for ( auto val : count2_ )
        {
            assert_true( val == 0.0 );
        }

#endif  // NDEBUG

    // ------------------------------------------------------------------------

    num_samples_1_ = 0.0;

    num_samples_2_ = 0.0;

    // ------------------------------------------------------------------------
    // Calculate eta1_, eta2_, eta_old_, count1_ and count2_.

    if ( !std::isnan( _old_weight ) )
        {
            for ( auto it = _begin; it != _split_begin; ++it )
                {
                    const auto ix = it->ix_output;

                    assert_true( count_committed_[ix] > 0.0 );

                    eta2_[ix] += 1.0 / count_committed_[ix];

                    ++count2_[ix];

                    ++num_samples_2_;

                    indices_.insert( ix );
                    indices_current_.insert( ix );
                }

            for ( auto it = _split_begin; it != _split_end; ++it )
                {
                    const auto ix = it->ix_output;

                    assert_true( count_committed_[ix] > 0.0 );

                    eta1_[ix] += 1.0 / count_committed_[ix];

                    ++count1_[ix];

                    ++num_samples_1_;

                    indices_.insert( ix );
                    indices_current_.insert( ix );
                }

            for ( auto it = _split_end; it != _end; ++it )
                {
                    const auto ix = it->ix_output;

                    assert_true( count_committed_[ix] > 0.0 );

                    eta2_[ix] += 1.0 / count_committed_[ix];

                    ++count2_[ix];

                    ++num_samples_2_;

                    indices_.insert( ix );
                    indices_current_.insert( ix );
                }

            for ( auto ix : indices_ )
                {
                    eta_old_[ix] = count1_[ix] + count2_[ix];
                }
        }
    else
        {
            for ( auto it = _begin; it != _split_begin; ++it )
                {
                    const auto ix = it->ix_output;

                    ++count2_[ix];

                    ++num_samples_2_;

                    indices_.insert( ix );
                    indices_current_.insert( ix );
                }

            for ( auto it = _split_begin; it != _split_end; ++it )
                {
                    const auto ix = it->ix_output;

                    ++count1_[ix];

                    ++num_samples_1_;

                    indices_.insert( ix );
                    indices_current_.insert( ix );
                }

            for ( auto it = _split_end; it != _end; ++it )
                {
                    const auto ix = it->ix_output;

                    ++count2_[ix];

                    ++num_samples_2_;

                    indices_.insert( ix );
                    indices_current_.insert( ix );
                }

            for ( auto ix : indices_ )
                {
                    eta_old_[ix] = 0.0;
                }
        }

    // ------------------------------------------------------------------------
    // Calculate eta1_2_null_, eta2_1_null_, w_fixed_1_ and w_fixed_2_.

    if ( allow_null_weights_ )
        {
            if ( std::isnan( _old_weight ) )
                {
                    activate( indices_.begin(), indices_.end() );
                }
            else
                {
                    deactivate( _old_weight, indices_.begin(), indices_.end() );
                }
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Avg::calc_diff(
    const Float _old_weight,
    const std::vector<containers::Match>::iterator _split_begin,
    const std::vector<containers::Match>::iterator _split_end )
{
    // ------------------------------------------------------------------------

    assert_true( _split_end >= _split_begin );

    // ------------------------------------------------------------------------

    if ( !std::isnan( _old_weight ) )
        {
            for ( auto it = _split_begin; it != _split_end; ++it )
                {
                    const auto ix = it->ix_output;

                    assert_true( ix < eta1_.size() );

                    assert_true( count_committed_[ix] > 0.0 );

                    eta1_[ix] += 1.0 / count_committed_[ix];
                    eta2_[ix] -= 1.0 / count_committed_[ix];

                    ++count1_[ix];
                    --count2_[ix];

                    assert_true( count2_[ix] >= 0.0 );

                    indices_current_.insert( ix );
                }
        }
    else
        {
            for ( auto it = _split_begin; it != _split_end; ++it )
                {
                    const auto ix = it->ix_output;

                    assert_true( ix < eta1_.size() );

                    ++count1_[ix];
                    --count2_[ix];

                    assert_true( count2_[ix] >= 0.0 );

                    indices_current_.insert( ix );
                }
        }

    // ------------------------------------------------------------------------

    const auto dist =
        static_cast<Float>( std::distance( _split_begin, _split_end ) );

    num_samples_1_ += dist;
    num_samples_2_ -= dist;

    // ------------------------------------------------------------------------

    if ( allow_null_weights_ )
        {
            if ( std::isnan( _old_weight ) )
                {
                    activate(
                        indices_current_.begin(), indices_current_.end() );
                }
            else
                {
                    deactivate(
                        _old_weight,
                        indices_current_.begin(),
                        indices_current_.end() );
                }
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Avg::calc_etas(
    const enums::Aggregation _agg,
    const enums::Update _update,
    const Float _old_weight,
    const std::vector<size_t>& _indices_current,
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta1_old,
    const std::vector<Float>& _eta2,
    const std::vector<Float>& _eta2_old )
{
    const auto [eta1, eta1_old, eta2, eta2_old] = intermediate_agg().calc_etas(
        true, _agg, _indices_current, _eta1, _eta1_old, _eta2, _eta2_old );

    child_->calc_etas(
        _agg,
        _update,
        _old_weight,
        intermediate_agg().indices_current(),
        *eta1,
        *eta1_old,
        *eta2,
        *eta2_old );

    intermediate_agg().update_etas_old( _agg );
}

// ----------------------------------------------------------------------------

std::pair<Float, std::array<Float, 3>> Avg::calc_pair(
    const enums::Aggregation _agg,
    const enums::Revert _revert,
    const enums::Update _update,
    const Float _old_weight,
    const std::vector<size_t>& _indices,
    const std::vector<size_t>& _indices_current,
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta1_old,
    const std::vector<Float>& _eta2,
    const std::vector<Float>& _eta2_old )

{
    const auto [eta1, eta1_old, eta2, eta2_old] = intermediate_agg().calc_etas(
        true, _agg, _indices_current, _eta1, _eta1_old, _eta2, _eta2_old );

    const auto weights = child_->calc_pair(
        _agg,
        _revert,
        _update,
        _old_weight,
        intermediate_agg().indices(),
        intermediate_agg().indices_current(),
        *eta1,
        *eta1_old,
        *eta2,
        *eta2_old );

    intermediate_agg().update_etas_old( _agg );

    return weights;
}

// ----------------------------------------------------------------------------

std::vector<std::pair<Float, std::array<Float, 3>>> Avg::calc_pairs(
    const enums::Revert _revert,
    const enums::Update _update,
    const Float _min_num_samples,
    const Float _old_intercept,
    const Float _old_weight,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _split_begin,
    const std::vector<containers::Match>::iterator _split_end,
    const std::vector<containers::Match>::iterator _end )
{
    // -------------------------------------------------------------

    assert_true( eta1_.size() == eta2_.size() );
    assert_true( eta1_.size() == count_committed_.size() );

    debug_log(
        "std::distance(_begin, _split_begin): " +
        std::to_string( std::distance( _begin, _split_begin ) ) );

    debug_log(
        "std::distance(_split_begin, _split_end): " +
        std::to_string( std::distance( _split_begin, _split_end ) ) );

    debug_log(
        "std::distance(_split_begin, _split_end): " +
        std::to_string( std::distance( _split_end, _end ) ) );

    // -------------------------------------------------------------

    switch ( _update )
        {
            case enums::Update::calc_all:
                update_ = enums::Update::calc_all;
                calc_all(
                    _revert,
                    _old_weight,
                    _begin,
                    _split_begin,
                    _split_end,
                    _end );
                break;

            case enums::Update::calc_diff:
                calc_diff( _old_weight, _split_begin, _split_end );
                break;

            default:
                assert_true( false && "Unknown Update!" );
        }

    // -------------------------------------------------------------

    auto results = std::vector<std::pair<Float, std::array<Float, 3>>>();

    // -------------------------------------------------------------

    if ( !impl_.is_balanced(
             num_samples_1_, num_samples_2_, _min_num_samples, comm_ ) )
        {
            return results;
        }

    // -------------------------------------------------------------

    assert_true( !allow_null_weights_ || !std::isnan( _old_weight ) );

    if ( !std::isnan( _old_weight ) )
        {
            results.push_back( child_->calc_pair(
                enums::Aggregation::avg,
                _revert,
                update_,
                _old_weight,
                indices_.unique_integers(),
                indices_current_.unique_integers(),
                eta1_,
                eta1_old_,
                eta2_,
                eta2_old_ ) );
        }

    if ( allow_null_weights_ )
        {
            results.push_back( child_->calc_pair(
                enums::Aggregation::avg_second_null,
                _revert,
                update_,
                _old_weight,
                indices_.unique_integers(),
                indices_current_.unique_integers(),
                eta1_2_null_,
                eta1_2_null_old_,
                w_fixed_1_,
                w_fixed_2_old_ ) );

            results.push_back( child_->calc_pair(
                enums::Aggregation::avg_first_null,
                _revert,
                update_,
                _old_weight,
                indices_.unique_integers(),
                indices_current_.unique_integers(),
                eta2_1_null_,
                eta2_1_null_old_,
                w_fixed_2_,
                w_fixed_2_old_ ) );
        }

    update_etas_old( _old_weight );

    update_ = enums::Update::calc_diff;

    // -------------------------------------------------------------

    if ( _revert == enums::Revert::False )
        {
            indices_current_.clear();
        }

    // -------------------------------------------------------------

    return results;

    // -------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Avg::calc_yhat(
    const Float _old_weight, const std::array<Float, 3>& _new_weights )
{
    assert_true( !std::isnan( std::get<0>( _new_weights ) ) );

    if ( std::isnan( std::get<2>( _new_weights ) ) )
        {
            assert_true( !std::isnan( std::get<1>( _new_weights ) ) );

            child_->calc_yhat(
                enums::Aggregation::avg_second_null,
                _old_weight,
                _new_weights,
                indices_.unique_integers(),
                eta1_2_null_,
                eta1_2_null_old_,
                w_fixed_1_,
                w_fixed_1_old_ );
        }
    else if ( std::isnan( std::get<1>( _new_weights ) ) )
        {
            assert_true( !std::isnan( std::get<2>( _new_weights ) ) );

            child_->calc_yhat(
                enums::Aggregation::avg_first_null,
                _old_weight,
                _new_weights,
                indices_.unique_integers(),
                eta2_1_null_,
                eta2_1_null_old_,
                w_fixed_2_,
                w_fixed_2_old_ );
        }
    else
        {
            child_->calc_yhat(
                enums::Aggregation::avg,
                _old_weight,
                _new_weights,
                indices_.unique_integers(),
                eta1_,
                eta1_old_,
                eta2_,
                eta2_old_ );
        }
}

// ----------------------------------------------------------------------------

void Avg::calc_yhat(
    const enums::Aggregation _agg,
    const Float _old_weight,
    const std::array<Float, 3>& _new_weights,
    const std::vector<size_t>& _indices,
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta1_old,
    const std::vector<Float>& _eta2,
    const std::vector<Float>& _eta2_old )
{
    assert_true( !std::isnan( std::get<0>( _new_weights ) ) );

    const auto [eta1, eta1_old, eta2, eta2_old] = intermediate_agg().calc_etas(
        true, _agg, _indices, _eta1, _eta1_old, _eta2, _eta2_old );

    child_->calc_yhat(
        _agg,
        _old_weight,
        _new_weights,
        intermediate_agg().indices(),
        *eta1,
        *eta1_old,
        *eta2,
        *eta2_old );

    intermediate_agg().update_etas_old( enums::Aggregation::avg );
}

// ----------------------------------------------------------------------------

void Avg::commit(
    const Float _old_intercept,
    const Float _old_weight,
    const std::array<Float, 3>& _weights )
{
    // ------------------------------------------------------------------------

    assert_true( eta1_.size() == eta2_.size() );
    assert_true( eta1_.size() == count_committed_.size() );

    assert_true( eta1_.size() == count1_.size() );
    assert_true( eta1_.size() == count2_.size() );
    assert_true( eta1_.size() == eta1_2_null_.size() );
    assert_true( eta1_.size() == eta2_1_null_.size() );

    assert_true( eta1_.size() == w_fixed_1_.size() );
    assert_true( eta1_.size() == w_fixed_2_.size() );
    assert_true( eta1_.size() == w_fixed_committed_.size() );

    // ------------------------------------------------------------------------

    if ( std::isnan( std::get<2>( _weights ) ) )
        {
            assert_true( !std::isnan( std::get<1>( _weights ) ) );

            if ( std::isnan( _old_weight ) )
                {
                    for ( auto ix : indices_ )
                        {
                            count_committed_[ix] += count1_[ix];
                        }
                }
            else
                {
                    for ( auto ix : indices_ )
                        {
                            assert_true( count_committed_[ix] >= count2_[ix] );
                            count_committed_[ix] -= count2_[ix];
                        }
                }

            for ( auto ix : indices_ )
                {
                    w_fixed_committed_[ix] =
                        eta1_2_null_[ix] * std::get<1>( _weights ) +
                        w_fixed_1_[ix];
                }
        }

    // ------------------------------------------------------------------------

    else if ( std::isnan( std::get<1>( _weights ) ) )
        {
            assert_true( !std::isnan( std::get<2>( _weights ) ) );

            if ( std::isnan( _old_weight ) )
                {
                    for ( auto ix : indices_ )
                        {
                            count_committed_[ix] += count2_[ix];
                        }
                }
            else
                {
                    for ( auto ix : indices_ )
                        {
                            assert_true( count_committed_[ix] >= count1_[ix] );
                            count_committed_[ix] -= count1_[ix];
                        }
                }

            for ( auto ix : indices_ )
                {
                    w_fixed_committed_[ix] =
                        eta2_1_null_[ix] * std::get<2>( _weights ) +
                        w_fixed_2_[ix];
                }
        }

    // ------------------------------------------------------------------------

    else
        {
            assert_true( !std::isnan( _old_weight ) );

            for ( auto ix : indices_ )
                {
                    w_fixed_committed_[ix] +=
                        eta1_[ix] * std::get<1>( _weights ) +
                        eta2_[ix] * std::get<2>( _weights ) -
                        ( eta1_[ix] + eta2_[ix] ) * _old_weight;
                }
        }

    // ------------------------------------------------------------------------

    for ( auto ix : indices_ )
        {
            assert_true( ix < count1_.size() );

            count1_[ix] = 0.0;
            count2_[ix] = 0.0;

            eta1_2_null_old_[ix] = 0.0;
            eta2_1_null_old_[ix] = 0.0;

            w_fixed_1_old_[ix] = 0.0;
            w_fixed_2_old_[ix] = 0.0;
        }

        // ------------------------------------------------------------------------

#ifndef NDEBUG

    for ( auto val : count1_ )
        {
            assert_true( val == 0.0 );
        }

    for ( auto val : count2_ )
        {
            assert_true( val == 0.0 );
        }

#endif  // NDEBUG

    // ------------------------------------------------------------------------

    impl_.commit( _weights );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Avg::deactivate(
    const Float _old_weight,
    const containers::IntSet::Iterator _begin,
    const containers::IntSet::Iterator _end )
{
    // -----------------------------------------------------------------

    for ( auto it = _begin; it != _end; ++it )
        {
            const auto ix = *it;

            assert_true( count_committed_[ix] >= 0.0 );
            assert_true( count1_[ix] >= 0.0 );
            assert_true( count2_[ix] >= 0.0 );

            assert_true( count_committed_[ix] >= count1_[ix] + count2_[ix] );

            if ( count_committed_[ix] == count2_[ix] )
                {
                    eta1_2_null_[ix] = w_fixed_1_[ix] = 0.0;
                }
            else
                {
                    assert_true( !std::isnan( w_fixed_committed_[ix] ) );
                    assert_true( count_committed_[ix] - count2_[ix] > 0.5 );

                    eta1_2_null_[ix] =
                        count1_[ix] / ( count_committed_[ix] - count2_[ix] );

                    w_fixed_1_[ix] =
                        ( w_fixed_committed_[ix] * count_committed_[ix] -
                          _old_weight * eta_old_[ix] ) /
                        ( count_committed_[ix] - count2_[ix] );

                    assert_true( !std::isnan( eta1_2_null_[ix] ) );
                    assert_true( !std::isnan( w_fixed_1_[ix] ) );
                }

            if ( count_committed_[ix] == count1_[ix] )
                {
                    eta2_1_null_[ix] = w_fixed_2_[ix] = 0.0;
                }
            else
                {
                    assert_true( !std::isnan( w_fixed_committed_[ix] ) );
                    assert_true( count_committed_[ix] - count1_[ix] > 0.5 );

                    eta2_1_null_[ix] =
                        count2_[ix] / ( count_committed_[ix] - count1_[ix] );

                    w_fixed_2_[ix] =
                        ( w_fixed_committed_[ix] * count_committed_[ix] -
                          _old_weight * eta_old_[ix] ) /
                        ( count_committed_[ix] - count1_[ix] );

                    assert_true( !std::isnan( eta2_1_null_[ix] ) );
                    assert_true( !std::isnan( w_fixed_2_[ix] ) );
                }
        }

    // -----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Float Avg::evaluate_split(
    const Float _old_intercept,
    const Float _old_weight,
    const std::array<Float, 3>& _weights,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _split,
    const std::vector<containers::Match>::iterator _end )
{
    // -----------------------------------------------------------------

    assert_true( !std::isinf( std::get<0>( _weights ) ) );
    assert_true( !std::isinf( std::get<1>( _weights ) ) );
    assert_true( !std::isinf( std::get<2>( _weights ) ) );

    assert_true(
        !std::isnan( std::get<1>( _weights ) ) ||
        !std::isnan( std::get<2>( _weights ) ) );

    calc_all( enums::Revert::False, _old_weight, _begin, _begin, _split, _end );

    calc_yhat( _old_weight, _weights );

    // -----------------------------------------------------------------

    Float loss_reduction = 0.0;

    if ( std::isnan( std::get<2>( _weights ) ) )
        {
            assert_true( !std::isnan( std::get<1>( _weights ) ) );

            loss_reduction =
                child_->evaluate_split( _old_intercept, _old_weight, _weights );
        }
    else if ( std::isnan( std::get<1>( _weights ) ) )
        {
            assert_true( !std::isnan( std::get<2>( _weights ) ) );

            loss_reduction =
                child_->evaluate_split( _old_intercept, _old_weight, _weights );
        }
    else
        {
            loss_reduction =
                child_->evaluate_split( _old_intercept, _old_weight, _weights );
        }

    // -----------------------------------------------------------------

    return loss_reduction;

    // -----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Avg::init_count_committed( const std::vector<containers::Match>& _matches )
{
    for ( auto m : _matches )
        {
            assert_true( m.ix_output < count_committed_.size() );

            ++count_committed_[m.ix_output];
        }
}

// ----------------------------------------------------------------------------

void Avg::resize( size_t _size )
{
    count_committed_ = std::vector<Float>( _size );

    count1_ = std::vector<Float>( _size );
    count2_ = std::vector<Float>( _size );

    eta1_2_null_ = std::vector<Float>( _size );
    eta2_1_null_ = std::vector<Float>( _size );

    eta1_2_null_old_ = std::vector<Float>( _size );
    eta2_1_null_old_ = std::vector<Float>( _size );

    eta_old_ = std::vector<Float>( _size );

    w_fixed_1_ = std::vector<Float>( _size );
    w_fixed_2_ = std::vector<Float>( _size );

    w_fixed_1_old_ = std::vector<Float>( _size );
    w_fixed_2_old_ = std::vector<Float>( _size );

    w_fixed_committed_ = std::vector<Float>( _size );

    indices_current_ = containers::IntSet( _size );

    impl_.resize( _size );
}

// ----------------------------------------------------------------------------

void Avg::revert( const Float _old_weight )
{
    // -------------------------------------------------------------

    if ( !std::isnan( _old_weight ) )
        {
            for ( auto ix : indices_current_ )
                {
                    eta2_[ix] += eta1_[ix];
                    eta1_[ix] = 0.0;
                }
        }

    for ( auto ix : indices_current_ )
        {
            count2_[ix] += count1_[ix];
            count1_[ix] = 0.0;
        }

    if ( allow_null_weights_ )
        {
            if ( std::isnan( _old_weight ) )
                {
                    activate(
                        indices_current_.begin(), indices_current_.end() );
                }
            else
                {
                    deactivate(
                        _old_weight,
                        indices_current_.begin(),
                        indices_current_.end() );
                }
        }

    // -------------------------------------------------------------

    assert_true( !allow_null_weights_ || !std::isnan( _old_weight ) );

    if ( !std::isnan( _old_weight ) )
        {
            child_->calc_etas(
                enums::Aggregation::avg,
                update_,
                _old_weight,
                indices_current_.unique_integers(),
                eta1_,
                eta1_old_,
                eta2_,
                eta2_old_ );
        }

    if ( allow_null_weights_ )
        {
            child_->calc_etas(
                enums::Aggregation::avg_second_null,
                update_,
                _old_weight,
                indices_current_.unique_integers(),
                eta1_2_null_,
                eta1_2_null_old_,
                w_fixed_1_,
                w_fixed_1_old_ );

            child_->calc_etas(
                enums::Aggregation::avg_first_null,
                update_,
                _old_weight,
                indices_current_.unique_integers(),
                eta2_1_null_,
                eta2_1_null_old_,
                w_fixed_2_,
                w_fixed_2_old_ );
        }

    update_etas_old( _old_weight );

    update_ = enums::Update::calc_diff;

    // -------------------------------------------------------------

    num_samples_2_ += num_samples_1_;

    num_samples_1_ = 0.0;

    // -------------------------------------------------------------

    indices_current_.clear();

    // -------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Avg::revert_to_commit()
{
    // ------------------------------------------------------------------------

    assert_true( count1_.size() == count2_.size() );

    // ------------------------------------------------------------------------

    for ( auto ix : indices_ )
        {
            assert_true( ix < count1_.size() );
            count1_[ix] = 0.0;
            count2_[ix] = 0.0;

            eta1_2_null_old_[ix] = 0.0;
            eta2_1_null_old_[ix] = 0.0;

            w_fixed_1_old_[ix] = 0.0;
            w_fixed_2_old_[ix] = 0.0;
        }

    // ------------------------------------------------------------------------

    impl_.revert_to_commit();

    assert_true( indices_.size() == 0 );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Float Avg::transform( const std::vector<Float>& _weights ) const
{
    Float count = 0.0;

    for ( auto weight : _weights )
        {
            if ( !std::isnan( weight ) )
                {
                    count += 1.0;
                }
        }

    Float result = 0.0;

    for ( auto weight : _weights )
        {
            if ( !std::isnan( weight ) )
                {
                    result += weight / count;
                }
        }

    return result;
}
// ----------------------------------------------------------------------------

void Avg::update_etas_old( const Float _old_weight )
{
    if ( !std::isnan( _old_weight ) )
        {
            for ( auto ix : indices_current_ )
                {
                    eta1_old_[ix] = eta1_[ix];
                    eta2_old_[ix] = eta2_[ix];
                }
        }

    for ( auto ix : indices_current_ )
        {
            eta1_2_null_old_[ix] = eta1_2_null_[ix];
            eta2_1_null_old_[ix] = eta2_1_null_[ix];

            w_fixed_1_old_[ix] = w_fixed_1_[ix];
            w_fixed_2_old_[ix] = w_fixed_2_[ix];
        }
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relboost
