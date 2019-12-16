#include "relboost/aggregations/aggregations.hpp"

namespace relboost
{
namespace aggregations
{
// ----------------------------------------------------------------------------

void Sum::calc_all(
    const enums::Revert _revert,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _split_begin,
    const std::vector<const containers::Match*>::iterator _split_end,
    const std::vector<const containers::Match*>::iterator _end )
{
    // ------------------------------------------------------------------------

    assert_true( indices_.size() == 0 );
    assert_true( indices_current_.size() == 0 );

    // ------------------------------------------------------------------------
    // All matches between _split_begin and _split_end are allocated to
    // _eta1. All others are allocated to eta1_.

    num_samples_1_ = 0.0;

    num_samples_2_ = 0.0;

    for ( auto it = _begin; it != _split_begin; ++it )
        {
            ++eta2_[( *it )->ix_output];

            ++num_samples_2_;

            indices_.insert( ( *it )->ix_output );
            indices_current_.insert( ( *it )->ix_output );
        }

    for ( auto it = _split_begin; it != _split_end; ++it )
        {
            ++eta1_[( *it )->ix_output];

            ++num_samples_1_;

            indices_.insert( ( *it )->ix_output );
            indices_current_.insert( ( *it )->ix_output );
        }

    for ( auto it = _split_end; it != _end; ++it )
        {
            ++eta2_[( *it )->ix_output];

            ++num_samples_2_;

            indices_.insert( ( *it )->ix_output );
            indices_current_.insert( ( *it )->ix_output );
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Sum::calc_diff(
    const enums::Revert _revert,
    const std::vector<const containers::Match*>::iterator _split_begin,
    const std::vector<const containers::Match*>::iterator _split_end )
{
    // ------------------------------------------------------------------------

    assert_true( _split_end >= _split_begin );

    // ------------------------------------------------------------------------
    // Incremental updates imply that we move samples from eta2_ to eta1_.

    for ( auto it = _split_begin; it != _split_end; ++it )
        {
            assert_true( ( *it )->ix_output < eta1_.size() );

            ++eta1_[( *it )->ix_output];

            --eta2_[( *it )->ix_output];

            indices_current_.insert( ( *it )->ix_output );

            assert_true( eta2_[( *it )->ix_output] >= 0.0 );
        }

    // ------------------------------------------------------------------------

    const auto dist =
        static_cast<Float>( std::distance( _split_begin, _split_end ) );

    num_samples_1_ += dist;
    num_samples_2_ -= dist;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Sum::calc_etas(
    const enums::Aggregation _agg,
    const std::vector<size_t>& _indices_current,
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta1_old,
    const std::vector<Float>& _eta2,
    const std::vector<Float>& _eta2_old )
{
    const auto [eta1, eta1_old, eta2, eta2_old] = intermediate_agg().calc_etas(
        false, _agg, _indices_current, _eta1, _eta1_old, _eta2, _eta2_old );

    child_->calc_etas(
        _agg,
        intermediate_agg().indices_current(),
        *eta1,
        *eta1_old,
        *eta2,
        *eta2_old );

    intermediate_agg().update_etas_old( _agg );
}

// ----------------------------------------------------------------------------

std::array<Float, 3> Sum::calc_weights(
    const enums::Aggregation _agg,
    const Float _old_weight,
    const std::vector<size_t>& _indices,
    const std::vector<size_t>& _indices_current,
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta1_old,
    const std::vector<Float>& _eta2,
    const std::vector<Float>& _eta2_old )

{
    const auto [eta1, eta1_old, eta2, eta2_old] = intermediate_agg().calc_etas(
        false, _agg, _indices_current, _eta1, _eta1_old, _eta2, _eta2_old );

    const auto weights = child_->calc_weights(
        _agg,
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

std::vector<std::pair<Float, std::array<Float, 3>>> Sum::calc_pairs(
    const enums::Revert _revert,
    const enums::Update _update,
    const Float _min_num_samples,
    const Float _old_intercept,
    const Float _old_weight,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _split_begin,
    const std::vector<const containers::Match*>::iterator _split_end,
    const std::vector<const containers::Match*>::iterator _end )
{
    // -------------------------------------------------------------

    assert_true( eta1_.size() == eta2_.size() );

    // -------------------------------------------------------------

    debug_log( "Sum::calc_weights" );

    debug_log(
        "std::distance(_begin, _split_begin): " +
        std::to_string( std::distance( _begin, _split_begin ) ) );

    debug_log(
        "std::distance(_split_begin, _split_end): " +
        std::to_string( std::distance( _split_begin, _split_end ) ) );

    debug_log(
        "std::distance(_split_end, _end): " +
        std::to_string( std::distance( _split_end, _end ) ) );

    // -------------------------------------------------------------

    if ( _update == enums::Update::calc_all )
        {
            calc_all( _revert, _begin, _split_begin, _split_end, _end );
        }
    else if ( _update == enums::Update::calc_diff )
        {
            calc_diff( _revert, _split_begin, _split_end );
        }
    else
        {
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

    const auto weights = std::vector<std::array<Float, 3>>{child_->calc_weights(
        enums::Aggregation::sum,
        _old_weight,
        indices_.unique_integers(),
        indices_current_.unique_integers(),
        eta1_,
        eta1_old_,
        eta2_,
        eta2_old_ )};

    update_etas_old();

    // -------------------------------------------------------------

    if ( _revert == enums::Revert::False )
        {
            indices_current_.clear();
        }

    // -----------------------------------------------------------------

    for ( const auto& w : weights )
        {
            assert_true( !std::isinf( std::get<0>( w ) ) );
            assert_true( !std::isinf( std::get<1>( w ) ) );
            assert_true( !std::isinf( std::get<2>( w ) ) );

            if ( std::isnan( std::get<0>( w ) ) )
                {
                    continue;
                }

            assert_true(
                !std::isnan( std::get<1>( w ) ) ||
                !std::isnan( std::get<2>( w ) ) );

            calc_yhat( _old_weight, w );

            const auto loss_reduction = child_->evaluate_split(
                _old_intercept,
                _old_weight,
                w,
                indices_.unique_integers(),
                eta1_,
                eta2_ );

            results.push_back( std::make_pair( loss_reduction, w ) );
        }

    // -------------------------------------------------------------

    return results;

    // -------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Sum::calc_yhat(
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
        false, _agg, _indices, _eta1, _eta1_old, _eta2, _eta2_old );

    child_->calc_yhat(
        _agg,
        _old_weight,
        _new_weights,
        intermediate_agg().indices(),
        *eta1,
        *eta1_old,
        *eta2,
        *eta2_old );

    intermediate_agg().update_etas_old( _agg );
}

// ----------------------------------------------------------------------------

void Sum::commit(
    const Float _old_intercept,
    const Float _old_weight,
    const std::array<Float, 3>& _weights,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _split,
    const std::vector<const containers::Match*>::iterator _end )
{
    assert_true( eta1_.size() == eta2_.size() );

    debug_log( "Sum::commit" );

    // When we are committing, the weight1 and weight2 matches are clearly
    // partitioned, so _begin == _split_begin.
    calc_all( enums::Revert::False, _begin, _begin, _split, _end );

    calc_yhat( _old_weight, _weights );

    impl_.commit( _weights );
};

// ----------------------------------------------------------------------------

Float Sum::evaluate_split(
    const Float _old_intercept,
    const Float _old_weight,
    const std::array<Float, 3>& _weights,
    const std::vector<size_t>& _indices,
    const std::vector<Float>& _eta1,
    const std::vector<Float>& _eta2 )
{
    return child_->evaluate_split(
        _old_intercept,
        _old_weight,
        _weights,
        indices_.unique_integers(),
        eta1_,
        eta2_ );
}

// ----------------------------------------------------------------------------

void Sum::revert( const Float _old_weight )
{
    for ( auto ix : indices_current_ )
        {
            eta2_[ix] += eta1_[ix];

            eta1_[ix] = 0.0;
        }

    child_->calc_etas(
        enums::Aggregation::sum,
        indices_current_.unique_integers(),
        eta1_,
        eta1_old_,
        eta2_,
        eta2_old_ );

    update_etas_old();

    num_samples_2_ += num_samples_1_;

    num_samples_1_ = 0.0;

    indices_current_.clear();
}

// ----------------------------------------------------------------------------

Float Sum::transform( const std::vector<Float>& _weights ) const
{
    return std::accumulate( _weights.begin(), _weights.end(), 0.0 );
}

// ----------------------------------------------------------------------------

void Sum::update_etas_old()
{
    for ( auto ix : indices_current_ )
        {
            eta1_old_[ix] = eta1_[ix];
            eta2_old_[ix] = eta2_[ix];
        }
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relboost
