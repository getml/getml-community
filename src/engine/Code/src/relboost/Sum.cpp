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

    assert( indices_.size() == 0 );

    // ------------------------------------------------------------------------
    // All matches between _split_begin and _split_end are allocated to _eta1.
    // All others are allocated to eta1_.

    for ( auto it = _begin; it != _split_begin; ++it )
        {
            ++eta2_[( *it )->ix_output];

            indices_.insert( ( *it )->ix_output );
        }

    for ( auto it = _split_begin; it != _split_end; ++it )
        {
            ++eta1_[( *it )->ix_output];

            indices_.insert( ( *it )->ix_output );
        }

    for ( auto it = _split_end; it != _end; ++it )
        {
            ++eta2_[( *it )->ix_output];

            indices_.insert( ( *it )->ix_output );
        }

    // ------------------------------------------------------------------------
    // If we need to be able to revert this, we have to keep track of all
    // ix for which eta1_[ix] != 0.0.

    if ( _revert == enums::Revert::True )
        {
            indices_current_.clear();

            for ( auto it = _split_begin; it != _split_end; ++it )
                {
                    const auto ix = ( *it )->ix_output;

                    indices_current_.insert( ix );
                }
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Sum::calc_diff(
    const enums::Revert _revert,
    const std::vector<const containers::Match*>::iterator _split_begin,
    const std::vector<const containers::Match*>::iterator _split_end )
{
    assert( _split_end >= _split_begin );

    // ------------------------------------------------------------------------
    // Incremental updates imply that we move samples from eta2_ to eta1_.

    for ( auto it = _split_begin; it != _split_end; ++it )
        {
            assert( ( *it )->ix_output < eta1_.size() );

            ++eta1_[( *it )->ix_output];

            --eta2_[( *it )->ix_output];

            assert( eta2_[( *it )->ix_output] >= 0.0 );
        }

    // ------------------------------------------------------------------------
    // If we need to be able to revert this, we have to keep track of all
    // ix which we have just changed.

    if ( _revert == enums::Revert::True )
        {
            indices_current_.clear();

            for ( auto it = _split_begin; it != _split_end; ++it )
                {
                    const auto ix = ( *it )->ix_output;

                    indices_current_.insert( ix );
                }
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<std::array<RELBOOST_FLOAT, 3>> Sum::calc_weights(
    const enums::Revert _revert,
    const enums::Update _update,
    const RELBOOST_FLOAT _old_weight,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _split_begin,
    const std::vector<const containers::Match*>::iterator _split_end,
    const std::vector<const containers::Match*>::iterator _end )
{
    assert( eta1_.size() == eta2_.size() );

    debug_log(
        "std::distance(_begin, _split): " +
        std::to_string( std::distance( _begin, _split ) ) );

    debug_log(
        "std::distance(_split, _end): " +
        std::to_string( std::distance( _split, _end ) ) );

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
            assert( false && "Unknown Update!" );
        }

    // -------------------------------------------------------------

    std::vector<std::array<RELBOOST_FLOAT, 3>> results = {child_->calc_weights(
        enums::Aggregation::sum,
        _old_weight,
        indices_.unique_integers(),
        eta1_,
        eta2_ )};

    // -------------------------------------------------------------

    return results;

    // -------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::array<RELBOOST_FLOAT, 3> Sum::calc_weights(
    const enums::Aggregation _agg,
    const RELBOOST_FLOAT _old_weight,
    const std::vector<size_t>& _indices,
    const std::vector<RELBOOST_FLOAT>& _eta1,
    const std::vector<RELBOOST_FLOAT>& _eta2 )
{
    for ( auto ix_input : _indices )
        {
            // -----------------------------------------------------------------
            // Figure out whether there are any matches in output table.

            assert( _eta1.size() == _eta2.size() );

            auto it =
                output_.indices_[0]->find( input_.join_keys_[0][ix_input] );

            if ( it == output_.indices_[0]->end() )
                {
                    continue;
                }

            // -----------------------------------------------------------------
            // If yes, update them.

            for ( auto ix_output : it->second )
                {
                    assert( ix_input < _eta1.size() );
                    assert( ix_output < eta1_.size() );

                    eta1_[ix_output] += _eta1[ix_input];
                    eta2_[ix_output] += _eta2[ix_input];

                    indices_.insert( ix_output );
                }

            // -----------------------------------------------------------------
        }

    return child_->calc_weights(
        _agg, _old_weight, indices_.unique_integers(), eta1_, eta2_ );
}

// ----------------------------------------------------------------------------

void Sum::commit(
    const std::vector<RELBOOST_FLOAT>& _eta1,
    const std::vector<RELBOOST_FLOAT>& _eta2,
    const std::vector<size_t>& _indices,
    const std::array<RELBOOST_FLOAT, 3>& _weights )
{
    // TODO
}

// ----------------------------------------------------------------------------

void Sum::commit(
    const RELBOOST_FLOAT _old_intercept,
    const RELBOOST_FLOAT _old_weight,
    const std::array<RELBOOST_FLOAT, 3>& _weights,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _split,
    const std::vector<const containers::Match*>::iterator _end )
{
    assert( eta1_.size() == eta2_.size() );

    // When we are committing, the weight1 and weight2 matches are clearly
    // partitioned, so _begin == _split_begin.
    calc_all( enums::Revert::False, _begin, _begin, _split, _end );

    calc_yhat( _old_weight, _weights );

    impl_.commit( _weights );
};

// ----------------------------------------------------------------------------

RELBOOST_FLOAT Sum::evaluate_split(
    const RELBOOST_FLOAT _old_intercept,
    const RELBOOST_FLOAT _old_weight,
    const std::array<RELBOOST_FLOAT, 3>& _weights )
{
    // -----------------------------------------------------------------
    // Calculate yhat.

    calc_yhat( _old_weight, _weights );

    // -----------------------------------------------------------------
    // Pass on to next higher level

    return child_->evaluate_split(
        _old_intercept,
        _old_weight,
        _weights,
        indices_.unique_integers(),
        eta1_,
        eta2_ );

    // -----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

RELBOOST_FLOAT Sum::evaluate_split(
    const RELBOOST_FLOAT _old_intercept,
    const RELBOOST_FLOAT _old_weight,
    const std::array<RELBOOST_FLOAT, 3>& _weights,
    const std::vector<size_t>& _indices,
    const std::vector<RELBOOST_FLOAT>& _eta1,
    const std::vector<RELBOOST_FLOAT>& _eta2 )
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

void Sum::revert( const RELBOOST_FLOAT _old_weight )
{
    for ( auto ix : indices_current_ )
        {
            eta2_[ix] += eta1_[ix];

            eta1_[ix] = 0.0;
        }

    indices_current_.clear();
}

// ----------------------------------------------------------------------------

RELBOOST_FLOAT Sum::transform(
    const std::vector<RELBOOST_FLOAT>& _weights ) const
{
    return std::accumulate( _weights.begin(), _weights.end(), 0.0 );
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relboost
