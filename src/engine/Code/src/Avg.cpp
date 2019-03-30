#include "aggregations/aggregations.hpp"

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

            assert( count_committed_[ix] >= 0.0 );
            assert( count1_[ix] >= 0.0 );
            assert( count2_[ix] >= 0.0 );

            if ( count_committed_[ix] + count1_[ix] == 0.0 )
                {
                    eta1_2_null_[ix] = w_fixed_1_[ix] = 0.0;
                }
            else
                {
                    assert( !std::isnan( w_fixed_committed_[ix] ) );

                    eta1_2_null_[ix] =
                        count1_[ix] / ( count_committed_[ix] + count1_[ix] );

                    w_fixed_1_[ix] = w_fixed_committed_[ix] *
                                     count_committed_[ix] /
                                     ( count_committed_[ix] + count1_[ix] );

                    assert( !std::isnan( eta1_2_null_[ix] ) );
                    assert( !std::isnan( w_fixed_1_[ix] ) );
                }

            if ( count_committed_[ix] + count2_[ix] == 0.0 )
                {
                    eta2_1_null_[ix] = w_fixed_2_[ix] = 0.0;
                }
            else
                {
                    assert( !std::isnan( w_fixed_committed_[ix] ) );

                    eta2_1_null_[ix] =
                        count2_[ix] / ( count_committed_[ix] + count2_[ix] );

                    w_fixed_2_[ix] = w_fixed_committed_[ix] *
                                     count_committed_[ix] /
                                     ( count_committed_[ix] + count2_[ix] );

                    assert( !std::isnan( eta2_1_null_[ix] ) );
                    assert( !std::isnan( w_fixed_2_[ix] ) );
                }
        }
}

// ----------------------------------------------------------------------------

void Avg::calc_all(
    const enums::Revert _revert,
    const RELBOOST_FLOAT _old_weight,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _split_begin,
    const std::vector<const containers::Match*>::iterator _split_end,
    const std::vector<const containers::Match*>::iterator _end )
{
    // ------------------------------------------------------------------------

    assert( eta1_.size() == eta2_.size() );
    assert( eta1_.size() == eta_old_.size() );
    assert( eta1_.size() == count_committed_.size() );
    assert( eta1_.size() == w_fixed_1_.size() );
    assert( eta1_.size() == w_fixed_2_.size() );

    assert( indices_.size() == 0 );

    // ------------------------------------------------------------------------

#ifndef NDEBUG

    for ( auto val : count1_ )
        {
            assert( val == 0.0 );
        }

    for ( auto val : count2_ )
        {
            assert( val == 0.0 );
        }

#endif  // NDEBUG

    // ------------------------------------------------------------------------
    // Calculate eta1_, eta2_, eta_old_, count1_ and count2_.

    if ( !std::isnan( _old_weight ) )
        {
            for ( auto it = _begin; it != _split_begin; ++it )
                {
                    const auto ix = ( *it )->ix_output;

                    assert( count_committed_[ix] > 0.0 );

                    eta2_[ix] += 1.0 / count_committed_[ix];

                    ++count2_[ix];

                    indices_.insert( ix );
                }

            for ( auto it = _split_begin; it != _split_end; ++it )
                {
                    const auto ix = ( *it )->ix_output;

                    assert( count_committed_[ix] > 0.0 );

                    eta1_[ix] += 1.0 / count_committed_[ix];

                    ++count1_[ix];

                    indices_.insert( ix );
                }

            for ( auto it = _split_end; it != _end; ++it )
                {
                    const auto ix = ( *it )->ix_output;

                    assert( count_committed_[ix] > 0.0 );

                    eta2_[ix] += 1.0 / count_committed_[ix];

                    ++count2_[ix];

                    indices_.insert( ix );
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
                    const auto ix = ( *it )->ix_output;

                    ++count2_[ix];

                    indices_.insert( ix );
                }

            for ( auto it = _split_begin; it != _split_end; ++it )
                {
                    const auto ix = ( *it )->ix_output;

                    ++count1_[ix];

                    indices_.insert( ix );
                }

            for ( auto it = _split_end; it != _end; ++it )
                {
                    const auto ix = ( *it )->ix_output;

                    ++count2_[ix];

                    indices_.insert( ix );
                }

            for ( auto ix : indices_ )
                {
                    eta_old_[ix] = 0.0;
                }
        }

    // ------------------------------------------------------------------------
    // If we need to be able to revert this, we have to keep track of all
    // ix, for which count_1[ix] != 0.0.

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
    // Calculate eta1_2_null_, eta2_1_null_, w_fixed_1_ and w_fixed_2_.

    if ( std::isnan( _old_weight ) )
        {
            activate( indices_.begin(), indices_.end() );
        }
    else
        {
            deactivate( _old_weight, indices_.begin(), indices_.end() );
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Avg::calc_diff(
    const RELBOOST_FLOAT _old_weight,
    const std::vector<const containers::Match*>::iterator _split_begin,
    const std::vector<const containers::Match*>::iterator _split_end )
{
    assert( _split_end >= _split_begin );

    indices_current_.clear();

    if ( !std::isnan( _old_weight ) )
        {
            for ( auto it = _split_begin; it != _split_end; ++it )
                {
                    const auto ix = ( *it )->ix_output;

                    assert( ix < eta1_.size() );

                    assert( count_committed_[ix] > 0.0 );

                    eta1_[ix] += 1.0 / count_committed_[ix];
                    eta2_[ix] -= 1.0 / count_committed_[ix];

                    ++count1_[ix];
                    --count2_[ix];

                    assert( count2_[ix] >= 0.0 );

                    indices_current_.insert( ix );
                }
        }
    else
        {
            for ( auto it = _split_begin; it != _split_end; ++it )
                {
                    const auto ix = ( *it )->ix_output;

                    assert( ix < eta1_.size() );

                    ++count1_[ix];
                    --count2_[ix];

                    assert( count2_[ix] >= 0.0 );

                    indices_current_.insert( ix );
                }
        }

    if ( std::isnan( _old_weight ) )
        {
            activate( indices_current_.begin(), indices_current_.end() );
        }
    else
        {
            deactivate(
                _old_weight, indices_current_.begin(), indices_current_.end() );
        }
}

// ----------------------------------------------------------------------------

std::vector<std::array<RELBOOST_FLOAT, 3>> Avg::calc_weights(
    const enums::Revert _revert,
    const enums::Update _update,
    const RELBOOST_FLOAT _old_weight,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _split_begin,
    const std::vector<const containers::Match*>::iterator _split_end,
    const std::vector<const containers::Match*>::iterator _end )
{
    // -------------------------------------------------------------

    assert( eta1_.size() == eta2_.size() );
    assert( eta1_.size() == count_committed_.size() );

    debug_log(
        "std::distance(_begin, _split): " +
        std::to_string( std::distance( _begin, _split ) ) );

    debug_log(
        "std::distance(_split, _end): " +
        std::to_string( std::distance( _split, _end ) ) );

    // -------------------------------------------------------------

    if ( _update == enums::Update::calc_all )
        {
            calc_all(
                _revert, _old_weight, _begin, _split_begin, _split_end, _end );
        }
    else if ( _update == enums::Update::calc_diff )
        {
            calc_diff( _old_weight, _split_begin, _split_end );
        }
    else
        {
            assert( false && "Unknown Update!" );
        }

    // -------------------------------------------------------------

    std::vector<std::array<RELBOOST_FLOAT, 3>> results;

    if ( !std::isnan( _old_weight ) )
        {
            results.push_back( child_->calc_weights(
                enums::Aggregation::avg,
                _old_weight,
                indices_.unique_integers(),
                eta1_,
                eta2_ ) );
        }

    results.push_back( child_->calc_weights(
        enums::Aggregation::avg_second_null,
        _old_weight,
        indices_.unique_integers(),
        eta1_2_null_,
        w_fixed_1_ ) );

    results.push_back( child_->calc_weights(
        enums::Aggregation::avg_first_null,
        _old_weight,
        indices_.unique_integers(),
        eta2_1_null_,
        w_fixed_2_ ) );

    // -------------------------------------------------------------

    return results;

    // -------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::array<RELBOOST_FLOAT, 3> Avg::calc_weights(
    const enums::Aggregation _agg,
    const RELBOOST_FLOAT _old_weight,
    const std::vector<size_t>& _indices,
    const std::vector<RELBOOST_FLOAT>& _eta1,
    const std::vector<RELBOOST_FLOAT>& _eta2 )
{
    indices_.clear();

    for ( auto ix_input : _indices )
        {
            // -----------------------------------------------------------------
            // Figure out whether there are any matches in output table.

            assert( _eta1.size() == _eta2.size() );

            assert( eta1_.size() == count_committed_.size() );
            assert( eta2_.size() == count_committed_.size() );

            auto it =
                output_.indices_[0]->find( input_.join_keys_[0][ix_input] );

            if ( it == output_.indices_[0]->end() )
                {
                    continue;
                }

            // -----------------------------------------------------------------
            // If yes, update eta1_ and eta2_.

            for ( auto ix_output : it->second )
                {
                    assert( ix_input < _eta1.size() );
                    assert( ix_output < eta1_.size() );

                    assert( count_committed_[ix_output] > 0.0 );

                    eta1_[ix_output] +=
                        _eta1[ix_input] / count_committed_[ix_output];

                    eta2_[ix_output] +=
                        _eta2[ix_input] / count_committed_[ix_output];

                    indices_.insert( ix_output );
                }

            // -----------------------------------------------------------------
        }

    return child_->calc_weights(
        _agg, _old_weight, indices_.unique_integers(), eta1_, eta2_ );
}

// ----------------------------------------------------------------------------

void Avg::calc_yhat(
    const RELBOOST_FLOAT _old_weight,
    const std::array<RELBOOST_FLOAT, 3>& _new_weights )
{
    assert( !std::isnan( std::get<0>( _new_weights ) ) );

    if ( std::isnan( std::get<2>( _new_weights ) ) )
        {
            assert( !std::isnan( std::get<1>( _new_weights ) ) );

            child_->calc_yhat(
                enums::Aggregation::avg_second_null,
                _old_weight,
                _new_weights,
                indices_.unique_integers(),
                eta1_2_null_,
                w_fixed_1_ );
        }
    else if ( std::isnan( std::get<1>( _new_weights ) ) )
        {
            assert( !std::isnan( std::get<2>( _new_weights ) ) );

            child_->calc_yhat(
                enums::Aggregation::avg_first_null,
                _old_weight,
                _new_weights,
                indices_.unique_integers(),
                eta2_1_null_,
                w_fixed_2_ );
        }
    else
        {
            child_->calc_yhat(
                enums::Aggregation::avg,
                _old_weight,
                _new_weights,
                indices_.unique_integers(),
                eta1_,
                eta2_ );
        }
}

// ----------------------------------------------------------------------------

void Avg::calc_yhat(
    const enums::Aggregation _agg,
    const RELBOOST_FLOAT _old_weight,
    const std::array<RELBOOST_FLOAT, 3>& _new_weights,
    const std::vector<size_t>& _indices,
    const std::vector<RELBOOST_FLOAT>& _eta1,
    const std::vector<RELBOOST_FLOAT>& _eta2 )
{
    assert( !std::isnan( std::get<0>( _new_weights ) ) );

    switch ( _agg )
        {
            case enums::Aggregation::avg_second_null:
                assert( !std::isnan( std::get<1>( _new_weights ) ) );
                child_->calc_yhat(
                    _agg,
                    _old_weight,
                    _new_weights,
                    indices_.unique_integers(),
                    eta1_2_null_,
                    w_fixed_1_ );
                break;

            case enums::Aggregation::avg_first_null:
                assert( !std::isnan( std::get<2>( _new_weights ) ) );
                child_->calc_yhat(
                    _agg,
                    _old_weight,
                    _new_weights,
                    indices_.unique_integers(),
                    eta2_1_null_,
                    w_fixed_2_ );
                break;

            case enums::Aggregation::avg:
                child_->calc_yhat(
                    _agg,
                    _old_weight,
                    _new_weights,
                    indices_.unique_integers(),
                    eta1_,
                    eta2_ );
                break;

            default:
                assert( false && "Unknown aggregation!" );
        }
}

// ----------------------------------------------------------------------------

void Avg::commit(
    const std::vector<RELBOOST_FLOAT>& _eta1,
    const std::vector<RELBOOST_FLOAT>& _eta2,
    const std::vector<size_t>& _indices,
    const std::array<RELBOOST_FLOAT, 3>& _weights )
{
    // TODO
}

// ----------------------------------------------------------------------------

void Avg::commit(
    const RELBOOST_FLOAT _old_intercept,
    const RELBOOST_FLOAT _old_weight,
    const std::array<RELBOOST_FLOAT, 3>& _weights,
    const std::vector<const containers::Match*>::iterator _begin,
    const std::vector<const containers::Match*>::iterator _split,
    const std::vector<const containers::Match*>::iterator _end )
{
    // ------------------------------------------------------------------------

    assert( eta1_.size() == eta2_.size() );
    assert( eta1_.size() == count_committed_.size() );

    assert( eta1_.size() == count1_.size() );
    assert( eta1_.size() == count2_.size() );
    assert( eta1_.size() == eta1_2_null_.size() );
    assert( eta1_.size() == eta2_1_null_.size() );

    assert( eta1_.size() == w_fixed_1_.size() );
    assert( eta1_.size() == w_fixed_2_.size() );
    assert( eta1_.size() == w_fixed_committed_.size() );

    // ------------------------------------------------------------------------

    // When we are committing, the weight1 and weight2 matches are clearly
    // partitioned, so _begin == _split_begin.
    calc_all( enums::Revert::False, _old_weight, _begin, _begin, _split, _end );

    calc_yhat( _old_weight, _weights );

    // ------------------------------------------------------------------------

    if ( std::isnan( std::get<2>( _weights ) ) )
        {
            assert( !std::isnan( std::get<1>( _weights ) ) );

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
                            assert( count_committed_[ix] >= count2_[ix] );
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
            assert( !std::isnan( std::get<2>( _weights ) ) );

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
                            assert( count_committed_[ix] >= count1_[ix] );
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
            assert( !std::isnan( _old_weight ) );

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
            assert( ix < count1_.size() );
            count1_[ix] = 0.0;
            count2_[ix] = 0.0;
        }

        // ------------------------------------------------------------------------

#ifndef NDEBUG

    for ( auto val : count1_ )
        {
            assert( val == 0.0 );
        }

    for ( auto val : count2_ )
        {
            assert( val == 0.0 );
        }

#endif  // NDEBUG

    // ------------------------------------------------------------------------

    impl_.commit( _weights );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Avg::deactivate(
    const RELBOOST_FLOAT _old_weight,
    const containers::IntSet::Iterator _begin,
    const containers::IntSet::Iterator _end )
{
    // -----------------------------------------------------------------

    for ( auto it = _begin; it != _end; ++it )
        {
            const auto ix = *it;

            assert( count_committed_[ix] >= 0.0 );
            assert( count1_[ix] >= 0.0 );
            assert( count2_[ix] >= 0.0 );

            assert( count_committed_[ix] >= count1_[ix] + count2_[ix] );

            if ( count_committed_[ix] == count2_[ix] )
                {
                    eta1_2_null_[ix] = w_fixed_1_[ix] = 0.0;
                }
            else
                {
                    assert( !std::isnan( w_fixed_committed_[ix] ) );

                    eta1_2_null_[ix] =
                        count1_[ix] / ( count_committed_[ix] - count2_[ix] );

                    w_fixed_1_[ix] =
                        ( w_fixed_committed_[ix] * count_committed_[ix] -
                          _old_weight * eta_old_[ix] ) /
                        ( count_committed_[ix] - count2_[ix] );

                    assert( !std::isnan( eta1_2_null_[ix] ) );
                    assert( !std::isnan( w_fixed_1_[ix] ) );
                }

            if ( count_committed_[ix] == count1_[ix] )
                {
                    eta2_1_null_[ix] = w_fixed_2_[ix] = 0.0;
                }
            else
                {
                    assert( !std::isnan( w_fixed_committed_[ix] ) );

                    eta2_1_null_[ix] =
                        count2_[ix] / ( count_committed_[ix] - count1_[ix] );

                    w_fixed_2_[ix] =
                        ( w_fixed_committed_[ix] * count_committed_[ix] -
                          _old_weight * eta_old_[ix] ) /
                        ( count_committed_[ix] - count1_[ix] );

                    assert( !std::isnan( eta2_1_null_[ix] ) );
                    assert( !std::isnan( w_fixed_2_[ix] ) );
                }
        }

    // -----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

RELBOOST_FLOAT Avg::evaluate_split(
    const RELBOOST_FLOAT _old_intercept,
    const RELBOOST_FLOAT _old_weight,
    const std::array<RELBOOST_FLOAT, 3>& _weights )
{
    // -----------------------------------------------------------------

    RELBOOST_FLOAT loss_reduction = 0.0;

    // -----------------------------------------------------------------

    calc_yhat( _old_weight, _weights );

    // -----------------------------------------------------------------

    if ( std::isnan( std::get<2>( _weights ) ) )
        {
            assert( !std::isnan( std::get<1>( _weights ) ) );

            loss_reduction = child_->evaluate_split(
                _old_intercept,
                _old_weight,
                _weights,
                indices_.unique_integers(),
                eta1_2_null_,
                eta_old_ );
        }
    else if ( std::isnan( std::get<1>( _weights ) ) )
        {
            assert( !std::isnan( std::get<2>( _weights ) ) );

            loss_reduction = child_->evaluate_split(
                _old_intercept,
                _old_weight,
                _weights,
                indices_.unique_integers(),
                eta2_1_null_,
                eta_old_ );
        }
    else
        {
            loss_reduction = child_->evaluate_split(
                _old_intercept,
                _old_weight,
                _weights,
                indices_.unique_integers(),
                eta1_,
                eta2_ );
        }

    // -----------------------------------------------------------------

    return loss_reduction;
}

// ----------------------------------------------------------------------------

RELBOOST_FLOAT Avg::evaluate_split(
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

void Avg::init_count_committed(
    const std::vector<const containers::Match*>& _matches_ptr )
{
    for ( auto m : _matches_ptr )
        {
            assert( m->ix_output < count_committed_.size() );

            ++count_committed_[m->ix_output];
        }
}

// ----------------------------------------------------------------------------

void Avg::resize( size_t _size )
{
    count_committed_.resize( _size );
    count1_.resize( _size );
    count2_.resize( _size );

    eta1_2_null_.resize( _size );
    eta2_1_null_.resize( _size );

    eta_old_.resize( _size );

    w_fixed_1_.resize( _size );
    w_fixed_2_.resize( _size );
    w_fixed_committed_.resize( _size );

    indices_current_.resize( _size );

    impl_.resize( _size );
}

// ----------------------------------------------------------------------------

void Avg::revert( const RELBOOST_FLOAT _old_weight )
{
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

    if ( std::isnan( _old_weight ) )
        {
            activate( indices_current_.begin(), indices_current_.end() );
        }
    else
        {
            deactivate(
                _old_weight, indices_current_.begin(), indices_current_.end() );
        }

    indices_current_.clear();
}

// ----------------------------------------------------------------------------

void Avg::revert_to_commit()
{
    // ------------------------------------------------------------------------

    assert( count1_.size() == count2_.size() );

    // ------------------------------------------------------------------------

    for ( auto ix : indices_ )
        {
            assert( ix < count1_.size() );
            count1_[ix] = 0.0;
            count2_[ix] = 0.0;
        }

        // ------------------------------------------------------------------------

#ifndef NDEBUG

    for ( auto val : count1_ )
        {
            assert( val == 0.0 );
        }

    for ( auto val : count2_ )
        {
            assert( val == 0.0 );
        }

#endif  // NDEBUG

    // ------------------------------------------------------------------------

    impl_.revert_to_commit();

    assert( indices_.size() == 0 );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

RELBOOST_FLOAT Avg::transform(
    const std::vector<RELBOOST_FLOAT>& _weights ) const
{
    RELBOOST_FLOAT count = 0.0;

    for ( auto weight : _weights )
        {
            if ( !std::isnan( weight ) )
                {
                    count += 1.0;
                }
        }

    RELBOOST_FLOAT result = 0.0;

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
}  // namespace aggregations
}  // namespace relboost
