#include "aggregations/aggregations.hpp"

namespace relboost
{
namespace aggregations
{
// ----------------------------------------------------------------------------

void AggregationImpl::commit( const std::array<RELBOOST_FLOAT, 3>& _weights )
{
    assert( eta1_.size() == eta2_.size() );

    child_->commit( eta1_, eta2_, indices_.unique_integers(), _weights );

    for ( auto ix : indices_ )
        {
            assert( ix < eta1_.size() );
            eta1_[ix] = 0.0;
            eta2_[ix] = 0.0;
        }

    indices_.clear();
    indices_current_.clear();
};

// ----------------------------------------------------------------------------

void AggregationImpl::reset()
{
    indices_.clear();
    indices_current_.clear();

    std::fill( eta1_.begin(), eta1_.end(), 0.0 );
    std::fill( eta2_.begin(), eta2_.end(), 0.0 );

    child_->reset();
}

// ----------------------------------------------------------------------------

void AggregationImpl::resize( size_t _size )
{
    eta1_.resize( _size );
    eta2_.resize( _size );
    indices_.resize( _size );
    indices_current_.resize( _size );
}

// ----------------------------------------------------------------------------

void AggregationImpl::revert_to_commit()
{
    assert( eta1_.size() == eta2_.size() );

    for ( auto ix : indices_ )
        {
            assert( ix < eta1_.size() );
            eta1_[ix] = 0.0;
            eta2_[ix] = 0.0;
        }

    child_->revert_to_commit( indices_.unique_integers() );

    indices_.clear();
    indices_current_.clear();
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relboost
