#include "optimizationcriteria/optimizationcriteria.hpp"

namespace autosql
{
namespace optimizationcriteria
{
// ----------------------------------------------------------------------------

OptimizationCriterionImpl::OptimizationCriterionImpl()
    : max_ix_( -1 ), storage_ix_( 0 ), value_( 0.0 ){};

// ----------------------------------------------------------------------------

void OptimizationCriterionImpl::commit(
    containers::Matrix<SQLNET_FLOAT>& _sufficient_statistics_committed )
{
    assert( max_ix_ >= 0 );

    assert( max_ix_ < storage_ix_ );

    const auto ncols = _sufficient_statistics_committed.ncols();

    std::copy(
        &sufficient_statistics_stored_( max_ix_, 0 ),
        &sufficient_statistics_stored_( max_ix_, ncols ),
        _sufficient_statistics_committed.begin() );

    std::copy(
        &sufficient_statistics_stored_( max_ix_, 0 ),
        &sufficient_statistics_stored_( max_ix_, ncols ),
        _sufficient_statistics_committed.begin() );

    value_ = values_stored_[max_ix_];
}

// ----------------------------------------------------------------------------

void OptimizationCriterionImpl::extend_storage_size(
    SQLNET_INT _storage_size_extension,
    SQLNET_INT _sufficient_statistics_ncols )
{
    // sufficient_statistics_stored_ has two extra columns for
    // num_samples_smaller and num_samples_greater

    assert( _storage_size_extension >= 0 );

    assert( _sufficient_statistics_ncols > 0 );

    assert(
        sufficient_statistics_stored_.ncols() ==
        _sufficient_statistics_ncols + 2 );

    containers::Matrix<SQLNET_FLOAT> extension(
        _storage_size_extension, _sufficient_statistics_ncols + 2 );

    sufficient_statistics_stored_.append( extension );

    extension = containers::Matrix<SQLNET_FLOAT>( _storage_size_extension, 1 );

    values_stored_.append( extension );
}

// ----------------------------------------------------------------------------

containers::Matrix<SQLNET_FLOAT>
OptimizationCriterionImpl::reduce_sufficient_statistics_stored() const
{
#ifdef SQLNET_PARALLEL

    containers::Matrix<SQLNET_FLOAT> sufficient_statistics_global(
        sufficient_statistics_stored().nrows(),
        sufficient_statistics_stored().ncols() );

    const SQLNET_INT count = sufficient_statistics_global.nrows() *
                             sufficient_statistics_global.ncols();

    SQLNET_PARALLEL_LIB::all_reduce(
        *comm_,                                 // comm
        sufficient_statistics_stored().data(),  // in_values
        count,                                  // n
        sufficient_statistics_global.data(),    // out_values
        std::plus<SQLNET_FLOAT>()               // op
    );

    comm_->barrier();

    return sufficient_statistics_global;

#else  // SQLNET_PARALLEL

    return sufficient_statistics_stored_;

#endif  // SQLNET_PARALLEL
}

// ----------------------------------------------------------------------------

void OptimizationCriterionImpl::reset(
    containers::Matrix<SQLNET_FLOAT>& _sufficient_statistics_current,
    containers::Matrix<SQLNET_FLOAT>& _sufficient_statistics_committed )
{
    std::fill(
        _sufficient_statistics_committed.begin(),
        _sufficient_statistics_committed.end(),
        0.0 );

    std::fill(
        _sufficient_statistics_current.begin(),
        _sufficient_statistics_current.end(),
        0.0 );
}

// ----------------------------------------------------------------------------

void OptimizationCriterionImpl::set_storage_size(
    SQLNET_INT _storage_size, SQLNET_INT _sufficient_statistics_ncols )
{
    // sufficient_statistics_stored_ has two extra columns for
    // num_samples_smaller and num_samples_greater

    sufficient_statistics_stored_ = containers::Matrix<SQLNET_FLOAT>(
        _storage_size, _sufficient_statistics_ncols + 2 );

    values_stored_ = containers::Matrix<SQLNET_FLOAT>( _storage_size, 1 );

    max_ix_ = -1;

    storage_ix_ = 0;
}

// ----------------------------------------------------------------------------

void OptimizationCriterionImpl::store_current_stage(
    const SQLNET_FLOAT _num_samples_smaller,
    const SQLNET_FLOAT _num_samples_greater,
    containers::Matrix<SQLNET_FLOAT> _sufficient_statistics_current )
{
    assert( storage_ix_ < sufficient_statistics_stored_.nrows() );

    std::copy(
        _sufficient_statistics_current.begin(),
        _sufficient_statistics_current.end(),
        &sufficient_statistics_stored_( storage_ix_, 0 ) );

    // num_samples_smaller and num_samples_greater are always the elements
    // in the last two columns of sufficient_statistics_stored_, which is
    // why sufficient_statistics_stored_ has two extra columns over ..._current
    // and ..._committed.
    sufficient_statistics_stored_(
        storage_ix_, _sufficient_statistics_current.ncols() ) =
        _num_samples_smaller;

    sufficient_statistics_stored_(
        storage_ix_, _sufficient_statistics_current.ncols() + 1 ) =
        _num_samples_greater;

    ++storage_ix_;
}

// ----------------------------------------------------------------------------
}
}
