#include "autosql/optimizationcriteria/optimizationcriteria.hpp"

namespace autosql
{
namespace optimizationcriteria
{
// ----------------------------------------------------------------------------

OptimizationCriterionImpl::OptimizationCriterionImpl()
    : max_ix_( -1 ), value_( 0.0 ){};

// ----------------------------------------------------------------------------

void OptimizationCriterionImpl::commit(
    std::vector<AUTOSQL_FLOAT>* _sufficient_statistics_committed )
{
    assert( max_ix_ >= 0 );

    assert( max_ix_ < storage_ix() );

    assert( max_ix_ < values_stored_.size() );

    assert( max_ix_ < sufficient_statistics_stored_.size() );

    const auto ncols = _sufficient_statistics_committed->size();

    // sufficient_statistics_stored_ has two extra columns for
    // num_samples_smaller and num_samples_greater
    assert( ncols == sufficient_statistics_stored_[max_ix_].size() - 2 );

    std::copy(
        sufficient_statistics_stored_[max_ix_].begin(),
        sufficient_statistics_stored_[max_ix_].begin() + ncols,
        _sufficient_statistics_committed->begin() );

    value_ = values_stored_[max_ix_];
}

// ----------------------------------------------------------------------------

std::vector<std::vector<AUTOSQL_FLOAT>>
OptimizationCriterionImpl::reduce_sufficient_statistics_stored() const
{
    std::vector<std::vector<AUTOSQL_FLOAT>> sufficient_statistics_global;

    for ( const auto& local : sufficient_statistics_stored() )
        {
            sufficient_statistics_global.push_back(
                std::vector<AUTOSQL_FLOAT>( local.size() ) );

            multithreading::all_reduce(
                *comm_,                                      // comm
                local.data(),                                // in_values
                local.size(),                                // n
                sufficient_statistics_global.back().data(),  // out_values
                std::plus<AUTOSQL_FLOAT>()                   // op
            );

            comm_->barrier();
        }

    return sufficient_statistics_global;
}

// ----------------------------------------------------------------------------

void OptimizationCriterionImpl::reset(
    std::vector<AUTOSQL_FLOAT>* _sufficient_statistics_current,
    std::vector<AUTOSQL_FLOAT>* _sufficient_statistics_committed )
{
    std::fill(
        _sufficient_statistics_committed->begin(),
        _sufficient_statistics_committed->end(),
        0.0 );

    std::fill(
        _sufficient_statistics_current->begin(),
        _sufficient_statistics_current->end(),
        0.0 );
}

// ----------------------------------------------------------------------------

void OptimizationCriterionImpl::store_current_stage(
    const AUTOSQL_FLOAT _num_samples_smaller,
    const AUTOSQL_FLOAT _num_samples_greater,
    const std::vector<AUTOSQL_FLOAT>& _sufficient_statistics_current )
{
    // num_samples_smaller and num_samples_greater are always the elements
    // in the last two columns of sufficient_statistics_stored_, which is
    // why sufficient_statistics_stored_ has two extra columns over ..._current
    // and ..._committed.

    sufficient_statistics_stored_.push_back( std::vector<AUTOSQL_FLOAT>(
        _sufficient_statistics_current.size() + 2 ) );

    std::copy(
        _sufficient_statistics_current.begin(),
        _sufficient_statistics_current.end(),
        sufficient_statistics_stored_.back().begin() );

    sufficient_statistics_stored_
        .back()[_sufficient_statistics_current.size()] = _num_samples_smaller;

    sufficient_statistics_stored_
        .back()[_sufficient_statistics_current.size() + 1] =
        _num_samples_greater;
}

// ----------------------------------------------------------------------------
}  // namespace optimizationcriteria
}  // namespace autosql
