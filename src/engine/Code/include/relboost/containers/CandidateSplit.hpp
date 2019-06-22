#ifndef RELBOOST_CONTAINERS_CANDIDATESPLIT_HPP_
#define RELBOOST_CONTAINERS_CANDIDATESPLIT_HPP_

namespace relboost
{
namespace containers
{
// -------------------------------------------------------------------------

struct CandidateSplit
{
    CandidateSplit(
        const Float _loss_reduction,
        const Split& _split,
        const std::array<Float, 3>& _weights )
        : loss_reduction_( _loss_reduction ),
          split_( _split ),
          weights_( _weights )
    {
    }

    ~CandidateSplit() = default;

    const Float loss_reduction_;

    const Split split_;

    const std::array<Float, 3> weights_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace relboost

#endif  // RELBOOST_CONTAINERS_CANDIDATESPLIT_HPP_
