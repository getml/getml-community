#ifndef RELBOOST_CONTAINERS_CANDIDATESPLIT_HPP_
#define RELBOOST_CONTAINERS_CANDIDATESPLIT_HPP_

// -------------------------------------------------------------------------

#include <array>

// -------------------------------------------------------------------------

#include "relboost/Float.hpp"
#include "relboost/Int.hpp"

// -------------------------------------------------------------------------

#include "relboost/containers/Split.hpp"

// -------------------------------------------------------------------------

namespace relboost {
namespace containers {

struct CandidateSplit {
  CandidateSplit(const Float _partial_loss, const Split& _split,
                 const std::array<Float, 3>& _weights)
      : partial_loss_(_partial_loss), split_(_split), weights_(_weights) {}

  ~CandidateSplit() = default;

  const Float partial_loss_;

  const Split split_;

  const std::array<Float, 3> weights_;
};

}  // namespace containers
}  // namespace relboost

#endif  // RELBOOST_CONTAINERS_CANDIDATESPLIT_HPP_
