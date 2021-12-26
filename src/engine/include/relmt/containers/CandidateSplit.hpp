#ifndef RELMT_CONTAINERS_CANDIDATESPLIT_HPP_
#define RELMT_CONTAINERS_CANDIDATESPLIT_HPP_

// -------------------------------------------------------------------------

#include <array>

// -------------------------------------------------------------------------

#include "relmt/Float.hpp"
#include "relmt/Int.hpp"

// -------------------------------------------------------------------------

#include "relmt/containers/Split.hpp"
#include "relmt/containers/Weights.hpp"

// -------------------------------------------------------------------------
namespace relmt {
namespace containers {
// -------------------------------------------------------------------------

struct CandidateSplit {
  CandidateSplit(const Float _partial_loss, const Split& _split,
                 const Weights& _weights)
      : partial_loss_(_partial_loss), split_(_split), weights_(_weights) {}

  ~CandidateSplit() = default;

  const Float partial_loss_;

  const Split split_;

  const Weights weights_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace relmt

#endif  // RELMT_CONTAINERS_CANDIDATESPLIT_HPP_
