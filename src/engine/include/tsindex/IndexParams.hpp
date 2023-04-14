// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CONTAINERS_TSINDEX_INDEXPARAMS_HPP_
#define CONTAINERS_TSINDEX_INDEXPARAMS_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <vector>

// ----------------------------------------------------------------------------

#include "fct/fct.hpp"

// ----------------------------------------------------------------------------

#include "tsindex/Float.hpp"
#include "tsindex/Int.hpp"

// ----------------------------------------------------------------------------

namespace tsindex {
class IndexParams {
 public:
  /// The join keys used for the index.
  const fct::Range<const Int*> join_keys_;

  /// The lower time stamps used for the the index.
  const fct::Range<const Float*> lower_ts_;

  /// The difference between the lower_ts and the upper_ts.
  const Float memory_;

  /// The rownums over which we actually have to build the index.
  /// We might not have to index the full data, because of
  /// multithreading or because there are entries in the peripheral
  /// table that have no matches in the population table.
  const std::shared_ptr<const std::vector<size_t>> rownums_;
};
}  // namespace tsindex

// ----------------------------------------------------------------------------

#endif  // CONTAINERS_TSINDEX_INDEXPARAMS_HPP_
