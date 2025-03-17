// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "containers/Encoding.hpp"

namespace containers {

Encoding::Encoding(const std::shared_ptr<memmap::Pool>& _pool,
                   const std::shared_ptr<const Encoding> _subencoding) {
  if (_pool) {
    init<MemoryMappedEncoding>(_pool, _subencoding);
  } else {
    init<InMemoryEncoding>(_pool, _subencoding);
  }
}

}  // namespace containers
