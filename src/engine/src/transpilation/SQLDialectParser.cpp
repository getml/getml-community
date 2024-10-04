// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "transpilation/SQLDialectParser.hpp"

#include <rfl/always_false.hpp>
#include <rfl/visit.hpp>
#include <stdexcept>

#include "transpilation/HumanReadableSQLGenerator.hpp"
#include "transpilation/TranspilationParams.hpp"

namespace transpilation {

rfl::Ref<const SQLDialectGenerator> SQLDialectParser::parse(
    const TranspilationParams& _params) {
  const auto handle =
      [](const auto& _dialect) -> rfl::Ref<const SQLDialectGenerator> {
    using Type = std::decay_t<decltype(_dialect)>;
    if constexpr (std::is_same<Type, rfl::Literal<"human-readable sql">>() ||
                  std::is_same<Type, rfl::Literal<"sqlite3">>()) {
      return rfl::Ref<const HumanReadableSQLGenerator>::make();
    } else {
      throw std::runtime_error(
          "The " + _dialect.name() +
          " dialect is not supported in the community edition. Please "
          "upgrade to getML enterprise to use this. An overview of what is "
          "supported in the community edition can be found in the official "
          "getML documentation.");
    }
  };

  return rfl::visit(handle, _params.dialect());
}

}  // namespace transpilation
