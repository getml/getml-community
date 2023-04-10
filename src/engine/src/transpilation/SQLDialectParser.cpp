// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "transpilation/SQLDialectParser.hpp"

#include "fct/always_false.hpp"
#include "fct/visit.hpp"
#include "transpilation/HumanReadableSQLGenerator.hpp"
#include "transpilation/TranspilationParams.hpp"

namespace transpilation {

fct::Ref<const SQLDialectGenerator> SQLDialectParser::parse(
    const TranspilationParams& _params) {
  const auto handle =
      [&_params](const auto& _dialect) -> fct::Ref<const SQLDialectGenerator> {
    using Type = std::decay_t<decltype(_dialect)>;
    if constexpr (std::is_same<Type, fct::Literal<"human-readable sql">>() ||
                  std::is_same<Type, fct::Literal<"sqlite3">>()) {
      return fct::Ref<const HumanReadableSQLGenerator>::make();
    } else {
      static_assert(fct::always_false_v<Type>, "Not all cases were covered.");
    }
  };

  return fct::visit(handle, _params.dialect_);
}

}  // namespace transpilation
