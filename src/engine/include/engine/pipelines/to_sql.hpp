// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_TOSQL_HPP_
#define ENGINE_PIPELINES_TOSQL_HPP_

#include <string>

#include "engine/pipelines/ToSQLParams.hpp"

namespace engine {
namespace pipelines {
namespace to_sql {

/// Expresses features as SQL code.
std::string to_sql(const ToSQLParams& _params);

}  // namespace to_sql
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_TOSQL_HPP_
