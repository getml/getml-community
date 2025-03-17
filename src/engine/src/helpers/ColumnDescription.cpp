// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "helpers/ColumnDescription.hpp"

namespace helpers {

  ColumnDescription::ColumnDescription(const MarkerType& _marker, const std::string& _table,
                    const std::string& _name)
      : val_(f_marker(_marker) * f_name(_name) * f_table(_table)) {}

ColumnDescription::ColumnDescription(const ReflectionType& _val) : val_(_val) {}

} // namespace helpers
