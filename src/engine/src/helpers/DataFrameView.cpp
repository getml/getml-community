// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "helpers/DataFrameView.hpp"

namespace helpers {

DataFrameView::DataFrameView(
    const DataFrame& _df,
    const std::shared_ptr<const std::vector<size_t>>& _rows)
    : df_(_df), rows_(_rows) {}

}  // namespace helpers
