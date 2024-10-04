// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FASTPROP_CONTAINER_COLUMNVIEW_HPP_
#define FASTPROP_CONTAINER_COLUMNVIEW_HPP_

#include "helpers/ColumnView.hpp"

namespace fastprop {
namespace containers {

template <class T, class ContainerType>
using ColumnView = helpers::ColumnView<T, ContainerType>;

}  // namespace containers
}  // namespace fastprop

#endif  // FASTPROP_CONTAINER_COLUMNVIEW_HPP_
