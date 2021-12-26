#ifndef FASTPROP_CONTAINER_COLUMNVIEW_HPP_
#define FASTPROP_CONTAINER_COLUMNVIEW_HPP_

#include "helpers/helpers.hpp"

namespace fastprop {
namespace containers {
// -------------------------------------------------------------------------

template <class T, class ContainerType>
using ColumnView = helpers::ColumnView<T, ContainerType>;

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace fastprop

#endif  // FASTPROP_CONTAINER_COLUMNVIEW_HPP_
