#ifndef MULTIREL_CONTAINER_COLUMNVIEW_HPP_
#define MULTIREL_CONTAINER_COLUMNVIEW_HPP_

#include "helpers/helpers.hpp"

namespace multirel {
namespace containers {

template <class T, class ContainerType>
using ColumnView = helpers::ColumnView<T, ContainerType>;

}  // namespace containers
}  // namespace multirel

#endif  // MULTIREL_CONTAINER_COLUMNVIEW_HPP_
