#ifndef GWT_H
#define GWT_H

#include <type_traits>
#include <utility>

namespace GWT {
namespace detail {

template <typename Callable>
concept SupplierCallable = std::is_invocable_v<Callable>;

template <typename Callable, typename ArgumentSupplierCallable>
concept TransformCallable =
    SupplierCallable<ArgumentSupplierCallable> &&
    std::is_invocable_v<Callable,
                        std::invoke_result_t<ArgumentSupplierCallable>>;

template <SupplierCallable>
class Given;

template <SupplierCallable>
class When;

template <SupplierCallable>
class Then;

template <SupplierCallable Supplier>
class Given {
 private:
  Supplier _supplier;

 public:
  explicit Given(Supplier&& supplier)
      : _supplier{std::forward<Supplier>(supplier)} {}

  ~Given() = default;

  explicit Given(Given const&) = delete;
  explicit Given(Given&&) = delete;
  auto operator=(Given const&) -> Given& = delete;

  template <TransformCallable<Supplier> Transformer>
  [[nodiscard]]
  auto when(Transformer&& transformer) {
    return When{[this, transformer = std::forward<Transformer>(transformer)]() {
      return transformer(this->_supplier());
    }};
  }
};

template <SupplierCallable Supplier>
class When {
 private:
  Supplier _supplier;

 public:
  explicit When(Supplier&& supplier)
      : _supplier{std::forward<Supplier>(supplier)} {}

  ~When() = default;

  explicit When(When const&) = delete;
  explicit When(When&&) = delete;
  auto operator=(When const&) -> When& = delete;

  template <TransformCallable<Supplier> Transformer>
  auto then(Transformer&& transformer) -> void {
    Then{[this, transformer = std::forward<Transformer>(transformer)]() {
      return transformer(this->_supplier());
    }};
  }
};

template <SupplierCallable Supplier>
class Then {
 private:
 public:
  explicit Then(Supplier&& supplier) { supplier(); }

  ~Then() = default;

  explicit Then(Then const&) = delete;
  explicit Then(Then&&) = delete;
  auto operator=(Then const&) -> Then& = delete;
};
}  // namespace detail

template <typename Object>
[[nodiscard]]
inline auto given(Object&& object) {
  if constexpr (std::is_invocable_v<Object>) {
    return detail::Given{std::forward<Object>(object)};
  } else {
    return detail::Given{
        [object = std::forward<Object>(object)]() { return object; }};
  }
}
}  // namespace GWT

#endif  // GWT_H
