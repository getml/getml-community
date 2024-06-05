#include <tuple>

#include "xgboost/context.h"

auto main() -> int {
  auto const ctx = xgboost::Context{};
  std::ignore = ctx.Device() == xgboost::DeviceOrd::CPU();
  std::ignore = ctx.Ordinal() == xgboost::Context::kCpuId;
}
