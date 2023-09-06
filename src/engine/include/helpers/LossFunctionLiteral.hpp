#ifndef HELPERS_LOSSFUNCTIONLITERAL_HPP_
#define HELPERS_LOSSFUNCTIONLITERAL_HPP_

#include "rfl/Literal.hpp"

namespace helpers {

using LossFunctionLiteral = rfl::Literal<"CrossEntropyLoss", "SquareLoss">;

}  // namespace helpers

#endif  // HELPERS_AGGREGATIONS_HPP_
