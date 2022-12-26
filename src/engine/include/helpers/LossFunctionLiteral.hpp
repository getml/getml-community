#ifndef HELPERS_LOSSFUNCTIONLITERAL_HPP_
#define HELPERS_LOSSFUNCTIONLITERAL_HPP_

#include "fct/Literal.hpp"

namespace helpers {

using LossFunctionLiteral = fct::Literal<"CrossEntropyLoss", "SquareLoss">;

}  // namespace helpers

#endif  // HELPERS_AGGREGATIONS_HPP_
