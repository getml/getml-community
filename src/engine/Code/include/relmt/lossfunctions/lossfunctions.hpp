#ifndef RELMT_LOSSFUNCTIONS_LOSSFUNCTIONS_HPP_
#define RELMT_LOSSFUNCTIONS_LOSSFUNCTIONS_HPP_

// ----------------------------------------------------------------------------
// Dependencies

#include <cmath>

#include <algorithm>
#include <array>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include <Eigen/Dense>

#include "multithreading/multithreading.hpp"

#include "relmt/Float.hpp"
#include "relmt/Int.hpp"

#include "relmt/Hyperparameters.hpp"

#include "relmt/containers/containers.hpp"

#include "relmt/enums/enums.hpp"

#include "relmt/utils/utils.hpp"

// ----------------------------------------------------------------------------

#include "relmt/lossfunctions/LossFunction.hpp"

#include "relmt/lossfunctions/LossFunctionImpl.hpp"

#include "relmt/lossfunctions/CrossEntropyLoss.hpp"
#include "relmt/lossfunctions/SquareLoss.hpp"

#include "relmt/lossfunctions/LossFunctionParser.hpp"

// ----------------------------------------------------------------------------

#endif  // RELMT_LOSSFUNCTIONS_LOSSFUNCTIONS_HPP_
