#ifndef RELBOOST_LOSSFUNCTIONS_LOSSFUNCTIONS_HPP_
#define RELBOOST_LOSSFUNCTIONS_LOSSFUNCTIONS_HPP_

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

#include "relboost/Float.hpp"
#include "relboost/Int.hpp"

#include "relboost/Hyperparameters.hpp"

#include "relboost/containers/containers.hpp"

#include "relboost/enums/enums.hpp"

#include "relboost/utils/utils.hpp"

// ----------------------------------------------------------------------------

#include "relboost/lossfunctions/LossFunction.hpp"

#include "relboost/lossfunctions/LossFunctionImpl.hpp"

#include "relboost/lossfunctions/CrossEntropyLoss.hpp"
#include "relboost/lossfunctions/SquareLoss.hpp"

#include "relboost/lossfunctions/LossFunctionParser.hpp"

// ----------------------------------------------------------------------------

#endif  // RELBOOST_LOSSFUNCTIONS_LOSSFUNCTIONS_HPP_
