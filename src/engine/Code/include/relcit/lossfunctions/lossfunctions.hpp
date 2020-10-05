#ifndef RELBOOSTXX_LOSSFUNCTIONS_LOSSFUNCTIONS_HPP_
#define RELBOOSTXX_LOSSFUNCTIONS_LOSSFUNCTIONS_HPP_

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

#include "relcit/Float.hpp"
#include "relcit/Int.hpp"

#include "relcit/Hyperparameters.hpp"

#include "relcit/containers/containers.hpp"

#include "relcit/enums/enums.hpp"

#include "relcit/utils/utils.hpp"

// ----------------------------------------------------------------------------

#include "relcit/lossfunctions/LossFunction.hpp"

#include "relcit/lossfunctions/LossFunctionImpl.hpp"

#include "relcit/lossfunctions/CrossEntropyLoss.hpp"
#include "relcit/lossfunctions/SquareLoss.hpp"

#include "relcit/lossfunctions/LossFunctionParser.hpp"

// ----------------------------------------------------------------------------

#endif  // RELBOOSTXX_LOSSFUNCTIONS_LOSSFUNCTIONS_HPP_
