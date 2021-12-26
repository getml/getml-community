#ifndef MULTIREL_UTILS_LINEARREGRESSION_HPP_
#define MULTIREL_UTILS_LINEARREGRESSION_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include <vector>

// ----------------------------------------------------------------------------

#include "multithreading/multithreading.hpp"

// ----------------------------------------------------------------------------

#include "multirel/Float.hpp"

// ----------------------------------------------------------------------------

namespace multirel {
namespace utils {
// ----------------------------------------------------------------------------

class LinearRegression {
 public:
  explicit LinearRegression(multithreading::Communicator* _comm)
      : comm_(_comm) {}

  LinearRegression(const Poco::JSON::Object& _obj,
                   multithreading::Communicator* _comm)
      : comm_(_comm) {
    *this = from_json_obj(_obj, comm_);
  }

  ~LinearRegression() = default;

  // -----------------------------------------

  // Fits a simple linear regression on each column
  // of _residuals w.r.t. _yhat, which has only one column
  void fit(const std::vector<Float>& _yhat,
           const std::vector<std::vector<Float>>& _residuals,
           const std::vector<Float>& _sample_weights);

  // Generates predictions based on _yhat
  std::vector<std::vector<Float>> predict(
      const std::vector<Float>& _yhat) const;

  /// Transforms the LinearRegression into a JSON object.
  Poco::JSON::Object to_json_obj() const;

  // -----------------------------------------

 private:
  /// Reconstructs a LinearRegression from a JSON object.
  LinearRegression from_json_obj(const Poco::JSON::Object& _obj,
                                 multithreading::Communicator* _comm) const;

  // -----------------------------------------

 private:
  // Communicator object
  multithreading::Communicator* comm_;

  // Zero intercepts or biases of the linear regression
  std::vector<Float> intercepts_;

  // Slope parameters of the linear regression
  std::vector<Float> slopes_;

  // -----------------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace utils
}  // namespace multirel

#endif  // MULTIREL_UTILS_LINEARREGRESSION_HPP_
