#ifndef RELMT_DECISIONTREES_DECISIONTREE_HPP_
#define RELMT_DECISIONTREES_DECISIONTREE_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <tuple>

// ----------------------------------------------------------------------------

#include "fastprop/subfeatures/subfeatures.hpp"
#include "helpers/helpers.hpp"

// ----------------------------------------------------------------------------

#include "relmt/Float.hpp"
#include "relmt/Hyperparameters.hpp"
#include "relmt/Int.hpp"
#include "relmt/containers/containers.hpp"
#include "relmt/lossfunctions/lossfunctions.hpp"
#include "relmt/utils/utils.hpp"

// ----------------------------------------------------------------------------

#include "relmt/decisiontrees/DecisionTreeNode.hpp"

// ----------------------------------------------------------------------------

namespace relmt {
namespace decisiontrees {
// ------------------------------------------------------------------------

class DecisionTree {
 public:
  DecisionTree(
      const std::shared_ptr<const Hyperparameters>& _hyperparameters,
      const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
      const size_t _peripheral_used,
      const std::shared_ptr<const utils::StandardScaler>& _output_scaler,
      const std::shared_ptr<const utils::StandardScaler>& _input_scaler,
      multithreading::Communicator* _comm);

  DecisionTree(
      const std::shared_ptr<const Hyperparameters>& _hyperparameters,
      const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
      const Poco::JSON::Object& _obj);

  ~DecisionTree() = default;

  // -----------------------------------------------------------------

 public:
  /// Calculates the column importances for this tree.
  void column_importances(utils::ImportanceMaker* _importance_maker) const;

  /// Fits the decision tree.
  void fit(const containers::DataFrameView& _output,
           const std::optional<containers::DataFrame>& _input,
           const containers::Subfeatures& _subfeatures,
           const containers::Rescaled& _output_rescaled,
           const containers::Rescaled& _input_rescaled,
           const std::vector<containers::Match>::iterator _begin,
           const std::vector<containers::Match>::iterator _end);

  /// Moves the column importances from the fast prop columns to the
  /// appropriate actual columns.
  void handle_fast_prop_importances(
      const fastprop::subfeatures::FastPropContainer& _fast_prop_container,
      const bool _is_subfeatures,
      utils::ImportanceMaker* _importance_maker) const;

  /// Expresses DecisionTree as Poco::JSON::Object.
  Poco::JSON::Object::Ptr to_json_obj() const;

  /// Transforms the data to form a prediction.
  std::shared_ptr<std::vector<Float>> transform(
      const containers::DataFrameView& _output,
      const containers::DataFrame& _input,
      const containers::Subfeatures& _subfeatures) const;

  /// Expresses the decision tree as SQL code.
  std::string to_sql(
      const helpers::StringIterator& _categories,
      const helpers::VocabularyTree& _vocabulary,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator,
      const std::string& _feature_prefix, const std::string _feature_num,
      const std::tuple<bool, bool, bool> _has_subfeatures) const;

  // -----------------------------------------------------------------

  /// Calculates the update rate.
  void calc_update_rate(const std::vector<Float>& _predictions) {
    update_rate_ = loss_function().calc_update_rate(_predictions);
  }

  /// Clears data no longer needed.
  void clear() { loss_function().clear(); }

  /// Trivial getter
  const Float intercept() const { return intercept_; }

  /// Trivial getter
  const size_t peripheral_used() const { return peripheral_used_; }

  /// Trivial setter.
  void set_comm(multithreading::Communicator* _comm) {
    comm_ = _comm;
    if (root_) {
      root_->set_comm(_comm);
    }
  }

  /// Trivial getter
  const Float update_rate() const { return update_rate_; }

 private:
  /// Calculates the weights for the root node.
  std::tuple<Float, Float, std::vector<Float>> calc_initial_weights(
      const containers::DataFrameView& _output,
      const containers::DataFrame& _input,
      const containers::Subfeatures& _subfeatures,
      const std::vector<containers::Match>::iterator _begin,
      const std::vector<containers::Match>::iterator _end);

  /// Parses the decision tree from a JSON object.
  void from_json_obj(
      const Poco::JSON::Object& _obj,
      const std::shared_ptr<lossfunctions::LossFunction>& _loss_function);

  /// We need to know which columns associated with the weights are actually
  /// time stamps, so we can correctly generate the SQLite3 code.
  std::vector<bool> make_is_ts(const containers::DataFrameView& _output,
                               const containers::DataFrame& _input) const;

  /// Returns a set of all the subfeatures used. This is required for the
  /// joins.
  std::set<size_t> make_subfeatures_used() const;

  // -----------------------------------------------------------------

 private:
  /// Trivial (private) accessor.
  multithreading::Communicator& comm() {
    assert_true(comm_ != nullptr);
    return *comm_;
  }

  /// Trivial (private) accessor
  const Hyperparameters& hyperparameters() const {
    assert_true(hyperparameters_);
    return *hyperparameters_;
  }

  /// Trivial (private) accessor
  const helpers::Schema& input() const {
    assert_true(input_);
    return *input_;
  }

  /// Trivial (private) accessor
  const utils::StandardScaler& input_scaler() const {
    assert_true(input_scaler_);
    return *input_scaler_;
  }

  /// Trivial (private) accessor
  lossfunctions::LossFunction& loss_function() {
    assert_true(loss_function_);
    return *loss_function_;
  }

  /// Trivial (private) accessor
  const lossfunctions::LossFunction& loss_function() const {
    assert_true(loss_function_);
    return *loss_function_;
  }

  /// Trivial (private) accessor
  const helpers::Schema& output() const {
    assert_true(output_);
    return *output_;
  }

  /// Trivial (private) accessor
  const utils::StandardScaler& output_scaler() const {
    assert_true(output_scaler_);
    return *output_scaler_;
  }

  // -----------------------------------------------------------------

 private:
  /// raw pointer to the communicator.
  multithreading::Communicator* comm_;

  /// Hyperparameters used to train the relmt model
  std::shared_ptr<const Hyperparameters> hyperparameters_;

  /// The loss reduction by the initial linear model.
  Float initial_loss_reduction_;

  /// The weights for the initial linear model.
  std::vector<Float> initial_weights_;

  /// The input table used (we keep it, because we need the colnames)
  std::shared_ptr<const helpers::Schema> input_;

  /// The scaler used for the input table.
  std::shared_ptr<const utils::StandardScaler> input_scaler_;

  /// The intercept term that is added after aggregation.
  Float intercept_;

  /// Determines whether the columns associated with the weights are time
  /// stamps.
  std::vector<bool> is_ts_;

  /// Loss function used to train the relmt model
  std::shared_ptr<lossfunctions::LossFunction> loss_function_;

  /// The output table used (we keep it, because we need the colnames)
  std::shared_ptr<const helpers::Schema> output_;

  /// The scaler used for the output table.
  std::shared_ptr<const utils::StandardScaler> output_scaler_;

  /// The peripheral table used.
  size_t peripheral_used_;

  /// The root of the decision tree.
  containers::Optional<DecisionTreeNode> root_;

  /// The update rate that is used when this tree is added to the prediction.
  Float update_rate_;

  // -----------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace relmt

// ----------------------------------------------------------------------------

#endif  // RELMT_DECISIONTREES_DECISIONTREE_HPP_
