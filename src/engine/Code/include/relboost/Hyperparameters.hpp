#ifndef RELBOOST_HYPERPARAMETERS_HPP_
#define RELBOOST_HYPERPARAMETERS_HPP_

// ----------------------------------------------------------------------------
// Dependencies

#include <string>

#include <Poco/JSON/Object.h>

#include "relboost/Float.hpp"
#include "relboost/Int.hpp"

#include "relboost/JSON.hpp"

// ----------------------------------------------------------------------------

namespace relboost
{
// ----------------------------------------------------------------------------

struct Hyperparameters
{
    // -----------------------------------------------------------------

    Hyperparameters();

    Hyperparameters( const Poco::JSON::Object& _obj );

    Hyperparameters( const Poco::JSON::Object::Ptr& _obj )
        : Hyperparameters( *_obj )
    {
    }

    ~Hyperparameters() = default;

    // --------------------------------------------------------

    /// Calculates the absolute number of selected features.
    size_t num_selected_features() const;

    /// Transforms the Hyperparameters into a JSON object
    Poco::JSON::Object::Ptr to_json_obj() const;

    // --------------------------------------------------------

    /// Transforms the Hyperparameters into a JSON string
    std::string to_json() const { return JSON::stringify( *to_json_obj() ); }

    // -----------------------------------------------------------------

    /// L1 regularization term on weights
    // const Float alpha;

    /// Specify which booster to use: gbtree, gblinear or dart.
    // const std::string booster;

    /// Subsample ratio of columns for each split, in each level.
    // const Float colsample_bylevel;

    /// Subsample ratio of columns when constructing each tree.
    // const Float colsample_bytree;

    /// The lag variable used for calculating the moving time windows. When set
    /// to 0.0 or negative value, moving time windows will be omitted.
    const Float delta_t_;

    /// Stores the hyperparameters of the feature selector - needed for .refresh
    /// to work properly.
    const Poco::JSON::Object::Ptr feature_selector_;

    /// Minimum loss reduction required to make a further partition on a leaf
    /// node of the tree.
    const Float gamma_;

    /// Whether we want categorical columns from the population table to be
    /// included in our prediction.
    const bool include_categorical_;

    /// The loss function used for the learning function.
    const std::string loss_function_;

    /// Maximum delta step we allow each tree’s weight estimation to be.
    // const Float max_delta_step;

    /// Maximum tree depth for base learners
    const Int max_depth_;

    /// Minimum number of samples.
    const Int min_num_samples_;

    /// Number of features (number of trees in boosted ensemble)
    const Int num_features_;

    /// Number of subfeatures
    const Int num_subfeatures_;

    /// ...
    // const Int num_parallel_tree;

    /// Number of parallel threads used to run xgboost
    const Int num_threads_;

    /// Stores the hyperparameters of the predictor - needed for .refresh
    /// to work properly.
    const Poco::JSON::Object::Ptr predictor_;

    /// L2 regularization term on weights
    const Float reg_lambda_;

    /// Proportional to the subsample ratio.
    const Float sampling_factor_;

    /// The seed used for initializing the random number generator.
    const unsigned int seed_;

    /// The session name is used to identify models belonging to a particular
    /// hyperparameter optimization. It is therefore not required for normal
    /// training and one of the few parameters that are optional.
    const std::string session_name_;

    /// The share of features to be selected.
    /// When set to 0, then there is no feature selection.
    const Float share_selected_features_;

    /// Boosting learning rate
    const Float shrinkage_;

    /// Whether to print messages while running boosting
    const bool silent_;

    /// The target on which to train.
    const Int target_num_;

    /// Whether to use timestamps,
    const bool use_timestamps_;
};

// ----------------------------------------------------------------------------
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_HYPERPARAMETERS_HPP_
