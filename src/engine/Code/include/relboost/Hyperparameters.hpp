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

    /// Transforms the Hyperparameters into a JSON object
    Poco::JSON::Object to_json_obj() const;

    // --------------------------------------------------------

    /// Transforms the Hyperparameters into a JSON string
    std::string to_json() const { return JSON::stringify( to_json_obj() ); }

    // -----------------------------------------------------------------

    /// L1 regularization term on weights
    // const Float alpha;

    /// Specify which booster to use: gbtree, gblinear or dart.
    // const std::string booster;

    /// Subsample ratio of columns for each split, in each level.
    // const Float colsample_bylevel;

    /// Subsample ratio of columns when constructing each tree.
    // const Float colsample_bytree;

    /// Boosting learning rate
    const Float eta_;

    /// Minimum loss reduction required to make a further partition on a leaf
    /// node of the tree.
    const Float gamma_;

    /// Whether we want categorical columns from the population table to be
    /// included in our prediction.
    const bool include_categorical_;

    /// L2 regularization term on weights
    const Float lambda_;

    /// Maximum delta step we allow each tree’s weight estimation to be.
    // const Float max_delta_step;

    /// Maximum tree depth for base learners
    const Int max_depth_;

    /// Minimum number of samples.
    const Int min_num_samples_;

    /// Number of features (number of trees in boosted ensemble)
    const Int num_features_;

    /// Number of features to be selected.
    const Int num_selected_features_;

    /// ...
    // const Int num_parallel_tree;

    /// Number of parallel threads used to run xgboost
    const Int num_threads_;

    /// The objective for the learning function.
    const std::string objective_;

    /// Proportional to the subsample ratio.
    const Float sampling_factor_;

    /// The seed used for initializing the random number generator.
    const size_t seed_;

    /// Whether to print messages while running boosting
    const bool silent_;

    /// Whether to use timestamps,
    const bool use_timestamps_;
};

// ----------------------------------------------------------------------------
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_HYPERPARAMETERS_HPP_
