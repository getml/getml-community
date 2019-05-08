#ifndef RELBOOST_HYPERPARAMETERS_HPP_
#define RELBOOST_HYPERPARAMETERS_HPP_

// ----------------------------------------------------------------------------
// Dependencies

#include <string>

#include <Poco/JSON/Object.h>

#include "relboost/types.hpp"

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
    // const RELBOOST_FLOAT alpha;

    /// Specify which booster to use: gbtree, gblinear or dart.
    // const std::string booster;

    /// Subsample ratio of columns for each split, in each level.
    // const RELBOOST_FLOAT colsample_bylevel;

    /// Subsample ratio of columns when constructing each tree.
    // const RELBOOST_FLOAT colsample_bytree;

    /// Boosting learning rate
    const RELBOOST_FLOAT eta_;

    /// Minimum loss reduction required to make a further partition on a leaf
    /// node of the tree.
    const RELBOOST_FLOAT gamma_;

    /// L2 regularization term on weights
    const RELBOOST_FLOAT lambda_;

    /// Maximum delta step we allow each tree’s weight estimation to be.
    // const RELBOOST_FLOAT max_delta_step;

    /// Maximum tree depth for base learners
    const RELBOOST_INT max_depth_;

    /// Minimum sum of instance weight needed in a child
    // const RELBOOST_FLOAT min_child_weights;

    /// Number of features (number of trees in boosted ensemble)
    const RELBOOST_INT num_features_;

    /// ...
    // const RELBOOST_INT num_parallel_tree;

    /// Number of parallel threads used to run xgboost
    const RELBOOST_INT num_threads_;

    /// The objective for the learning function.
    const std::string objective_;

    /// The seed used for initializing the random number generator.
    const size_t seed_;

    /// Whether to print messages while running boosting
    const bool silent_;

    /// Subsample ratio of the training instance.
    const RELBOOST_FLOAT subsample_;

    /// Whether to use timestamps,
    const bool use_timestamps_;
};

// ----------------------------------------------------------------------------
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_HYPERPARAMETERS_HPP_