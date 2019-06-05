#ifndef AUTOSQL_DESCRIPTORS_XGBOOSTHYPERPARMS_HPP_
#define AUTOSQL_DESCRIPTORS_XGBOOSTHYPERPARMS_HPP_

namespace autosql
{
namespace descriptors
{
// ----------------------------------------------------------------------

/// Abstract base class for a predictor
struct XGBoostHyperparams
{
    // -----------------------------------------

    XGBoostHyperparams( const Poco::JSON::Object &_json_obj );

    ~XGBoostHyperparams() = default;

    // -----------------------------------------

    /// L1 regularization term on weights
    const SQLNET_FLOAT alpha;

    /// Specify which booster to use: gbtree, gblinear or dart.
    const std::string booster;

    /// Subsample ratio of columns for each split, in each level.
    const SQLNET_FLOAT colsample_bylevel;

    /// Subsample ratio of columns when constructing each tree.
    const SQLNET_FLOAT colsample_bytree;

    /// Boosting learning rate
    const SQLNET_FLOAT eta;

    /// Minimum loss reduction required to make a further partition on a leaf
    /// node of the tree.
    const SQLNET_FLOAT gamma;

    /// L2 regularization term on weights
    const SQLNET_FLOAT lambda;

    /// Maximum delta step we allow each tree’s weight estimation to be.
    const SQLNET_FLOAT max_delta_step;

    /// Maximum tree depth for base learners
    const SQLNET_INT max_depth;

    /// Minimum sum of instance weight needed in a child
    const SQLNET_FLOAT min_child_weights;

    /// Number of iterations (number of trees in boosted ensemble)
    const SQLNET_INT n_iter;

    /// For dart only. Which normalization to use.
    const std::string normalize_type;

    /// ...
    const SQLNET_INT num_parallel_tree;

    /// Number of parallel threads used to run xgboost
    const SQLNET_INT nthread;

    /// The objective for the learning function.
    const std::string objective;

    /// For dart only. If true, at least one tree will be dropped out.
    const bool one_drop;

    /// For dart only. Dropout rate.
    const SQLNET_FLOAT rate_drop;

    /// For dart only. Whether you want to use "uniform" or "weighted"
    /// sampling
    const std::string sample_type;

    /// Whether to print messages while running boosting
    const bool silent;

    /// For dart only. Probability of skipping dropout.
    const SQLNET_FLOAT skip_drop;

    /// Subsample ratio of the training instance.
    const SQLNET_FLOAT subsample;
};

// ------------------------------------------------------------------------
}
}

#endif  // AUTOSQL_DESCRIPTORS_XGBOOSTHYPERPARMS_HPP_
