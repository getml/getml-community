#ifndef ENGINE_PREDICTORS_XGBOOSTHYPERPARMS_HPP_
#define ENGINE_PREDICTORS_XGBOOSTHYPERPARMS_HPP_

namespace engine
{
namespace predictors
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
    const ENGINE_FLOAT alpha_;

    /// Specify which booster to use: gbtree, gblinear or dart.
    const std::string booster_;

    /// Subsample ratio of columns for each split, in each level.
    const ENGINE_FLOAT colsample_bylevel_;

    /// Subsample ratio of columns when constructing each tree.
    const ENGINE_FLOAT colsample_bytree_;

    /// Boosting learning rate
    const ENGINE_FLOAT eta_;

    /// Minimum loss reduction required to make a further partition on a leaf
    /// node of the tree.
    const ENGINE_FLOAT gamma_;

    /// L2 regularization term on weights
    const ENGINE_FLOAT lambda_;

    /// Maximum delta step we allow each tree’s weight estimation to be.
    const ENGINE_FLOAT max_delta_step_;

    /// Maximum tree depth for base learners
    const size_t max_depth_;

    /// Minimum sum of instance weight needed in a child
    const ENGINE_FLOAT min_child_weights_;

    /// Number of iterations (number of trees in boosted ensemble)
    const size_t n_iter_;

    /// For dart only. Which normalization to use.
    const std::string normalize_type_;

    /// ...
    const size_t num_parallel_tree_;

    /// Number of parallel threads used to run xgboost
    const size_t nthread_;

    /// The objective for the learning function.
    const std::string objective_;

    /// For dart only. If true, at least one tree will be dropped out.
    const bool one_drop_;

    /// For dart only. Dropout rate.
    const ENGINE_FLOAT rate_drop_;

    /// For dart only. Whether you want to use "uniform" or "weighted"
    /// sampling
    const std::string sample_type_;

    /// Whether to print messages while running boosting
    const bool silent_;

    /// For dart only. Probability of skipping dropout.
    const ENGINE_FLOAT skip_drop_;

    /// Subsample ratio of the training instance.
    const ENGINE_FLOAT subsample_;
};

// ------------------------------------------------------------------------
}  // namespace predictors
}  // namespace engine

#endif  // ENGINE_PREDICTORS_XGBOOSTHYPERPARMS_HPP_
