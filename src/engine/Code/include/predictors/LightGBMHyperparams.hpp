#ifndef PREDICTORS_LIGHTGBMHYPERPARMS_HPP_
#define PREDICTORS_LIGHTGBMHYPERPARMS_HPP_

namespace predictors
{
// ----------------------------------------------------------------------

/// Hyperparameters for LightGBM.
struct LightGBMHyperparams
{
    // -----------------------------------------

    LightGBMHyperparams( const Poco::JSON::Object &_json_obj );

    ~LightGBMHyperparams() = default;

    // -----------------------------------------

    /// "gbdt", traditional Gradient Boosting Decision Tree.
    /// "dart", Dropouts meet Multiple Additive Regression Trees.
    /// "goss", Gradient-based One-Side Sampling.
    /// "rf", Random Forest.
    const std::string boosting_type_;

    /// Subsample ratio of the columns for each tree.
    const Float colsample_bytree_;

    /// Boosting learning rate.
    const Float learning_rate_;

    /// Maximum tree depth for base learners, <=0 means no limit.
    const Int max_depth_;

    /// Minimum number of samples needed in a child
    const Int min_child_samples_;

    /// Minimum sum of instance weights needed in a child
    const Float min_child_weight_;

    /// Minimum gain required for a split.
    const Float min_split_gain_;

    /// Number of estimators (number of trees in boosted ensemble)
    const Int n_estimators_;

    /// Maximum tree leaves for base learners.
    const Int num_leaves_;

    /// Number of parallel threads used to run LightGBM.
    const Int n_jobs_;

    /// Specify the learning task and the corresponding learning
    /// objective or a custom objective function to be used (see note below).
    /// Default:
    /// ‘regression’ for LGBMRegressor,
    /// ‘binary’ or ‘multiclass’ for LGBMClassifier,
    /// ‘lambdarank’ for LGBMRanker.
    const std::string objective_;

    /// Seed for subsampling.
    const Int random_state_;

    /// L1 regularization term.
    const Float reg_alpha_;

    /// L2 regularization term.
    const Float reg_lambda_;

    /// Whether to print messages while running boosting
    const bool silent_;

    /// Subsample ratio.
    const Float subsample_;

    /// Number of samples for constructing bins.
    const Int subsample_for_bin_;

    /// Subsample frequency. <=0 means no enable.
    const Int subsample_freq_;
};

// ------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_LIGHTGBMHYPERPARMS_HPP_
