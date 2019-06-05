#ifndef AUTOSQL_DESCRIPTORS_SCORES_HPP_
#define AUTOSQL_DESCRIPTORS_SCORES_HPP_

namespace autosql
{
namespace descriptors
{
// ----------------------------------------------------------------------------

/// Scores are measure by which the predictive performance of the model is
/// evaluated.
class Scores
{
    // ------------------------------------------------------
   public:
    Scores() {}

    Scores( const Poco::JSON::Object& _json_obj ) : Scores()
    {
        from_json_obj( _json_obj );
    }

    ~Scores() = default;

    // ------------------------------------------------------

   public:
    /// Parses a JSON object
    void from_json_obj( const Poco::JSON::Object& _json_obj );

    /// Transforms the Scores into a JSON object
    Poco::JSON::Object to_json_obj() const;

    // ------------------------------------------------------

   public:
    /// Trivial accessor
    std::vector<std::vector<AUTOSQL_FLOAT>>& accuracy_curves()
    {
        return accuracy_curves_;
    }

    /// Trivial accessor
    const std::vector<std::vector<AUTOSQL_FLOAT>>& accuracy_curves() const
    {
        return accuracy_curves_;
    }

    /// Trivial accessor
    std::vector<std::vector<std::vector<AUTOSQL_FLOAT>>>& average_targets()
    {
        return average_targets_;
    }

    /// Trivial accessor
    const std::vector<std::vector<std::vector<AUTOSQL_FLOAT>>>& average_targets()
        const
    {
        return average_targets_;
    }

    /// Trivial accessor
    std::vector<std::vector<AUTOSQL_FLOAT>>& feature_correlations()
    {
        return feature_correlations_;
    }

    /// Trivial accessor
    const std::vector<std::vector<AUTOSQL_FLOAT>>& feature_correlations() const
    {
        return feature_correlations_;
    }

    /// Trivial accessor
    std::vector<std::vector<AUTOSQL_INT>>& feature_densities()
    {
        return feature_densities_;
    }

    /// Trivial accessor
    const std::vector<std::vector<AUTOSQL_INT>>& feature_densities() const
    {
        return feature_densities_;
    }

    /// Trivial accessor
    std::vector<std::vector<AUTOSQL_FLOAT>>& feature_importances()
    {
        return feature_importances_;
    }

    /// Trivial accessor
    const std::vector<std::vector<AUTOSQL_FLOAT>>& feature_importances() const
    {
        return feature_importances_;
    }

    /// Trivial accessor
    std::vector<std::vector<AUTOSQL_FLOAT>>& fpr() { return fpr_; }

    /// Trivial accessor
    const std::vector<std::vector<AUTOSQL_FLOAT>>& fpr() const { return fpr_; }

    /// Trivial accessor
    std::vector<std::vector<AUTOSQL_FLOAT>>& labels() { return labels_; }

    /// Trivial accessor
    const std::vector<std::vector<AUTOSQL_FLOAT>>& labels() const
    {
        return labels_;
    }

    /// Trivial accessor
    std::vector<std::vector<AUTOSQL_FLOAT>>& tpr() { return tpr_; }

    /// Trivial accessor
    const std::vector<std::vector<AUTOSQL_FLOAT>>& tpr() const { return tpr_; }

    // ------------------------------------------------------

   private:
    /// Accuracy
    std::vector<AUTOSQL_FLOAT> accuracy_;

    /// The accuracy curves feeding the accuracy scores.
    std::vector<std::vector<AUTOSQL_FLOAT>> accuracy_curves_;

    /// Area under curve
    std::vector<AUTOSQL_FLOAT> auc_;

    /// Average of targets w.r.t. different bins of the feature.
    std::vector<std::vector<std::vector<AUTOSQL_FLOAT>>> average_targets_;

    /// Logarithm of likelihood of predictions
    std::vector<AUTOSQL_FLOAT> cross_entropy_;

    /// Correlations coefficients of features with targets.
    std::vector<std::vector<AUTOSQL_FLOAT>> feature_correlations_;

    /// Densities of the features.
    std::vector<std::vector<AUTOSQL_INT>> feature_densities_;

    /// Importances of individual features w.r.t. targets.
    std::vector<std::vector<AUTOSQL_FLOAT>> feature_importances_;

    /// False positive rate.
    std::vector<std::vector<AUTOSQL_FLOAT>> fpr_;

    /// Min, max and step_size for feature_densities and average targets.
    std::vector<std::vector<AUTOSQL_FLOAT>> labels_;

    /// Mean absolute error
    std::vector<AUTOSQL_FLOAT> mae_;

    /// Minimum prediction - needed for plotting the accuracy.
    std::vector<AUTOSQL_FLOAT> prediction_min_;

    /// Stepsize - needed for plotting the accuracy.
    std::vector<AUTOSQL_FLOAT> prediction_step_size_;

    /// Root mean squared error
    std::vector<AUTOSQL_FLOAT> rmse_;

    /// Predictive rsquared
    std::vector<AUTOSQL_FLOAT> rsquared_;

    /// True positive rate.
    std::vector<std::vector<AUTOSQL_FLOAT>> tpr_;
};

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace autosql

#endif  // AUTOSQL_DESCRIPTORS_SCORES_HPP_
