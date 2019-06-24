#ifndef METRICS_SCORES_HPP_
#define METRICS_SCORES_HPP_

namespace metrics
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
    /// Parses a JSON object.
    void from_json_obj( const Poco::JSON::Object& _json_obj );

    /// Saves the scores to a JSON file.
    void save( const std::string& _fname ) const;

    /// Transforms the Scores into a JSON object.
    Poco::JSON::Object to_json_obj() const;

    // ------------------------------------------------------

   public:
    /// Trivial accessor
    std::vector<std::vector<Float>>& accuracy_curves()
    {
        return accuracy_curves_;
    }

    /// Trivial accessor
    const std::vector<std::vector<Float>>& accuracy_curves() const
    {
        return accuracy_curves_;
    }

    /// Trivial accessor
    std::vector<std::vector<std::vector<Float>>>& average_targets()
    {
        return average_targets_;
    }

    /// Trivial accessor
    const std::vector<std::vector<std::vector<Float>>>& average_targets() const
    {
        return average_targets_;
    }

    /// Trivial accessor
    std::vector<std::vector<Float>>& feature_correlations()
    {
        return feature_correlations_;
    }

    /// Trivial accessor
    const std::vector<std::vector<Float>>& feature_correlations() const
    {
        return feature_correlations_;
    }

    /// Trivial accessor
    std::vector<std::vector<Int>>& feature_densities()
    {
        return feature_densities_;
    }

    /// Trivial accessor
    const std::vector<std::vector<Int>>& feature_densities() const
    {
        return feature_densities_;
    }

    /// Trivial accessor
    std::vector<std::vector<Float>>& feature_importances()
    {
        return feature_importances_;
    }

    /// Trivial accessor
    const std::vector<std::vector<Float>>& feature_importances() const
    {
        return feature_importances_;
    }

    /// Trivial accessor
    std::vector<std::string>& feature_names() { return feature_names_; }

    /// Trivial accessor
    const std::vector<std::string>& feature_names() const
    {
        return feature_names_;
    }

    /// Trivial accessor
    std::vector<std::vector<Float>>& fpr() { return fpr_; }

    /// Trivial accessor
    const std::vector<std::vector<Float>>& fpr() const { return fpr_; }

    /// Trivial accessor
    std::vector<std::vector<Float>>& labels() { return labels_; }

    /// Trivial accessor
    const std::vector<std::vector<Float>>& labels() const { return labels_; }

    /// Trivial accessor
    std::vector<std::vector<Float>>& tpr() { return tpr_; }

    /// Trivial accessor
    const std::vector<std::vector<Float>>& tpr() const { return tpr_; }

    // ------------------------------------------------------

   private:
    /// Accuracy
    std::vector<Float> accuracy_;

    /// The accuracy curves feeding the accuracy scores.
    std::vector<std::vector<Float>> accuracy_curves_;

    /// Area under curve
    std::vector<Float> auc_;

    /// Average of targets w.r.t. different bins of the feature.
    std::vector<std::vector<std::vector<Float>>> average_targets_;

    /// Logarithm of likelihood of predictions
    std::vector<Float> cross_entropy_;

    /// Correlations coefficients of features with targets.
    std::vector<std::vector<Float>> feature_correlations_;

    /// Densities of the features.
    std::vector<std::vector<Int>> feature_densities_;

    /// Importances of individual features w.r.t. targets.
    std::vector<std::vector<Float>> feature_importances_;

    /// The names of the features.
    std::vector<std::string> feature_names_;

    /// False positive rate.
    std::vector<std::vector<Float>> fpr_;

    /// Min, max and step_size for feature_densities and average targets.
    std::vector<std::vector<Float>> labels_;

    /// Mean absolute error
    std::vector<Float> mae_;

    /// Minimum prediction - needed for plotting the accuracy.
    std::vector<Float> prediction_min_;

    /// Stepsize - needed for plotting the accuracy.
    std::vector<Float> prediction_step_size_;

    /// Root mean squared error
    std::vector<Float> rmse_;

    /// Predictive rsquared
    std::vector<Float> rsquared_;

    /// True positive rate.
    std::vector<std::vector<Float>> tpr_;
};

// ----------------------------------------------------------------------------
}  // namespace metrics

#endif  // METRICS_SCORES_HPP_
