#ifndef MULTIREL_DESCRIPTORS_HYPERPARAMETERS_HPP_
#define MULTIREL_DESCRIPTORS_HYPERPARAMETERS_HPP_

namespace multirel
{
namespace descriptors
{
// ----------------------------------------------------------------------------
// All hyperparameters

struct Hyperparameters
{
    // ------------------------------------------------------

    Hyperparameters( const Poco::JSON::Object& _json_obj );

    ~Hyperparameters() = default;

    // ------------------------------------------------------

    /// Transforms the hyperparameters into a JSON object
    Poco::JSON::Object::Ptr to_json_obj() const;

    // ------------------------------------------------------

    /// Calculates the absolute number of selected features.
    size_t num_selected_features() const;

    /// Transforms the hyperparameters into a JSON string
    std::string to_json() const { return JSON::stringify( *to_json_obj() ); }

    // ------------------------------------------------------

    /// Describes the aggregations that may be used
    const std::vector<std::string> aggregations_;

    /// Whether we want categorical columns from the population table to be
    /// included in our prediction.
    const bool include_categorical_;

    /// Stores the hyperparameters of the feature selector - needed for .refresh
    /// to work properly.
    const Poco::JSON::Object::Ptr feature_selector_;

    /// The loss function to be used
    const std::string loss_function_;

    /// The number of features to be extracted
    const size_t num_features_;

    /// The number of subfeatures to be extracted. Only relevant for
    /// "snowflake" data model.
    const size_t num_subfeatures_;

    /// The number of threads to be used, 0 for automatic determination
    const size_t num_threads_;

    /// Stores the hyperparameters of the predictor - needed for .refresh
    /// to work properly.
    const Poco::JSON::Object::Ptr predictor_;

    /// Whether you just want to select the features one by one
    const bool round_robin_;

    /// Determines the sampling rate - parameter passed by the user
    const Float sampling_factor_;

    /// The seed used for sampling.
    const unsigned int seed_;

    /// The share of aggregations randomly selected
    const Float share_aggregations_;

    /// The share of features to be selected.
    /// When set to 0, then there is no feature selection.
    const Float share_selected_features_;

    /// Shrinkage of learning rate
    const Float shrinkage_;

    /// Whether we want to print out the "Trained FEATURE_..." message
    const bool silent_;

    /// Hyperparameters necessary for training the tree
    const std::shared_ptr<const TreeHyperparameters> tree_hyperparameters_;

    /// Whether time stamps are to be used (should be set to true
    /// for all but idiosyncratic cases - it is the golden rule of
    /// predictive analytics)!
    const bool use_timestamps_;
};

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace multirel

#endif  // MULTIREL_DESCRIPTORS_HYPERPARAMETERS_HPP_
