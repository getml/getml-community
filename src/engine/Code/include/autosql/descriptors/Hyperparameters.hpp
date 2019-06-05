#ifndef AUTOSQL_DESCRIPTORS_HYPERPARAMETERS_HPP_
#define AUTOSQL_DESCRIPTORS_HYPERPARAMETERS_HPP_

namespace autosql
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

    /// Calculates the number of selected features.
    static size_t calc_num_selected_features(
        const Poco::JSON::Object& _json_obj );

    /// Transforms the hyperparameters into a JSON object
    Poco::JSON::Object to_json_obj() const;

    // ------------------------------------------------------

    /// Transforms the hyperparameters into a JSON string
    std::string to_json() const { return JSON::stringify( to_json_obj() ); }

    // ------------------------------------------------------

    /// Describes the aggregations that may be used
    const std::vector<std::string> aggregations_;

    /// Determines whether max_length_probe == max_length
    const bool fast_training_;

    /// The loss function to be used
    const std::string loss_function_;

    /// The number of features to be extracted
    const size_t num_features_;

    /// The number of features to be selected. Must be smaller than
    /// num_features. When set to 0, then there is no feature selection.
    const size_t num_selected_features_;

    /// The number of subfeatures to be extracted. Only relevant for
    /// "snowflake" data model.
    const size_t num_subfeatures_;

    /// The number of threads to be used, 0 for automatic determination
    const size_t num_threads_;

    /// Whether you just want to select the features one by one
    const bool round_robin_;

    /// Determines the sampling rate - parameter passed by the user
    const AUTOSQL_FLOAT sampling_factor_;

    /// The share of sample from the population table included
    /// in the training process. This is the only non-const variable
    /// as it depends on the size of the training set.
    AUTOSQL_FLOAT sampling_rate_;

    /// The seed used for the hyperparameter optimization
    const size_t seed_;

    /// The share of aggregations randomly selected
    const AUTOSQL_FLOAT share_aggregations_;

    /// Shrinkage of learning rate
    const AUTOSQL_FLOAT shrinkage_;

    /// Hyperparameters necessary for training the tree
    const std::shared_ptr<const TreeHyperparameters> tree_hyperparameters_;

    /// Whether time stamps are to be used (should be set to true
    /// for all but idiosyncratic cases - it is the golden rule of
    /// predictive analytics)!
    const bool use_timestamps_;
};

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace autosql

#endif  // AUTOSQL_DESCRIPTORS_HYPERPARAMETERS_HPP_
