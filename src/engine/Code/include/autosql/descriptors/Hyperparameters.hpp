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
    static SQLNET_INT calc_num_selected_features(
        const Poco::JSON::Object& _json_obj );

    /// Determines whether the _json_obj contains a features selector and
    /// returns it, if applicable.
    static containers::Optional<const Poco::JSON::Object>
    parse_feature_selector( const Poco::JSON::Object& _json_obj );

    /// Determines whether the _json_obj contains a predictor and
    /// returns it, if applicable.
    static containers::Optional<const Poco::JSON::Object> parse_predictor(
        const Poco::JSON::Object& _json_obj );

    /// Transforms the hyperparameters into a JSON object
    Poco::JSON::Object to_json_obj() const;

    // ------------------------------------------------------

    /// Transforms the hyperparameters into a JSON string
    std::string to_json() const { return JSON::stringify( to_json_obj() ); }

    // ------------------------------------------------------

    /// Describes the aggregations that may be used
    const std::vector<std::string> aggregations;

    /// Determines whether max_length_probe == max_length
    const bool fast_training;

    /// Hyperparameters for the feature selector (optional)
    const containers::Optional<const Poco::JSON::Object>
        feature_selector_hyperparams;

    /// The loss function to be used
    const std::string loss_function;

    /// The number of features to be extracted
    const SQLNET_INT num_features;

    /// The number of features to be selected. Must be smaller than
    /// num_features. When set to 0, then there is no feature selection.
    const SQLNET_INT num_selected_features;

    /// The number of subfeatures to be extracted. Only relevant for
    /// "snowflake" data model.
    const SQLNET_INT num_subfeatures;

    /// The number of threads to be used, 0 for automatic determination
    const SQLNET_INT num_threads;

    /// Hyperparameters for the predictor (optional)
    const containers::Optional<const Poco::JSON::Object> predictor_hyperparams;

    /// Whether you just want to select the features one by one
    const bool round_robin;

    /// Determines the sampling rate - parameter passed by the user
    const SQLNET_FLOAT sampling_factor;

    /// The share of sample from the population table included
    /// in the training process. This is the only non-const variable
    /// as it depends on the size of the training set.
    SQLNET_FLOAT sampling_rate;

    /// The seed used for the hyperparameter optimization
    const SQLNET_INT seed;

    /// The share of aggregations randomly selected
    const SQLNET_FLOAT share_aggregations;

    /// Shrinkage of learning rate
    const SQLNET_FLOAT shrinkage;

    /// Hyperparameters necessary for training the tree
    const TreeHyperparameters tree_hyperparameters;

    /// Whether time stamps are to be used (should be set to true
    /// for all but idiosyncratic cases - it is the golden rule of
    /// predictive analytics)!
    const bool use_timestamps;
};

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace autosql
#endif  // AUTOSQL_DESCRIPTORS_HYPERPARAMETERS_HPP_
