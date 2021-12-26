#ifndef FASTPROP_HYPERPARAMETERS_HPP_
#define FASTPROP_HYPERPARAMETERS_HPP_

#include "jsonutils/jsonutils.hpp"

#include "fastprop/Float.hpp"
#include "fastprop/Int.hpp"

namespace fastprop
{
// ----------------------------------------------------------------------------

struct Hyperparameters
{
    // ------------------------------------------------------

    static constexpr const char* CROSS_ENTROPY_LOSS = "CrossEntropyLoss";
    static constexpr const char* SQUARE_LOSS = "SquareLoss";

    // ------------------------------------------------------

    Hyperparameters( const Poco::JSON::Object& _json_obj );

    ~Hyperparameters() = default;

    // ------------------------------------------------------

    /// Transforms the hyperparameters into a JSON object
    Poco::JSON::Object::Ptr to_json_obj() const;

    // ------------------------------------------------------

    /// Transforms the hyperparameters into a JSON string
    std::string to_json() const
    {
        return jsonutils::JSON::stringify( *to_json_obj() );
    }

    // ------------------------------------------------------

    /// Describes the aggregations that may be used
    const std::vector<std::string> aggregations_;

    /// Size of the moving time windows.
    const Float delta_t_;

    /// The loss function (FastProp is completely unsupervised, so we simply
    /// have this for consistency).
    const std::string loss_function_;

    /// The maximum lag.
    const size_t max_lag_;

    /// The minimum document frequency required for a string to become part of
    /// the vocabulary.
    const size_t min_df_;

    /// The number of categories from which we would like to extract numerical
    /// features.
    const size_t n_most_frequent_;

    /// The maximum number of features generated.
    const size_t num_features_;

    /// The number of threads we want to use
    const Int num_threads_;

    /// The sampling factor to use. Set to 1 for no sampling.
    const Float sampling_factor_;

    /// Whether we want logging.
    const bool silent_;

    /// The maximum size of the vocabulary.
    const size_t vocab_size_;
};

// ----------------------------------------------------------------------------
}  // namespace fastprop

#endif  // FASTPROP_HYPERPARAMETERS_HPP_
