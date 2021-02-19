#ifndef FASTPROP_HYPERPARAMETERS_HPP_
#define FASTPROP_HYPERPARAMETERS_HPP_

#include "jsonutils/jsonutils.hpp"

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

    /// The loss function (FastProp is completely unsupervised, so we simply
    /// have this for consistency).
    const std::string loss_function_;

    /// The number of categories from which we would like to extract numerical
    /// features.
    const size_t n_most_frequent_;

    /// The maximum number of features generated.
    const size_t num_features_;

    /// The number of threads we want to use
    const Int num_threads_;

    /// Whether we want logging.
    const bool silent_;
};

// ----------------------------------------------------------------------------
}  // namespace fastprop

#endif  // FASTPROP_HYPERPARAMETERS_HPP_
