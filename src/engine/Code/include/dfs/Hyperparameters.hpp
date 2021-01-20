#ifndef DFS_HYPERPARAMETERS_HPP_
#define DFS_HYPERPARAMETERS_HPP_

#include "jsonutils/jsonutils.hpp"

#include "dfs/Int.hpp"

namespace dfs
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

    /// The loss function (DFS is completely unsupervised, so we simply have
    /// this for consistency).
    const std::string loss_function_;

    /// The number of threads we want to use
    const Int num_threads_;

    /// Whether we want logging.
    const bool silent_;
};

// ----------------------------------------------------------------------------
}  // namespace dfs

#endif  // DFS_HYPERPARAMETERS_HPP_
