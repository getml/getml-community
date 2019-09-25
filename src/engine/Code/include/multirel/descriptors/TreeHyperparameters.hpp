#ifndef MULTIREL_DESCRIPTORS_TREEHYPERPARAMETERS_HPP_
#define MULTIREL_DESCRIPTORS_TREEHYPERPARAMETERS_HPP_

namespace multirel
{
namespace descriptors
{
// ----------------------------------------------------------------------------
// Hyperparameters that need to be passed to the tree (there are surprisingly
// few)

struct TreeHyperparameters
{
    TreeHyperparameters( const Poco::JSON::Object& _json_obj )
        : allow_sets_( JSON::get_value<bool>( _json_obj, "allow_sets_" ) ),
          delta_t_( JSON::get_value<Float>( _json_obj, "delta_t_" ) ),
          grid_factor_( JSON::get_value<Float>( _json_obj, "grid_factor_" ) ),
          max_length_( JSON::get_value<Int>( _json_obj, "max_length_" ) ),
          min_num_samples_(
              JSON::get_value<Int>( _json_obj, "min_num_samples_" ) ),
          regularization_(
              JSON::get_value<Float>( _json_obj, "regularization_" ) ),
          share_conditions_(
              JSON::get_value<Float>( _json_obj, "share_conditions_" ) )
    {
    }

    // ------------------------------------------------------

    /// Whether we want to allow the algorithm to summarize categorical features
    /// in sets.
    const bool allow_sets_;

    /// Size of the moving time windows.
    const Float delta_t_;

    /// Proportional to the frequency of critical values
    const Float grid_factor_;

    /// The maximum depth of a decision tree
    const size_t max_length_;

    /// The minimum number of samples needed for a split
    const size_t min_num_samples_;

    /// Minimum improvement in R2 necessary for a split
    const Float regularization_;

    /// The share of conditions randomly selected
    const Float share_conditions_;
};

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace multirel

#endif  // MULTIREL_DESCRIPTORS_TREEHYPERPARAMETERS_HPP_
