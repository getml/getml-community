#ifndef AUTOSQL_DESCRIPTORS_TREEHYPERPARAMETERS_HPP_
#define AUTOSQL_DESCRIPTORS_TREEHYPERPARAMETERS_HPP_

namespace autosql
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
          grid_factor_( JSON::get_value<Float>( _json_obj, "grid_factor_" ) ),
          lag_( JSON::get_value<Float>( _json_obj, "lag_" ) ),
          max_length_( JSON::get_value<Int>( _json_obj, "max_length_" ) ),
          max_length_probe_(
              TreeHyperparameters::calc_max_length_probe( _json_obj ) ),
          min_num_samples_(
              JSON::get_value<Int>( _json_obj, "min_num_samples_" ) ),
          regularization_(
              JSON::get_value<Float>( _json_obj, "regularization_" ) ),
          share_conditions_(
              JSON::get_value<Float>( _json_obj, "share_conditions_" ) )
    {
    }

    // ------------------------------------------------------

    /// Calculates max_length_probe
    static size_t calc_max_length_probe( const Poco::JSON::Object& _json_obj )
    {
        if ( JSON::get_value<bool>( _json_obj, "fast_training_" ) &&
             !JSON::get_value<bool>( _json_obj, "round_robin_" ) )
            {
                return 0;
            }
        else
            {
                return JSON::get_value<size_t>( _json_obj, "max_length_" );
            }
    }

    // ------------------------------------------------------

    /// Whether we want to allow the algorithm to summarize categorical features
    /// in sets.
    const bool allow_sets_;

    /// Proportional to the frequency of critical values
    const Float grid_factor_;

    /// Lag used for the moving time windows.
    const Float lag_;

    /// The maximum depth of a decision tree
    const size_t max_length_;

    /// The maximum depth during the "probing" phase
    const size_t max_length_probe_;

    /// The minimum number of samples needed for a split
    const size_t min_num_samples_;

    /// Minimum improvement in R2 necessary for a split
    const Float regularization_;

    /// The share of conditions randomly selected
    const Float share_conditions_;
};

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace autosql

#endif  // AUTOSQL_DESCRIPTORS_TREEHYPERPARAMETERS_HPP_
