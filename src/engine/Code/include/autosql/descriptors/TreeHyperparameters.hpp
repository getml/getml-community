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
        : allow_sets( _json_obj.AUTOSQL_GET_VALUE<bool>( "allow_sets_" ) ),
          grid_factor(
              _json_obj.AUTOSQL_GET_VALUE<AUTOSQL_FLOAT>( "grid_factor_" ) ),
          max_length( _json_obj.AUTOSQL_GET_VALUE<AUTOSQL_INT>( "max_length_" ) ),
          max_length_probe(
              TreeHyperparameters::calc_max_length_probe( _json_obj ) ),
          min_num_samples(
              _json_obj.AUTOSQL_GET_VALUE<AUTOSQL_INT>( "min_num_samples_" ) ),
          regularization(
              _json_obj.AUTOSQL_GET_VALUE<AUTOSQL_FLOAT>( "regularization_" ) ),
          share_conditions(
              _json_obj.AUTOSQL_GET_VALUE<AUTOSQL_FLOAT>( "share_conditions_" ) )
    {
    }

    // ------------------------------------------------------

    /// Calculates max_length_probe
    static AUTOSQL_INT calc_max_length_probe(
        const Poco::JSON::Object& _json_obj )
    {
        if ( _json_obj.AUTOSQL_GET_VALUE<bool>( "fast_training_" ) &&
             !_json_obj.AUTOSQL_GET_VALUE<bool>( "round_robin_" ) )
            {
                return 0;
            }
        else
            {
                return _json_obj.AUTOSQL_GET_VALUE<AUTOSQL_INT>( "max_length_" );
            }
    }

    // ------------------------------------------------------

    /// Whether we want to allow the algorithm to summarize categorical features
    /// in sets.
    const bool allow_sets;

    /// Proportional to the frequency of critical values
    const AUTOSQL_FLOAT grid_factor;

    /// The maximum depth of a decision tree
    const AUTOSQL_INT max_length;

    /// The maximum depth during the "probing" phase
    const AUTOSQL_INT max_length_probe;

    /// The minimum number of samples needed for a split
    const AUTOSQL_INT min_num_samples;

    /// Minimum improvement in R2 necessary for a split
    const AUTOSQL_FLOAT regularization;

    /// The share of conditions randomly selected
    const AUTOSQL_FLOAT share_conditions;
};

// ----------------------------------------------------------------------------
}  // namespace descriptors
}  // namespace autosql
#endif  // AUTOSQL_DESCRIPTORS_TREEHYPERPARAMETERS_HPP_
