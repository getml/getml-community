#ifndef AUTOSQL_DECISIONTREES_CANDIDATETREEBUILDER_HPP_
#define AUTOSQL_DECISIONTREES_CANDIDATETREEBUILDER_HPP_

namespace autosql
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

class CandidateTreeBuilder
{
    // -------------------------------------------------------------------------

   public:
    /// This works as follows:
    /// 1) build_candidate_trees(...) builds a list of all possible
    /// combinations.
    /// 2) The trees can then be randomly removed, left untouched or a
    /// round robin approach is applied.
    static std::list<DecisionTree> build_candidates(
        const TableHolder &_table_holder,
        const std::vector<descriptors::SameUnits> &_same_units,
        const SQLNET_INT _ix_feature,
        const descriptors::Hyperparameters _hyperparameters,
        containers::Optional<aggregations::AggregationImpl> &_aggregation_impl,
        std::mt19937 &_random_number_generator,
        SQLNET_COMMUNICATOR *_comm = nullptr );

    // -------------------------------------------------------------------------

   private:
    /// Adds trees with COUNT aggregations
    static void add_counts(
        const TableHolder &_table_holder,
        const std::vector<descriptors::SameUnits> &_same_units,
        const descriptors::Hyperparameters &_hyperparameters,
        const SQLNET_INT _ix_perip_used,
        std::mt19937 &_random_number_generator,
        containers::Optional<aggregations::AggregationImpl> &_aggregation_impl,
        SQLNET_COMMUNICATOR *_comm,
        std::list<DecisionTree> &_candidate_trees );

    /// Adds trees with COUNT DISTIINCT and COUNT MINUS COUNT DISTINCT
    /// aggregations
    static void add_count_distincts(
        const TableHolder &_table_holder,
        const std::vector<descriptors::SameUnits> &_same_units,
        const descriptors::Hyperparameters &_hyperparameters,
        const SQLNET_INT _ix_perip_used,
        std::mt19937 &_random_number_generator,
        containers::Optional<aggregations::AggregationImpl> &_aggregation_impl,
        SQLNET_COMMUNICATOR *_comm,
        std::list<DecisionTree> &_candidate_trees );

    /// Adds trees with all other aggregations
    static void add_other_aggs(
        const TableHolder &_table_holder,
        const std::vector<descriptors::SameUnits> &_same_units,
        const descriptors::Hyperparameters &_hyperparameters,
        const SQLNET_INT _ix_perip_used,
        std::mt19937 &_random_number_generator,
        containers::Optional<aggregations::AggregationImpl> &_aggregation_impl,
        SQLNET_COMMUNICATOR *_comm,
        std::list<DecisionTree> &_candidate_trees );

    /// Adds aggregations over the subfeatures
    static void add_subfeature_aggs(
        const TableHolder &_table_holder,
        const std::vector<descriptors::SameUnits> &_same_units,
        const descriptors::Hyperparameters &_hyperparameters,
        const SQLNET_INT _ix_perip_used,
        std::mt19937 &_random_number_generator,
        containers::Optional<aggregations::AggregationImpl> &_aggregation_impl,
        SQLNET_COMMUNICATOR *_comm,
        std::list<DecisionTree> &_candidate_trees );

    /// Builds all candidate trees based on the available
    /// data and aggregations
    static std::list<DecisionTree> build_candidate_trees(
        const TableHolder &_table_holder,
        const std::vector<descriptors::SameUnits> &_same_units,
        const descriptors::Hyperparameters _hyperparameters,
        std::mt19937 &_random_number_generator,
        containers::Optional<aggregations::AggregationImpl> &_aggregation_impl,
        SQLNET_COMMUNICATOR *_comm = nullptr );

    /// Determines whether a particular column may be used to comparison only.
    static bool is_comparison_only(
        const TableHolder &_table_holder,
        const DataUsed _data_used,
        const SQLNET_INT _ix_perip_used,
        const SQLNET_INT _ix_column_used );

    /// When the user provides a _share_aggregations that is not 1.0,
    /// the DecisionTreeEnsemble actually initializes all trees,
    /// and then removes all but _share_aggregations of trees. This is performed
    /// by this member function.
    static void randomly_remove_candidate_trees(
        const SQLNET_FLOAT _share_aggregations,
        std::mt19937 &_random_number_generator,
        std::list<DecisionTree> &_candidate_trees,
        SQLNET_COMMUNICATOR *_comm = nullptr );

    /// For the round_robin approach, we remove all features but
    /// one - the remaining one is a different one every time.
    static void round_robin(
        const SQLNET_INT _ix_feature,
        std::list<DecisionTree> &_candidate_trees );

    /// Returns the number of columns
    static SQLNET_INT get_ncols(
        const std::vector<containers::DataFrame> &_peripheral_tables,
        const std::vector<descriptors::SameUnits> &_same_units,
        const SQLNET_INT _ix_perip_used,
        const DataUsed _data_used );

    // -------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}
}
#endif  // AUTOSQL_DECISIONTREES_CANDIDATETREEBUILDER_HPP_
