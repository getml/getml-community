#ifndef MULTIREL_ENSEMBLE_CANDIDATETREEBUILDER_HPP_
#define MULTIREL_ENSEMBLE_CANDIDATETREEBUILDER_HPP_

namespace multirel
{
namespace ensemble
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
    static std::list<decisiontrees::DecisionTree> build_candidates(
        const decisiontrees::TableHolder &_table_holder,
        const std::vector<descriptors::SameUnits> &_same_units,
        const size_t _ix_feature,
        const descriptors::Hyperparameters &_hyperparameters,
        containers::Optional<aggregations::AggregationImpl> *_aggregation_impl,
        std::mt19937 *_random_number_generator,
        multithreading::Communicator *_comm );

    // -------------------------------------------------------------------------

   private:
    /// Adds trees with COUNT aggregations
    static void add_counts(
        const decisiontrees::TableHolder &_table_holder,
        const std::vector<descriptors::SameUnits> &_same_units,
        const descriptors::Hyperparameters &_hyperparameters,
        const Int _ix_perip_used,
        std::mt19937 *_random_number_generator,
        containers::Optional<aggregations::AggregationImpl> *_aggregation_impl,
        multithreading::Communicator *_comm,
        std::list<decisiontrees::DecisionTree> *_candidate_trees );

    /// Adds trees with COUNT DISTIINCT and COUNT MINUS COUNT DISTINCT
    /// aggregations
    static void add_count_distincts(
        const decisiontrees::TableHolder &_table_holder,
        const std::vector<descriptors::SameUnits> &_same_units,
        const descriptors::Hyperparameters &_hyperparameters,
        const Int _ix_perip_used,
        std::mt19937 *_random_number_generator,
        containers::Optional<aggregations::AggregationImpl> *_aggregation_impl,
        multithreading::Communicator *_comm,
        std::list<decisiontrees::DecisionTree> *_candidate_trees );

    /// Adds trees with all other aggregations
    static void add_other_aggs(
        const decisiontrees::TableHolder &_table_holder,
        const std::vector<descriptors::SameUnits> &_same_units,
        const descriptors::Hyperparameters &_hyperparameters,
        const Int _ix_perip_used,
        std::mt19937 *_random_number_generator,
        containers::Optional<aggregations::AggregationImpl> *_aggregation_impl,
        multithreading::Communicator *_comm,
        std::list<decisiontrees::DecisionTree> *_candidate_trees );

    /// Adds aggregations over the subfeatures
    static void add_subfeature_aggs(
        const decisiontrees::TableHolder &_table_holder,
        const std::vector<descriptors::SameUnits> &_same_units,
        const descriptors::Hyperparameters &_hyperparameters,
        const Int _ix_perip_used,
        std::mt19937 *_random_number_generator,
        containers::Optional<aggregations::AggregationImpl> *_aggregation_impl,
        multithreading::Communicator *_comm,
        std::list<decisiontrees::DecisionTree> *_candidate_trees );

    /// Builds all candidate trees based on the available
    /// data and aggregations
    static std::list<decisiontrees::DecisionTree> build_candidate_trees(
        const decisiontrees::TableHolder &_table_holder,
        const std::vector<descriptors::SameUnits> &_same_units,
        const descriptors::Hyperparameters _hyperparameters,
        std::mt19937 *_random_number_generator,
        containers::Optional<aggregations::AggregationImpl> *_aggregation_impl,
        multithreading::Communicator *_comm );

    /// Determines whether a particular column may be used to comparison only.
    static bool is_comparison_only(
        const decisiontrees::TableHolder &_table_holder,
        const enums::DataUsed _data_used,
        const size_t _ix_perip_used,
        const size_t _ix_column_used );

    /// When the user provides a _share_aggregations that is not 1.0,
    /// the decisiontrees::DecisionTreeEnsemble actually initializes all trees,
    /// and then removes all but _share_aggregations of trees. This is performed
    /// by this member function.
    static void randomly_remove_candidate_trees(
        const Float _share_aggregations,
        std::mt19937 *_random_number_generator,
        multithreading::Communicator *_comm,
        std::list<decisiontrees::DecisionTree> *_candidate_trees );

    /// For the round_robin approach, we remove all features but
    /// one - the remaining one is a different one every time.
    static void round_robin(
        const size_t _ix_feature,
        std::list<decisiontrees::DecisionTree> *_candidate_trees );

    /// Returns the number of columns
    static size_t get_ncols(
        const std::vector<containers::DataFrame> &_peripheral_tables,
        const std::vector<descriptors::SameUnits> &_same_units,
        const size_t _ix_perip_used,
        const enums::DataUsed _data_used );

    /// Returns true if _agg is FIRST or LAST, but there are no time stamps in
    /// _peripheral.
    static bool skip_first_last(
        const std::string &_agg, const containers::DataFrame &_peripheral );

    /// Transforms a same_unit enum to a same_unit_..._ts, if applicable.
    /// Otherwise, it just returns the original value.
    static enums::DataUsed to_ts(
        const decisiontrees::TableHolder &_table_holder,
        const std::vector<descriptors::SameUnits> &_same_units,
        const enums::DataUsed _data_used,
        const size_t _ix_perip_used,
        const size_t _ix_column_used );

    // -------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace multirel
#endif  // MULTIREL_ENSEMBLE_CANDIDATETREEBUILDER_HPP_
