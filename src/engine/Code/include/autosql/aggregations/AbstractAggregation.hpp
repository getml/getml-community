#ifndef AUTOSQL_AGGREGATIONS_ABSTRACTAGGREGATION_HPP_
#define AUTOSQL_AGGREGATIONS_ABSTRACTAGGREGATION_HPP_

namespace autosql
{
namespace aggregations
{
// ----------------------------------------------------------------------------

class AbstractAggregation
{
   public:
    AbstractAggregation(){};

    virtual ~AbstractAggregation() = default;

    // --------------------------------------

    /// Activates all samples
    virtual void activate_all(
        const bool _init_opt,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end ) = 0;

    /// Activates all samples that contain _category
    /// Used for prediction
    virtual void activate_samples_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end ) = 0;

    /// Iterates through the categories and selectively
    /// activates samples
    /// Used for training
    virtual void activate_samples_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) = 0;

    /// Activates all samples that do not contain _category
    /// Used for prediction
    virtual void activate_samples_not_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end ) = 0;

    /// Iterates through the categories and selectively
    /// activates samples
    /// Used for training
    virtual void activate_samples_not_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) = 0;

    /// Iterates through the samples and activates those
    /// samples that are greater than the critical value
    virtual void activate_samples_from_above(
        const Float _critical_value,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end ) = 0;

    /// Iterates through the samples and activates them
    /// starting with the greatest
    virtual void activate_samples_from_above(
        const std::vector<Float> &_critical_values,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end ) = 0;

    /// Iterates through the samples and activates those
    /// samples that smaller than or equal to the critical value
    virtual void activate_samples_from_below(
        const Float _critical_value,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end ) = 0;

    /// Iterates through the samples and activates them
    /// starting with the smallest
    virtual void activate_samples_from_below(
        const std::vector<Float> &_critical_values,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end ) = 0;

    /// Gets rid of data that is no longer needed.
    virtual void clear() = 0;

    /// Some aggregations, such as min and max, contain additional
    /// containers. If we do not clear them, they will take up
    /// too much memory.
    virtual void clear_extras() = 0;

    /// Commits the current stage of the yhats contained in
    /// updates_stored
    virtual void commit() = 0;

    /// Deactivates all samples that contains _category
    /// Used for prediction
    virtual void deactivate_samples_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end ) = 0;

    /// Iteratres through the categories and selectively
    /// deactivates samples
    /// Used for training
    virtual void deactivate_samples_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) = 0;

    /// Iterates through the samples and deactivates those
    /// samples that are greater than the critical value
    virtual void deactivate_samples_from_above(
        const Float _critical_value,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end ) = 0;

    /// Iterates through the samples and deactivates them
    /// starting with the greatest
    virtual void deactivate_samples_from_above(
        const std::vector<Float> &_critical_values,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end ) = 0;

    /// Iterates through the samples and deactivates those
    /// samples that smaller than or equal to the critical value
    virtual void deactivate_samples_from_below(
        const Float _critical_value,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end ) = 0;

    /// Iterates through the samples and deactivates them
    /// starting with the smallest
    virtual void deactivate_samples_from_below(
        const std::vector<Float> &_critical_values,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end ) = 0;

    /// Dectivates all samples that do not contain _category
    /// Used for prediction
    virtual void deactivate_samples_not_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _sample_container_end ) = 0;

    /// Iterates through the categories and selectively
    /// deactivates samples
    /// Used for training
    virtual void deactivate_samples_not_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) = 0;

    /// Deactivates all samples where the numerical_value contains null values.
    /// Such samples must always be deactivated.
    virtual void deactivate_samples_with_null_values(
        containers::MatchPtrs::iterator _sample_container_begin,
        containers::MatchPtrs::iterator _null_values_separator ) = 0;

    /// Returns a string describing the type of the intermediate aggregation
    /// needed
    virtual std::string intermediate_type() const = 0;

    /// Returns an intermediate aggregation representing this aggregation
    virtual std::shared_ptr<optimizationcriteria::OptimizationCriterion>
    make_intermediate(
        std::shared_ptr<IntermediateAggregationImpl> _impl ) const = 0;

    /// Whether the aggregation requires the samples to be sorted
    /// by value_to_be_aggregated
    virtual bool needs_sorting() const = 0;

    /// Initializes yhat_, yhat_committed_ and yhat_stored_ and all variables
    /// related to the aggregations with 0.0.
    virtual void reset() = 0;

    /// Reinstates the status of yhat the last time commit()
    /// had been called
    virtual void revert_to_commit() = 0;

    /// Separates the samples for which the value to be aggregated is NULL
    virtual containers::Matches::iterator separate_null_values(
        containers::Matches &_samples ) = 0;

    /// Separates the pointers to samples for which the value to be aggregated
    /// is NULL
    virtual containers::MatchPtrs::iterator separate_null_values(
        containers::MatchPtrs &_samples ) = 0;

    /// Trivial setter
    virtual void set_aggregation_impl(
        containers::Optional<AggregationImpl> *_aggregation_impl ) = 0;

    /// Trivial setter
    virtual void set_optimization_criterion(
        optimizationcriteria::OptimizationCriterion
            *_optimization_criterion ) = 0;

    /// Sets the beginning and the end of the actual samples -
    /// some aggregations like MIN or MAX needs this information.
    virtual void set_samples_begin_end(
        containers::Match *_samples_begin, containers::Match *_samples_end ) = 0;

    /// Trivial setter
    virtual void set_value_to_be_aggregated(
        const containers::Column<Float> &_value_to_be_aggregated ) = 0;

    /// Trivial setter
    virtual void set_value_to_be_aggregated(
        const containers::ColumnView<
            Float,
            std::map<Int, Int>> &_value_to_be_aggregated ) = 0;

    /// Trivial setter
    virtual void set_value_to_be_aggregated(
        const containers::Column<Int> &_value_to_be_aggregated ) = 0;

    /// Trivial setter
    virtual void set_value_to_be_compared(
        const containers::Column<Float> &_value_to_be_compared ) = 0;

    /// Trivial setter
    virtual void set_value_to_be_compared(
        const containers::ColumnView<Float, std::vector<size_t>>
            &_value_to_be_compared ) = 0;

    /// Sorts the samples by value to be aggregated (within the element in
    /// population table)
    virtual void sort_samples(
        containers::Matches::iterator _samples_begin,
        containers::Matches::iterator _samples_end ) = 0;

    /// Returns a string describing the type of the aggregation
    virtual std::string type() const = 0;

    /// Updates the optimization criterion, makes it store its
    /// current stage and clears updates_current_
    virtual void update_optimization_criterion_and_clear_updates_current(
        const Float _num_samples_smaller,
        const Float _num_samples_greater ) = 0;

    /// Returns a reference to the predictions stored by the aggregation
    virtual std::vector<Float> &yhat() = 0;
};

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace autosql

#endif  // AUTOSQL_AGGREGATIONS_ABSTRACTAGGREGATION_HPP_
