#ifndef MULTIREL_AGGREGATIONS_ABSTRACTFITAGGREGATION_HPP_
#define MULTIREL_AGGREGATIONS_ABSTRACTFITAGGREGATION_HPP_

namespace multirel
{
namespace aggregations
{
// ----------------------------------------------------------------------------

class AbstractFitAggregation
{
   public:
    AbstractFitAggregation(){};

    virtual ~AbstractFitAggregation() = default;

    // --------------------------------------

    /// Activates all samples
    virtual void activate_all(
        const bool _init_opt,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) = 0;

    /// Iterates through the categories and selectively
    /// activates samples
    virtual void activate_matches_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) = 0;

    /// Iterates through the individual words and selectively
    /// activates samples.
    virtual void activate_matches_containing_words(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::WordIndex &_index ) = 0;

    /// Implements a lag functionality through moving time windows.
    virtual void activate_matches_in_window(
        const std::vector<size_t> &_indptr,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) = 0;

    /// Implements a lag functionality through moving time windows.
    virtual void activate_matches_outside_window(
        const std::vector<size_t> &_indptr,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) = 0;

    /// Iterates through the categories and selectively
    /// activates samples
    virtual void activate_matches_not_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) = 0;

    /// Iterates through the words and selectively
    /// activates matches.
    /// Used for individual words only.
    virtual void activate_matches_not_containing_words(
        const std::vector<Int>::const_iterator _words_begin,
        const std::vector<Int>::const_iterator _words_end,
        const Revert _revert,
        const containers::WordIndex &_index ) = 0;

    /// Iterates through the samples and activates them
    /// starting with the greatest
    virtual void activate_matches_from_above(
        const std::vector<size_t> &_indptr,
        const containers::MatchPtrs::const_iterator &_matches_begin,
        const containers::MatchPtrs::const_iterator &_matches_end ) = 0;

    /// Iterates through the samples and activates them
    /// starting with the smallest
    virtual void activate_matches_from_below(
        const std::vector<size_t> &_indptr,
        const containers::MatchPtrs::const_iterator &_matches_begin,
        const containers::MatchPtrs::const_iterator &_matches_end ) = 0;

    /// Activates all matches between _separator and _match_container_end.
    virtual void activate_partition_from_above(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _separator,
        containers::MatchPtrs::iterator _match_container_end ) = 0;

    /// Activates all matches between _match_container_begin and _separator.
    virtual void activate_partition_from_below(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _separator,
        containers::MatchPtrs::iterator _match_container_end ) = 0;

    /// Gets rid of data that is no longer needed.
    virtual void clear() = 0;

    /// Some aggregations, such as min and max, contain additional
    /// containers. If we do not clear them, they will take up
    /// too much memory.
    virtual void clear_extras() = 0;

    /// Commits the current stage of the yhats contained in
    /// updates_stored
    virtual void commit() = 0;

    /// Iteratres through the categories and selectively
    /// deactivates samples
    virtual void deactivate_matches_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) = 0;

    /// Iterates through the words and selectively
    /// activates matches.
    /// Used for individual words only.
    virtual void deactivate_matches_containing_words(
        const std::vector<Int>::const_iterator _words_begin,
        const std::vector<Int>::const_iterator _words_end,
        const Revert _revert,
        const containers::WordIndex &_index ) = 0;

    /// Iterates through the samples and deactivates them
    /// starting with the greatest
    virtual void deactivate_matches_from_above(
        const std::vector<size_t> &_indptr,
        const containers::MatchPtrs::const_iterator &_matches_begin,
        const containers::MatchPtrs::const_iterator &_matches_end ) = 0;

    /// Iterates through the samples and deactivates them
    /// starting with the smallest
    virtual void deactivate_matches_from_below(
        const std::vector<size_t> &_indptr,
        const containers::MatchPtrs::const_iterator &_matches_begin,
        const containers::MatchPtrs::const_iterator &_matches_end ) = 0;

    /// Implements a lag functionality through moving time windows.
    virtual void deactivate_matches_in_window(
        const std::vector<size_t> &_indptr,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) = 0;

    /// Implements a lag functionality through moving time windows.
    virtual void deactivate_matches_outside_window(
        const std::vector<size_t> &_indptr,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) = 0;

    /// Iterates through the categories and selectively
    /// deactivates samples
    virtual void deactivate_matches_not_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) = 0;

    /// Iterates through the words and selectively
    /// deactivates matches.
    /// Used for individual words only.
    virtual void deactivate_matches_not_containing_words(
        const std::vector<Int>::const_iterator _words_begin,
        const std::vector<Int>::const_iterator _words_end,
        const Revert _revert,
        const containers::WordIndex &_index ) = 0;

    /// Deactivates all samples where the numerical_value contains null values.
    /// Such samples must always be deactivated.
    virtual void deactivate_matches_with_null_values(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _null_values_separator ) = 0;

    /// Activates all matches between _separator and _match_container_end.
    virtual void deactivate_partition_from_above(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _separator,
        containers::MatchPtrs::iterator _match_container_end ) = 0;

    /// Activates all matches between _match_container_begin and _separator.
    virtual void deactivate_partition_from_below(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _separator,
        containers::MatchPtrs::iterator _match_container_end ) = 0;

    /// Initializes yhat_, yhat_committed_ and yhat_stored_ and all variables
    /// related to the aggregations with 0.0.
    virtual void reset() = 0;

    /// Reinstates the status of yhat the last time commit()
    /// had been called
    virtual void revert_to_commit() = 0;

    /// Separates the pointers to samples for which the value to be aggregated
    /// is NULL
    virtual containers::MatchPtrs::iterator separate_null_values(
        containers::MatchPtrs *_match_ptrs ) const = 0;

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
}  // namespace multirel

#endif  // MULTIREL_AGGREGATIONS_ABSTRACTFITAGGREGATION_HPP_
