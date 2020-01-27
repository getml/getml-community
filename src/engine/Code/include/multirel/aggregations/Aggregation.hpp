#ifndef MULTIREL_AGGREGATIONS_AGGREGATION_HPP_
#define MULTIREL_AGGREGATIONS_AGGREGATION_HPP_

namespace multirel
{
namespace aggregations
{
// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
class Aggregation : public AbstractAggregation
{
   public:
    Aggregation() : AbstractAggregation(), aggregation_impl_( nullptr ){};

    ~Aggregation() = default;

    // --------------------------------------

    /// Activates all samples
    void activate_all(
        const bool _init_opt,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Activates all samples that contain any category between
    /// _categories_begin and _categories_end. Used for prediction.
    void activate_samples_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Iterates through the categories and selectively
    /// activates samples.
    /// Used for training.
    void activate_samples_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) final;

    /// Iterates through the samples and activates those.
    /// samples that are greater than the critical value.
    void activate_samples_from_above(
        const Float _critical_value,
        containers::MatchPtrs::const_iterator _match_container_begin,
        containers::MatchPtrs::const_iterator _match_container_end ) final;

    /// Iterates through the samples and activates them
    /// starting with the greatest.
    void activate_samples_from_above(
        const std::vector<size_t> &_indptr,
        const containers::MatchPtrs::const_iterator &_matches_begin,
        const containers::MatchPtrs::const_iterator &_matches_end ) final;

    /// Iterates through the samples and activates those
    /// samples that smaller than or equal to the critical value.
    void activate_samples_from_below(
        const Float _critical_value,
        containers::MatchPtrs::const_iterator _match_container_begin,
        containers::MatchPtrs::const_iterator _match_container_end ) final;

    /// Iterates through the samples and activates them
    /// starting with the smallest.
    void activate_samples_from_below(
        const std::vector<size_t> &_indptr,
        const containers::MatchPtrs::const_iterator &_matches_begin,
        const containers::MatchPtrs::const_iterator &_matches_end ) final;

    /// Implements a lag functionality through moving time windows - used by
    /// transform.
    void activate_samples_in_window(
        const Float _critical_value,
        const Float _delta_t,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Implements a lag functionality through moving time windows - used by
    /// fit.
    void activate_samples_in_window(
        const std::vector<size_t> &_indptr,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Implements a lag functionality through moving time windows - used by
    /// transform.
    void activate_samples_outside_window(
        const Float _critical_value,
        const Float _delta_t,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Implements a lag functionality through moving time windows - used by
    /// fit.
    void activate_samples_outside_window(
        const std::vector<size_t> &_indptr,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Activates all samples that do not contain any category between
    /// _categories_begin and _categories_end. Used for prediction.
    void activate_samples_not_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Iterates through the categories and selectively
    /// activates samples.
    /// Used for training.
    void activate_samples_not_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) final;

    /// Gets rid of data that is no longer needed.
    void clear() final;

    /// Commits the current stage of the yhats contained in
    /// updates_stored.
    void commit() final;

    /// Deactivates all samples that contain any category between
    /// _categories_begin and _categories_end. Used for prediction.
    void deactivate_samples_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Iteratres through the categories and selectively
    /// deactivates samples.
    /// Used for training.
    void deactivate_samples_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) final;

    /// Iterates through the samples and deactivates those
    /// samples that are greater than the critical value.
    void deactivate_samples_from_above(
        const Float _critical_value,
        containers::MatchPtrs::const_iterator _match_container_begin,
        containers::MatchPtrs::const_iterator _match_container_end ) final;

    /// Iterates through the samples and deactivates them
    /// starting with the greatest.
    void deactivate_samples_from_above(
        const std::vector<size_t> &_indptr,
        const containers::MatchPtrs::const_iterator &_matches_begin,
        const containers::MatchPtrs::const_iterator &_matches_end ) final;

    /// Iterates through the samples and deactivates those
    /// samples that smaller than or equal to the critical value.
    void deactivate_samples_from_below(
        const Float _critical_value,
        containers::MatchPtrs::const_iterator _match_container_begin,
        containers::MatchPtrs::const_iterator _match_container_end ) final;

    /// Iterates through the samples and deactivates them
    /// starting with the smallest.
    void deactivate_samples_from_below(
        const std::vector<size_t> &_indptr,
        const containers::MatchPtrs::const_iterator &_matches_begin,
        const containers::MatchPtrs::const_iterator &_matches_end ) final;

    /// Implements a lag functionality through moving time windows - used by
    /// transform.
    void deactivate_samples_in_window(
        const Float _critical_value,
        const Float _delta_t,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Implements a lag functionality through moving time windows - used by
    /// fit.
    void deactivate_samples_in_window(
        const std::vector<size_t> &_indptr,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Implements a lag functionality through moving time windows - used by
    /// transform.
    void deactivate_samples_outside_window(
        const Float _critical_value,
        const Float _delta_t,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Implements a lag functionality through moving time windows - used by
    /// fit.
    void deactivate_samples_outside_window(
        const std::vector<size_t> &_indptr,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Deactivates all samples that do not contain any category between
    /// _categories_begin and _categories_end. Used for prediction.
    void deactivate_samples_not_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Iterates through the categories and selectively
    /// deactivates samples.
    /// Used for training.
    void deactivate_samples_not_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) final;

    /// Deactivates all samples where the numerical_value contains null values.
    /// Such samples must always be deactivated.
    void deactivate_samples_with_null_values(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _null_values_separator ) final;

    /// Initializes optimization criterion after all samples have been
    /// activated.
    void init_optimization_criterion(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end );

    /// Returns a string describing the type of the intermediate aggregation
    /// needed
    std::string intermediate_type() const final;

    /// Returns an intermediate aggregation representing this aggregation
    std::shared_ptr<optimizationcriteria::OptimizationCriterion>
    make_intermediate(
        std::shared_ptr<IntermediateAggregationImpl> _impl ) const final;

    /// Resets yhat_, yhat_committed_ and yhat_stored_ and all variables
    /// related to the aggregations with 0.0.
    void reset() final;

    /// Reinstates the status of yhat the last time commit()
    /// had been called.
    void revert_to_commit() final;

    /// Separates the samples for which the value to be aggregated is NULL
    containers::Matches::iterator separate_null_values(
        containers::Matches *_matches );

    /// Separates the pointers to samples for which the value to be aggregated
    /// is NULL
    containers::MatchPtrs::iterator separate_null_values(
        containers::MatchPtrs *_match_ptrs );

    /// Sorts the samples by value to be aggregated (within the element in
    /// population table)
    void sort_matches(
        containers::Matches::iterator _matches_begin,
        containers::Matches::iterator _matches_end );

    /// Updates the optimization criterion, makes it store its
    /// current stage and clears updates_current()
    void update_optimization_criterion_and_clear_updates_current(
        const Float _num_samples_smaller,
        const Float _num_samples_greater ) final;

    // --------------------------------------
    // Here we break with our usual convention
    // of alphabetical ordering, to increase readability
    // and maintainability.

    // --------------------------------------
    // AVG aggregation

    /// AVG aggregation:
    /// Activate sample - activating a sample means that
    /// it is now included in the aggregation, but has not
    /// been included in the aggregation before.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Avg>::value,
            int>::type = 0>
    inline void activate_sample( containers::Match *_sample )
    {
        assert_true( _sample->ix_x_popul >= 0 );
        assert_true(
            static_cast<size_t>( _sample->ix_x_popul ) < yhat_inline().size() );

        assert_true( _sample->ix_x_popul < static_cast<Int>( sum().size() ) );
        assert_true( _sample->ix_x_popul < static_cast<Int>( count().size() ) );

        assert_true(
            value_to_be_aggregated( _sample ) ==
            value_to_be_aggregated( _sample ) );

        static_assert( needs_sum_, "sum needed" );
        static_assert( needs_count_, "count needed" );

        sum()[_sample->ix_x_popul] += value_to_be_aggregated( _sample );

        count()[_sample->ix_x_popul] += 1.0;

        assert_true( count()[_sample->ix_x_popul] > 0.0 );

        yhat_inline()[_sample->ix_x_popul] =
            sum()[_sample->ix_x_popul] / count()[_sample->ix_x_popul];
    }

    /// AVG aggregation:
    /// Deactivate sample - deactivating a sample means that
    /// it has been activated before, but it is now no longer
    /// included in the aggregation.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Avg>::value,
            int>::type = 0>
    inline void deactivate_sample( containers::Match *_sample )
    {
        assert_true( _sample->ix_x_popul >= 0 );
        assert_true(
            static_cast<size_t>( _sample->ix_x_popul ) < yhat_inline().size() );

        assert_true( _sample->ix_x_popul < static_cast<Int>( sum().size() ) );
        assert_true( _sample->ix_x_popul < static_cast<Int>( count().size() ) );

        assert_true( count()[_sample->ix_x_popul] > 0.0 );

        sum()[_sample->ix_x_popul] -= value_to_be_aggregated( _sample );

        count()[_sample->ix_x_popul] -= 1.0;

        yhat_inline()[_sample->ix_x_popul] =
            ( ( count()[_sample->ix_x_popul] > 0.5 )
                  ? ( sum()[_sample->ix_x_popul] /
                      count()[_sample->ix_x_popul] )
                  : ( 0.0 ) );
    }

    // --------------------------------------
    // COUNT aggregation

    /// COUNT aggregation:
    /// Activate sample - activating a sample means that
    /// it is now included in the aggregation, but has not
    /// been included in the aggregation before.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Count>::value,
            int>::type = 0>
    inline void activate_sample( containers::Match *_sample )
    {
        yhat_inline()[_sample->ix_x_popul] += 1.0;

        assert_true( yhat_inline()[_sample->ix_x_popul] > 0.0 );
    }

    /// COUNT aggregation:
    /// Deactivate sample - deactivating a sample means that
    /// it has been activated before, but it is now no longer
    /// included in the aggregation.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Count>::value,
            int>::type = 0>
    inline void deactivate_sample( containers::Match *_sample )
    {
        assert_true( yhat_inline()[_sample->ix_x_popul] > 0.0 );

        yhat_inline()[_sample->ix_x_popul] -= 1.0;
    }

    // --------------------------------------
    // COUNT DISTINCT aggregation

    /// COUNT DISTINCT aggregation:
    /// Activate sample - activating a sample means that
    /// it is now included in the aggregation, but has not
    /// been included in the aggregation before.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::CountDistinct>::value,
            int>::type = 0>
    inline void activate_sample( containers::Match *_sample )
    {
        assert_true( _sample->activated == false );

        assert_true( yhat_inline()[_sample->ix_x_popul] > -0.5 );

        static_assert( needs_altered_samples_, "altered samples needed" );

        _sample->activated = true;

        altered_samples().push_back( _sample );

        // We need to figure out if there is another sample
        // that has the same value as _sample. If there is
        // in fact another one, we should not increase the count.

        // Note that the samples are already ordered.

        const Float val = value_to_be_aggregated( _sample );

        for ( auto it = _sample - 1; it >= samples_begin_; --it )
            {
                if ( value_to_be_aggregated( it ) != val ||
                     it->ix_x_popul != _sample->ix_x_popul )
                    {
                        break;
                    }

                if ( it->activated )
                    {
                        return;
                    }
            }

        for ( auto it = _sample + 1; it < samples_end_; ++it )
            {
                if ( value_to_be_aggregated( it ) != val ||
                     it->ix_x_popul != _sample->ix_x_popul )
                    {
                        break;
                    }

                if ( it->activated )
                    {
                        return;
                    }
            }

        // No matches have been found - we can increase the count!
        yhat_inline()[_sample->ix_x_popul] += 1.0;
    }

    /// COUNT DISTINCT aggregation:
    /// Deactivate sample - deactivating a sample means that
    /// it has been activated before, but it is now no longer
    /// included in the aggregation.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::CountDistinct>::value,
            int>::type = 0>
    inline void deactivate_sample( containers::Match *_sample )
    {
        assert_true( _sample->activated );

        assert_true( yhat_inline()[_sample->ix_x_popul] > 0.5 );

        _sample->activated = false;

        altered_samples().push_back( _sample );

        // We need to figure out if there is another sample
        // that has the same value as _sample. If there is
        // in fact another one, we should not decrease the count.

        // Note that the samples are already ordered.

        const Float val = value_to_be_aggregated( _sample );

        for ( auto it = _sample - 1; it >= samples_begin_; --it )
            {
                if ( value_to_be_aggregated( it ) != val ||
                     it->ix_x_popul != _sample->ix_x_popul )
                    {
                        break;
                    }

                if ( it->activated )
                    {
                        return;
                    }
            }

        for ( auto it = _sample + 1; it < samples_end_; ++it )
            {
                if ( value_to_be_aggregated( it ) != val ||
                     it->ix_x_popul != _sample->ix_x_popul )
                    {
                        break;
                    }

                if ( it->activated )
                    {
                        return;
                    }
            }

        // No matches have been found - we can decrease the count!
        yhat_inline()[_sample->ix_x_popul] -= 1.0;
    }

    // --------------------------------------
    // COUNT MINUS COUNT DISTINCT aggregation

    /// COUNT MINUS COUNT DISTINCT aggregation:
    /// Activate sample - activating a sample means that
    /// it is now included in the aggregation, but has not
    /// been included in the aggregation before.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::CountMinusCountDistinct>::value,
            int>::type = 0>
    inline void activate_sample( containers::Match *_sample )
    {
        assert_true( _sample->activated == false );

        assert_true( yhat_inline()[_sample->ix_x_popul] > -0.5 );

        static_assert( needs_altered_samples_, "altered samples needed" );

        _sample->activated = true;

        altered_samples().push_back( _sample );

        // We need to figure out if there is another sample
        // that has the same value as _sample. If there is
        // in fact another one, we should not increase the count.

        // Note that the samples are already ordered.

        const Float val = value_to_be_aggregated( _sample );

        for ( auto it = _sample - 1; it >= samples_begin_; --it )
            {
                if ( value_to_be_aggregated( it ) != val ||
                     it->ix_x_popul != _sample->ix_x_popul )
                    {
                        break;
                    }

                if ( it->activated )
                    {
                        yhat_inline()[_sample->ix_x_popul] += 1.0;
                        return;
                    }
            }

        for ( auto it = _sample + 1; it < samples_end_; ++it )
            {
                if ( value_to_be_aggregated( it ) != val ||
                     it->ix_x_popul != _sample->ix_x_popul )
                    {
                        break;
                    }

                if ( it->activated )
                    {
                        yhat_inline()[_sample->ix_x_popul] += 1.0;
                        return;
                    }
            }
    }

    /// COUNT MINUS COUNT DISTINCT aggregation:
    /// Deactivate sample - deactivating a sample means that
    /// it has been activated before, but it is now no longer
    /// included in the aggregation.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::CountMinusCountDistinct>::value,
            int>::type = 0>
    inline void deactivate_sample( containers::Match *_sample )
    {
        assert_true( _sample->activated );

        assert_true( yhat_inline()[_sample->ix_x_popul] > -0.5 );

        _sample->activated = false;

        altered_samples_.push_back( _sample );

        // We need to figure out if there is another sample
        // that has the same value as _sample. If there is
        // in fact another one, we should not decrease the count.

        // Note that the samples are already ordered.

        const Float val = value_to_be_aggregated( _sample );

        for ( auto it = _sample - 1; it >= samples_begin_; --it )
            {
                if ( value_to_be_aggregated( it ) != val ||
                     it->ix_x_popul != _sample->ix_x_popul )
                    {
                        break;
                    }

                if ( it->activated )
                    {
                        yhat_inline()[_sample->ix_x_popul] -= 1.0;
                        return;
                    }
            }

        for ( auto it = _sample + 1; it < samples_end_; ++it )
            {
                if ( value_to_be_aggregated( it ) != val ||
                     it->ix_x_popul != _sample->ix_x_popul )
                    {
                        break;
                    }

                if ( it->activated )
                    {
                        yhat_inline()[_sample->ix_x_popul] -= 1.0;
                        return;
                    }
            }
    }

    // --------------------------------------
    // MAX aggregation

    /// MAX aggregation:
    /// Activate sample - activating a sample means that
    /// it is now included in the aggregation, but has not
    /// been included in the aggregation before.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Max>::value,
            int>::type = 0>
    inline void activate_sample( containers::Match *_sample )
    {
        assert_true( _sample->activated == false );

        ++( count()[_sample->ix_x_popul] );

        assert_true( count()[_sample->ix_x_popul] > 0.0 );

        static_assert( needs_sample_ptr_, "sample_ptr needed" );
        static_assert( needs_count_, "count needed" );
        static_assert( needs_altered_samples_, "altered samples needed" );

        _sample->activated = true;

        altered_samples().push_back( _sample );

        if ( count()[_sample->ix_x_popul] < 1.5 ||
             _sample > sample_ptr()[_sample->ix_x_popul] )
            {
                sample_ptr()[_sample->ix_x_popul] = _sample;

                yhat_inline()[_sample->ix_x_popul] =
                    value_to_be_aggregated( _sample );
            }
    }

    /// MAX aggregation:
    /// Deactivate sample - deactivating a sample means that
    /// it has been activated before, but it is now no longer
    /// included in the aggregation.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Max>::value,
            int>::type = 0>
    void deactivate_sample( containers::Match *_sample )
    {
        assert_true( _sample->activated );

        assert_true( count()[_sample->ix_x_popul] > 0.0 );

        --( count()[_sample->ix_x_popul] );

        _sample->activated = false;

        altered_samples_.push_back( _sample );

        // If there are no activated samples left, then
        // set to zero
        if ( count()[_sample->ix_x_popul] < 0.5 )
            {
                yhat_inline()[_sample->ix_x_popul] = 0.0;

                return;
            }

        // If the deactivated sample was the max value, find the
        // second biggest value
        if ( _sample == sample_ptr()[_sample->ix_x_popul] )
            {
                // The first sample that has the same ix_popul it finds
                // must be the second biggest, because samples have been sorted
                auto it = find_next_smaller( _sample );

                sample_ptr()[it->ix_x_popul] = it;

                yhat_inline()[it->ix_x_popul] = value_to_be_aggregated( it );
            }
    }

    // --------------------------------------
    // MEDIAN aggregation

    /// MEDIAN aggregation:
    /// Activate sample - activating a sample means that
    /// it is now included in the aggregation, but has not
    /// been included in the aggregation before.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Median>::value,
            int>::type = 0>
    inline void activate_sample( containers::Match *_sample )
    {
        assert_true( _sample->activated == false );

        ++( count()[_sample->ix_x_popul] );

        assert_true( count()[_sample->ix_x_popul] > 0.0 );

        assert_true( _sample->activated == false );

        static_assert( needs_sample_ptr_, "sample_ptr needed" );
        static_assert( needs_count_, "count needed" );
        static_assert( needs_altered_samples_, "altered samples needed" );

        _sample->activated = true;

        altered_samples().push_back( _sample );

        // If this is the only activated sample, just take
        // this as the value and return.
        if ( count()[_sample->ix_x_popul] < 1.5 )
            {
                sample_ptr()[_sample->ix_x_popul] = _sample;

                yhat_inline()[_sample->ix_x_popul] =
                    value_to_be_aggregated( _sample );

                return;
            }

        const auto count =
            static_cast<Int>( this->count()[_sample->ix_x_popul] );

        if ( count % 2 == 0 )
            {
                // Number of activated samples is now even,)
                // used to be odd.

                auto it_greater = sample_ptr()[_sample->ix_x_popul];

                auto it_smaller = it_greater;

                // Because we cannot take the average of two containers::Match*,
                // we always store the GREATER one by convention when
                // there is an even number of samples.
                if ( _sample > it_greater )
                    {
                        it_greater = find_next_greater( it_greater );

                        sample_ptr()[_sample->ix_x_popul] = it_greater;
                    }
                else
                    {
                        it_smaller = find_next_smaller( it_smaller );

                        // If _sample < sample_ptr()[_sample->ix_x_popul],
                        // then the new pair consists of the previous sample and
                        // the new one. But, by convention,
                        // sample_ptr()[_sample->ix_x_popul] must point to
                        // the greater one, so
                        // sample_ptr()[_sample->ix_x_popul] does not
                        // change.
                    }

                yhat_inline()[_sample->ix_x_popul] =
                    ( value_to_be_aggregated( it_greater ) +
                      value_to_be_aggregated( it_smaller ) ) /
                    2.0;
            }
        else
            {
                // Number of activated samples is now odd,
                // used to be even.

                auto it = sample_ptr()[_sample->ix_x_popul];

                if ( _sample < it )
                    {
                        it = find_next_smaller( it );

                        sample_ptr()[_sample->ix_x_popul] = it;
                    }

                // We always store the greater containers::Match*. So when the
                // _sample > sample_ptr()[_sample->ix_x_popul],
                // just leave sample_ptr()[_sample->ix_x_popul]
                // as it is.

                yhat_inline()[_sample->ix_x_popul] =
                    value_to_be_aggregated( it );
            }
    }

    /// MEDIAN aggregation:
    /// Deactivate sample - deactivating a sample means that
    /// it has been activated before, but it is now no longer
    /// included in the aggregation.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Median>::value,
            int>::type = 0>
    void deactivate_sample( containers::Match *_sample )
    {
        assert_true( _sample->activated );

        assert_true( count()[_sample->ix_x_popul] > 0.0 );

        assert_true( _sample->activated );

        --( count()[_sample->ix_x_popul] );

        _sample->activated = false;

        altered_samples().push_back( _sample );

        // If there is no sample left, set to zero
        if ( count()[_sample->ix_x_popul] < 0.5 )
            {
                yhat_inline()[_sample->ix_x_popul] = 0.0;

                return;
            }

        const auto count =
            static_cast<Int>( this->count()[_sample->ix_x_popul] );

        if ( count % 2 == 0 )
            {
                // Number of activated samples is now even,
                // used to be odd.

                auto it_greater = sample_ptr()[_sample->ix_x_popul];

                auto it_smaller = it_greater;

                // Because we cannot take the average of two containers::Match*,
                // we always store the GREATER one by convention when
                // there is an even number of samples.
                if ( _sample < it_greater )
                    {
                        it_greater = find_next_greater( it_greater );

                        sample_ptr()[_sample->ix_x_popul] = it_greater;
                    }
                else if ( _sample > it_greater )
                    {
                        it_smaller = find_next_smaller( it_smaller );
                    }
                else
                    {
                        it_greater = find_next_greater( it_greater );

                        it_smaller = find_next_smaller( it_smaller );

                        sample_ptr()[_sample->ix_x_popul] = it_greater;
                    }

                yhat_inline()[_sample->ix_x_popul] =
                    ( value_to_be_aggregated( it_greater ) +
                      value_to_be_aggregated( it_smaller ) ) /
                    2.0;
            }
        else
            {
                // Number of activated samples is now odd,
                // used to be even.

                auto it = sample_ptr()[_sample->ix_x_popul];

                if ( _sample >= it )
                    {
                        it = find_next_smaller( it );

                        sample_ptr()[_sample->ix_x_popul] = it;
                    }

                // If _sample < it, just leave
                // sample_ptr()[_sample->ix_x_popul] as it is.

                yhat_inline()[_sample->ix_x_popul] =
                    value_to_be_aggregated( it );
            }
    }

    // --------------------------------------
    // MIN aggregation

    /// MIN aggregation:
    /// Activate sample - activating a sample means that
    /// it is now included in the aggregation, but has not
    /// been included in the aggregation before.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Min>::value,
            int>::type = 0>
    inline void activate_sample( containers::Match *_sample )
    {
        assert_true( _sample->activated == false );

        ++( count()[_sample->ix_x_popul] );

        assert_true( count()[_sample->ix_x_popul] > 0.0 );

        static_assert( needs_sample_ptr_, "sample_ptr needed" );
        static_assert( needs_count_, "count needed" );
        static_assert( needs_altered_samples_, "altered samples needed" );

        _sample->activated = true;

        altered_samples().push_back( _sample );

        if ( count()[_sample->ix_x_popul] < 1.5 ||
             _sample < sample_ptr()[_sample->ix_x_popul] )
            {
                sample_ptr()[_sample->ix_x_popul] = _sample;

                yhat_inline()[_sample->ix_x_popul] =
                    value_to_be_aggregated( _sample );
            }
    }

    /// MIN aggregation:
    /// Deactivate sample - deactivating a sample means that
    /// it has been activated before, but it is now no longer
    /// included in the aggregation.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Min>::value,
            int>::type = 0>
    void deactivate_sample( containers::Match *_sample )
    {
        assert_true( _sample->activated );

        assert_true( count()[_sample->ix_x_popul] > 0.0 );

        --( count()[_sample->ix_x_popul] );

        _sample->activated = false;

        altered_samples().push_back( _sample );

        // If there are no activated samples left, then
        // set to zero
        if ( count()[_sample->ix_x_popul] < 0.5 )
            {
                yhat_inline()[_sample->ix_x_popul] = 0.0;

                return;
            }

        // If the deactivated sample was the min value, find the
        // second smallest value
        if ( _sample == sample_ptr()[_sample->ix_x_popul] )
            {
                // The first sample that has the same ix_popul it finds
                // must be the second smallest, because sample have been sorted
                auto it = find_next_greater( _sample );

                sample_ptr()[it->ix_x_popul] = it;

                yhat_inline()[it->ix_x_popul] = value_to_be_aggregated( it );
            }
    }

    // --------------------------------------
    // SKEWNESS aggregation

    /// SKEWNESS aggregation:
    /// Activate sample - activating a sample means that
    /// it is now included in the aggregation, but has not
    /// been included in the aggregation before.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Skewness>::value,
            int>::type = 0>
    inline void activate_sample( containers::Match *_sample )
    {
        const Float val = value_to_be_aggregated( _sample );

        static_assert( needs_sum_, "sum needed" );
        static_assert( needs_sum_squared_, "sum_squared needed" );
        static_assert( needs_sum_cubed_, "sum_cubed needed" );
        static_assert( needs_count_, "count needed" );

        sum()[_sample->ix_x_popul] += val;

        sum_squared()[_sample->ix_x_popul] += val * val;

        sum_cubed()[_sample->ix_x_popul] += val * val * val;

        count()[_sample->ix_x_popul] += 1.0;

        assert_true( count()[_sample->ix_x_popul] > 0.0 );

        calculate_skewness( _sample );
    }

    /// SKEWNESS aggregation:
    /// Calculates the skewness of all activated samples.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Skewness>::value,
            int>::type = 0>
    inline void calculate_skewness( containers::Match *_sample )
    {
        if ( count()[_sample->ix_x_popul] == 0.0 )
            {
                yhat_inline()[_sample->ix_x_popul] = 0.0;
            }
        else
            {
                const Float mean =
                    sum()[_sample->ix_x_popul] / count()[_sample->ix_x_popul];

                const Float stddev = std::sqrt(
                    sum_squared()[_sample->ix_x_popul] /
                        count()[_sample->ix_x_popul] -
                    mean * mean );

                const Float skewness = ( ( sum_cubed()[_sample->ix_x_popul] /
                                           count()[_sample->ix_x_popul] ) -
                                         ( 3.0 * mean * stddev * stddev ) -
                                         ( mean * mean * mean ) ) /
                                       ( stddev * stddev * stddev );

                yhat_inline()[_sample->ix_x_popul] =
                    ( skewness != skewness ) ? ( 0.0 ) : ( skewness );
            }
    }

    /// SKEWNESS aggregation:
    /// Deactivate sample - deactivating a sample means that
    /// it has been activated before, but it is now no longer
    /// included in the aggregation.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Skewness>::value,
            int>::type = 0>
    inline void deactivate_sample( containers::Match *_sample )
    {
        const Float val = value_to_be_aggregated( _sample );

        sum()[_sample->ix_x_popul] -= val;

        sum_squared()[_sample->ix_x_popul] -= val * val;

        sum_cubed()[_sample->ix_x_popul] -= val * val * val;

        assert_true( count()[_sample->ix_x_popul] > 0.0 );

        count()[_sample->ix_x_popul] -= 1.0;

        calculate_skewness( _sample );
    }

    // --------------------------------------
    // STDDEV aggregation

    /// STDDEV aggregation:
    /// Activate sample - activating a sample means that
    /// it is now included in the aggregation, but has not
    /// been included in the aggregation before.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Stddev>::value,
            int>::type = 0>
    inline void activate_sample( containers::Match *_sample )
    {
        const Float val = value_to_be_aggregated( _sample );

        static_assert( needs_sum_, "sum needed" );
        static_assert( needs_sum_squared_, "sum_squared needed" );
        static_assert( needs_count_, "count needed" );

        sum()[_sample->ix_x_popul] += val;

        sum_squared()[_sample->ix_x_popul] += val * val;

        count()[_sample->ix_x_popul] += 1.0;

        assert_true( count()[_sample->ix_x_popul] > 0.0 );

        const Float mean =
            sum()[_sample->ix_x_popul] / count()[_sample->ix_x_popul];

        yhat_inline()[_sample->ix_x_popul] = std::sqrt(
            sum_squared()[_sample->ix_x_popul] / count()[_sample->ix_x_popul] -
            mean * mean );

        yhat_inline()[_sample->ix_x_popul] =
            ( yhat_inline()[_sample->ix_x_popul] !=
              yhat_inline()[_sample->ix_x_popul] )
                ? ( 0.0 )
                : ( yhat_inline()[_sample->ix_x_popul] );
    }

    /// STDDEV aggregation:
    /// Deactivate sample - deactivating a sample means that
    /// it has been activated before, but it is now no longer
    /// included in the aggregation.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Stddev>::value,
            int>::type = 0>
    inline void deactivate_sample( containers::Match *_sample )
    {
        const Float val = value_to_be_aggregated( _sample );

        sum()[_sample->ix_x_popul] -= val;

        sum_squared()[_sample->ix_x_popul] -= val * val;

        assert_true( count()[_sample->ix_x_popul] > 0.0 );

        count()[_sample->ix_x_popul] -= 1.0;

        if ( count()[_sample->ix_x_popul] == 0.0 )
            {
                yhat_inline()[_sample->ix_x_popul] = 0.0;
            }
        else
            {
                const Float mean =
                    sum()[_sample->ix_x_popul] / count()[_sample->ix_x_popul];

                yhat_inline()[_sample->ix_x_popul] = std::sqrt(
                    sum_squared()[_sample->ix_x_popul] /
                        count()[_sample->ix_x_popul] -
                    mean * mean );

                yhat_inline()[_sample->ix_x_popul] =
                    ( yhat_inline()[_sample->ix_x_popul] !=
                      yhat_inline()[_sample->ix_x_popul] )
                        ? ( 0.0 )
                        : ( yhat_inline()[_sample->ix_x_popul] );
            }
    }

    // --------------------------------------
    // SUM aggregation

    /// SUM aggregation:
    /// Activate sample - activating a sample means that
    /// it is now included in the aggregation, but has not
    /// been included in the aggregation before.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Sum>::value,
            int>::type = 0>
    inline void activate_sample( containers::Match *_sample )
    {
        yhat_inline()[_sample->ix_x_popul] += value_to_be_aggregated( _sample );
    }

    /// SUM aggregation:
    /// Deactivate sample - deactivating a sample means that
    /// it has been activated before, but it is now no longer
    /// included in the aggregation.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Sum>::value,
            int>::type = 0>
    inline void deactivate_sample( containers::Match *_sample )
    {
        yhat_inline()[_sample->ix_x_popul] -= value_to_be_aggregated( _sample );
    }

    // --------------------------------------
    // VAR aggregation

    /// VAR aggregation:
    /// Activate sample - activating a sample means that
    /// it is now included in the aggregation, but has not
    /// been included in the aggregation before.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Var>::value,
            int>::type = 0>
    inline void activate_sample( containers::Match *_sample )
    {
        const Float val = value_to_be_aggregated( _sample );

        static_assert( needs_sum_, "sum needed" );
        static_assert( needs_sum_squared_, "sum_squared needed" );
        static_assert( needs_count_, "count needed" );

        sum()[_sample->ix_x_popul] += val;

        sum_squared()[_sample->ix_x_popul] += val * val;

        count()[_sample->ix_x_popul] += 1.0;

        assert_true( count()[_sample->ix_x_popul] > 0.0 );

        const Float mean =
            sum()[_sample->ix_x_popul] / count()[_sample->ix_x_popul];

        yhat_inline()[_sample->ix_x_popul] =
            sum_squared()[_sample->ix_x_popul] / count()[_sample->ix_x_popul] -
            mean * mean;
    }

    /// VAR aggregation:
    /// Deactivate sample - deactivating a sample means that
    /// it has been activated before, but it is now no longer
    /// included in the aggregation.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Var>::value,
            int>::type = 0>
    inline void deactivate_sample( containers::Match *_sample )
    {
        const Float val = value_to_be_aggregated( _sample );

        sum()[_sample->ix_x_popul] -= val;

        sum_squared()[_sample->ix_x_popul] -= val * val;

        assert_true( count()[_sample->ix_x_popul] > 0.0 );

        count()[_sample->ix_x_popul] -= 1.0;

        if ( count()[_sample->ix_x_popul] == 0.0 )
            {
                yhat_inline()[_sample->ix_x_popul] = 0.0;
            }
        else
            {
                const Float mean =
                    sum()[_sample->ix_x_popul] / count()[_sample->ix_x_popul];

                yhat_inline()[_sample->ix_x_popul] =
                    sum_squared()[_sample->ix_x_popul] /
                        count()[_sample->ix_x_popul] -
                    mean * mean;
            }
    }

    // --------------------------------------

   public:
    /// Clear all extras
    void clear_extras() final { altered_samples().clear(); }

    /// Returns the mode (enums::Mode::fit or enums::Mode::transform).
    enums::Mode mode() const final { return mode_; }

    /// Whether this is an aggregation that requires the samples to be sorted
    bool needs_sorting() const final { return needs_sorting_; }

    /// Trivial setter
    void set_aggregation_impl(
        containers::Optional<AggregationImpl> *_aggregation_impl )
    {
        aggregation_impl_ = _aggregation_impl->get();
    }

    /// Trivial setter
    void set_optimization_criterion( optimizationcriteria::OptimizationCriterion
                                         *_optimization_criterion ) final
    {
        optimization_criterion_ = _optimization_criterion;
    }

    /// Trivial setter
    void set_samples_begin_end(
        containers::Match *_samples_begin,
        containers::Match *_samples_end ) final
    {
        samples_begin_ = _samples_begin;
        samples_end_ = _samples_end;
    }

    /// Trivial setter
    void set_value_to_be_aggregated(
        const containers::Column<Float> &_value_to_be_aggregated ) final
    {
        value_to_be_aggregated() =
            containers::ColumnView<Float, std::map<Int, Int>>(
                _value_to_be_aggregated );
    }

    /// Trivial setter
    void set_value_to_be_aggregated(
        const containers::Column<Int> &_value_to_be_aggregated ) final
    {
        value_to_be_aggregated_categorical() =
            containers::ColumnView<Int, std::map<Int, Int>>(
                _value_to_be_aggregated );
    }

    /// Trivial setter
    void set_value_to_be_aggregated(
        const containers::ColumnView<Float, std::map<Int, Int>>
            &_value_to_be_aggregated ) final
    {
        value_to_be_aggregated() = _value_to_be_aggregated;
    }

    /// Trivial setter
    void set_value_to_be_compared(
        const containers::Column<Float> &_value_to_be_compared ) final
    {
        value_to_be_compared() =
            containers::ColumnView<Float, std::vector<size_t>>(
                _value_to_be_compared );
    }

    /// Trivial setter
    void set_value_to_be_compared(
        const containers::ColumnView<Float, std::vector<size_t>>
            &_value_to_be_compared ) final
    {
        value_to_be_compared() = _value_to_be_compared;
    }

    /// Returns a string describing the type of the aggregation
    std::string type() const final { return AggType::type(); }

    /// Returns a reference to the predictions stored by the aggregation
    std::vector<Float> &yhat() final
    {
        assert_true( aggregation_impl_ != nullptr );
        return aggregation_impl_->yhat_;
    }

    // --------------------------------------

   private:
    /// Trivial accessor
    std::vector<containers::Match *> &altered_samples()
    {
        return altered_samples_;
    }

    /// Trivial accessor
    inline std::vector<Float> &count()
    {
        assert_true( aggregation_impl_ != nullptr );
        return aggregation_impl_->count_;
    }

    /// Trivial accessor
    inline std::vector<Float> &count_committed()
    {
        assert_true( aggregation_impl_ != nullptr );
        return aggregation_impl_->count_committed_;
    }

    /// Finds the next greater sample that is activated.
    /// Assumes that there is at least one activated sample
    /// greater than _begin.
    inline containers::Match *find_next_greater( containers::Match *_begin )
    {
        auto it = _begin + 1;

        while ( it->activated == false )
            {
                assert_true( it < samples_end_ );
                assert_true( it->ix_x_popul == _begin->ix_x_popul );
                ++it;
            }

        return it;
    }

    /// Finds the next smaller sample that is activated.
    /// Assumes that there is at least one activated sample
    /// smaller than _begin.
    inline containers::Match *find_next_smaller( containers::Match *_begin )
    {
        auto it = _begin - 1;

        while ( it->activated == false )
            {
                assert_true( it >= samples_begin_ );
                assert_true( it->ix_x_popul == _begin->ix_x_popul );
                --it;
            }

        return it;
    }

    /// Trivial accessor
    inline optimizationcriteria::OptimizationCriterion *optimization_criterion()
    {
        assert_true( optimization_criterion_ );
        return optimization_criterion_;
    }

    /// Trivial accessor
    inline std::vector<containers::Match *> &sample_ptr()
    {
        assert_true( aggregation_impl_ != nullptr );
        return aggregation_impl_->sample_ptr_;
    }

    /// Trivial accessor
    inline std::vector<containers::Match *> &sample_ptr_committed()
    {
        assert_true( aggregation_impl_ != nullptr );
        return aggregation_impl_->sample_ptr_committed_;
    }

    /// Trivial accessor
    inline std::vector<Float> &sum()
    {
        assert_true( aggregation_impl_ != nullptr );
        return aggregation_impl_->sum_;
    }

    /// Trivial accessor
    inline std::vector<Float> &sum_committed()
    {
        assert_true( aggregation_impl_ != nullptr );
        return aggregation_impl_->sum_committed_;
    }

    /// Trivial accessor
    inline std::vector<Float> &sum_cubed()
    {
        assert_true( aggregation_impl_ != nullptr );
        return aggregation_impl_->sum_cubed_;
    }

    /// Trivial accessor
    inline std::vector<Float> &sum_cubed_committed()
    {
        assert_true( aggregation_impl_ != nullptr );
        return aggregation_impl_->sum_cubed_committed_;
    }

    /// Trivial accessor
    inline std::vector<Float> &sum_squared()
    {
        assert_true( aggregation_impl_ != nullptr );
        return aggregation_impl_->sum_squared_;
    }

    /// Trivial accessor
    inline std::vector<Float> &sum_squared_committed()
    {
        assert_true( aggregation_impl_ != nullptr );
        return aggregation_impl_->sum_squared_committed_;
    }

    /// Trivial accessor
    inline containers::IntSet &updates_current()
    {
        assert_true( aggregation_impl_ != nullptr );
        return aggregation_impl_->updates_current_;
    }

    /// Trivial accessor
    inline containers::IntSet &updates_stored()
    {
        assert_true( aggregation_impl_ != nullptr );
        return aggregation_impl_->updates_stored_;
    }

    /// Trivial accessor
    inline containers::ColumnView<Float, std::map<Int, Int>>
        &value_to_be_aggregated()
    {
        assert_true( aggregation_impl_ != nullptr );
        return aggregation_impl_->value_to_be_aggregated_;
    }

    /// Trivial accessor
    inline containers::ColumnView<Int, std::map<Int, Int>>
        &value_to_be_aggregated_categorical()
    {
        assert_true( aggregation_impl_ != nullptr );
        return aggregation_impl_->value_to_be_aggregated_categorical_;
    }

    /// Accessor for the value to be aggregated to be used for all
    /// aggregations that ...
    /// 1. ...are NOT applied to categorical data
    /// 2. ...ARE a comparison
    /// 3. ...the value to be compared IS in the population table
    template <
        enums::DataUsed data_used = data_used_,
        bool is_population = is_population_,
        typename std::enable_if<
            !AggregationType::IsCategorical<data_used>::value &&
                AggregationType::IsComparison<data_used>::value &&
                is_population,
            int>::type = 0>
    inline const Float value_to_be_aggregated(
        const containers::Match *_sample )
    {
        return value_to_be_compared()[_sample->ix_x_popul] -
               value_to_be_aggregated().col()[_sample->ix_x_perip];
    }

    /// Accessor for the value to be aggregated to be used for all
    /// aggregations that ...
    /// 1. ...are NOT applied to categorical data
    /// 2. ...ARE a comparison
    /// 3. ...the value to be compared is NOT in the population table
    template <
        enums::DataUsed data_used = data_used_,
        bool is_population = is_population_,
        typename std::enable_if<
            !AggregationType::IsCategorical<data_used>::value &&
                AggregationType::IsComparison<data_used>::value &&
                !is_population,
            int>::type = 0>
    inline const Float value_to_be_aggregated(
        const containers::Match *_sample )
    {
        return value_to_be_compared().col()[_sample->ix_x_perip] -
               value_to_be_aggregated().col()[_sample->ix_x_perip];
    }

    /// Accessor for the value to be aggregated to be used for all
    /// aggregations that ...
    /// 1. ...are NOT applied to categorical data
    /// 2. ...are NOT a comparison
    /// 3. ...are NOT applied to to subfeatures
    template <
        enums::DataUsed data_used = data_used_,
        typename std::enable_if<
            !AggregationType::IsCategorical<data_used>::value &&
                !AggregationType::IsComparison<data_used>::value &&
                data_used != enums::DataUsed::x_subfeature,
            int>::type = 0>
    inline const Float value_to_be_aggregated(
        const containers::Match *_sample )
    {
        return value_to_be_aggregated().col()[_sample->ix_x_perip];
    }

    /// Accessor for the value to be aggregated to be used for all
    /// aggregations that ...
    /// 1. ...are NOT applied to categorical data
    /// 2. ...are NOT a comparison
    /// 3. ...ARE applied to subfeatures
    template <
        enums::DataUsed data_used = data_used_,
        typename std::enable_if<
            !AggregationType::IsCategorical<data_used>::value &&
                !AggregationType::IsComparison<data_used>::value &&
                data_used == enums::DataUsed::x_subfeature,
            int>::type = 0>
    inline const Float value_to_be_aggregated(
        const containers::Match *_sample )
    {
        return value_to_be_aggregated()[static_cast<Int>(
            _sample->ix_x_perip )];
    }

    /// Accessor for the value to be aggregated to be used for all
    /// aggregations that ...
    /// 1. ...ARE applied to categorical data
    template <
        enums::DataUsed data_used = data_used_,
        typename std::enable_if<
            AggregationType::IsCategorical<data_used>::value,
            int>::type = 0>
    inline const Float value_to_be_aggregated(
        const containers::Match *_sample )
    {
        return static_cast<Float>(
            value_to_be_aggregated_categorical().col()[_sample->ix_x_perip] );
    }

    /// Accessor for the value to be compared - this is needed for the
    /// time_stamps and the same_units.
    inline containers::ColumnView<Float, std::vector<size_t>>
        &value_to_be_compared()
    {
        assert_true( aggregation_impl_ != nullptr );
        return aggregation_impl_->value_to_be_compared_;
    }

    /// Accessor for the value to be compared - this is needed for the
    /// time_stamps and the same_units.
    inline std::vector<Float> &yhat_committed()
    {
        assert_true( aggregation_impl_ != nullptr );
        return aggregation_impl_->yhat_committed_;
    }

    /// Trivial accessor
    inline std::vector<Float> &yhat_inline()
    {
        assert_true( aggregation_impl_ != nullptr );
        return aggregation_impl_->yhat_;
    }

    /// Trivial accessor
    inline std::vector<Float> &yhat_stored()
    {
        assert_true( aggregation_impl_ != nullptr );
        return aggregation_impl_->yhat_stored_;
    }

    // --------------------------------------

   private:
    /// Pimpl for aggregation
    AggregationImpl *aggregation_impl_;

    /// Contains pointers to samples that have been changed
    /// since the last commit
    std::vector<containers::Match *> altered_samples_;

    /// Whether the appropriate intermediate aggregation is AVG.
    constexpr static bool avg_intermediate_ =
        std::is_same<AggType, AggregationType::Avg>() ||
        std::is_same<AggType, AggregationType::Max>() ||
        std::is_same<AggType, AggregationType::Median>() ||
        std::is_same<AggType, AggregationType::Min>();

    /// Pointer to the optimization criterion used
    optimizationcriteria::OptimizationCriterion *optimization_criterion_;

    /// Whether the aggregation requires recording which samples have been
    /// altered.
    constexpr static bool needs_altered_samples_ =
        std::is_same<AggType, AggregationType::CountDistinct>() ||
        std::is_same<AggType, AggregationType::CountMinusCountDistinct>() ||
        std::is_same<AggType, AggregationType::Max>() ||
        std::is_same<AggType, AggregationType::Median>() ||
        std::is_same<AggType, AggregationType::Min>();

    /// Whether the aggregation relies on count()
    constexpr static bool needs_count_ =
        std::is_same<AggType, AggregationType::Avg>() ||
        std::is_same<AggType, AggregationType::Max>() ||
        std::is_same<AggType, AggregationType::Median>() ||
        std::is_same<AggType, AggregationType::Min>() ||
        std::is_same<AggType, AggregationType::Skewness>() ||
        std::is_same<AggType, AggregationType::Stddev>() ||
        std::is_same<AggType, AggregationType::Var>();

    /// Whether the aggregation relies on sum()
    constexpr static bool needs_sample_ptr_ =
        std::is_same<AggType, AggregationType::Max>() ||
        std::is_same<AggType, AggregationType::Median>() ||
        std::is_same<AggType, AggregationType::Min>();

    /// Whether the aggregation needs sorting
    constexpr static bool needs_sorting_ =
        std::is_same<AggType, AggregationType::CountDistinct>() ||
        std::is_same<AggType, AggregationType::CountMinusCountDistinct>() ||
        std::is_same<AggType, AggregationType::Max>() ||
        std::is_same<AggType, AggregationType::Median>() ||
        std::is_same<AggType, AggregationType::Min>();

    /// Whether the aggregation relies on sum()
    constexpr static bool needs_sum_ =
        std::is_same<AggType, AggregationType::Avg>() ||
        std::is_same<AggType, AggregationType::Skewness>() ||
        std::is_same<AggType, AggregationType::Stddev>() ||
        std::is_same<AggType, AggregationType::Var>();

    /// Whether the aggregation relies on sum_cubed()
    constexpr static bool needs_sum_cubed_ =
        std::is_same<AggType, AggregationType::Skewness>();

    /// Whether the aggregation relies on sum_squared()
    constexpr static bool needs_sum_squared_ =
        std::is_same<AggType, AggregationType::Skewness>() ||
        std::is_same<AggType, AggregationType::Stddev>() ||
        std::is_same<AggType, AggregationType::Var>();

    /// Whether there is no appropriate intermediate aggregation.
    constexpr static bool no_intermediate_ =
        std::is_same<AggType, AggregationType::Count>() ||
        std::is_same<AggType, AggregationType::CountDistinct>() ||
        std::is_same<AggType, AggregationType::CountMinusCountDistinct>();

    /// Pointer to the first element in samples - some aggregations
    /// like min and max need to know this
    containers::Match *samples_begin_;

    /// Pointer to the element behind the last element in samples -
    /// some aggregations like min and max need to know this
    containers::Match *samples_end_;

    /// Denotes whether the updates since the last commit had
    /// been activated or deactivated. revert_to_commit() needs this
    /// piece of information.
    bool updates_have_been_activated_;
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::activate_all(
    const bool _init_opt,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end )
{
    debug_log( "activate_all..." );

    for ( auto it = _match_container_begin; it != _match_container_end; ++it )
        {
            ( *it )->activated = false;
        }

    for ( auto it = _match_container_begin; it != _match_container_end; ++it )
        {
            activate_sample( *it );
        }

    if ( _init_opt )
        {
            updates_stored().clear();

            for ( auto it = _match_container_begin; it != _match_container_end;
                  ++it )
                {
                    updates_stored().insert( ( *it )->ix_x_popul );
                }

            init_optimization_criterion(
                _match_container_begin, _match_container_end );
        }

    debug_log( "activate_all...done" );
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    activate_samples_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end )
{
    Float num_samples_smaller = 0.0;

    Float num_samples_greater = 0.0;

    for ( auto it = _match_container_begin; it != _match_container_end; ++it )
        {
            bool activate = false;

            for ( auto cat = _categories_begin; cat < _categories_end; ++cat )
                {
                    assert_true(
                        cat == _categories_begin || *cat > *( cat - 1 ) );

                    if ( ( *it )->categorical_value == *cat )
                        {
                            activate = true;
                            break;
                        }
                    else if ( ( *it )->categorical_value < *cat )
                        {
                            break;
                        }
                }

            if ( activate )
                {
                    activate_sample( *it );

                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            updates_stored().insert( ( *it )->ix_x_popul );
                            updates_current().insert( ( *it )->ix_x_popul );

                            ++num_samples_smaller;
                        }
                }
            else
                {
                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            ++num_samples_greater;
                        }
                }
        }

    if constexpr ( mode_ == enums::Mode::fit )
        {
            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    activate_samples_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index )
{
    // ------------------------------------------------------------------

    Float num_samples_smaller = 0.0;

    const auto sample_size =
        static_cast<Float>( std::distance( _index.begin(), _index.end() ) );

    // ------------------------------------------------------------------

    for ( auto cat = _categories_begin; cat < _categories_end; ++cat )
        {
            for ( auto it = _index.begin( *cat ); it < _index.end( *cat );
                  ++it )
                {
                    assert_true( ( *it )->categorical_value == *cat );
                    activate_sample( *it );

                    updates_stored().insert( ( *it )->ix_x_popul );
                    updates_current().insert( ( *it )->ix_x_popul );

                    ++num_samples_smaller;
                }

            if ( _revert != Revert::not_at_all )
                {
                    update_optimization_criterion_and_clear_updates_current(
                        num_samples_smaller,
                        sample_size - num_samples_smaller );
                }

            if ( _revert == Revert::after_each_category )
                {
                    revert_to_commit();
                    optimization_criterion()->revert_to_commit();
                    num_samples_smaller = 0.0;
                }
        }

    // ------------------------------------------------------------------

    if ( _revert == Revert::after_all_categories )
        {
            revert_to_commit();
            optimization_criterion()->revert_to_commit();
        }
    else if ( _revert == Revert::not_at_all )
        {
            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, sample_size - num_samples_smaller );
        }

    // ------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    activate_samples_from_above(
        const Float _critical_value,
        containers::MatchPtrs::const_iterator _match_container_begin,
        containers::MatchPtrs::const_iterator _match_container_end )
{
    Float num_samples_smaller = 0.0;

    Float num_samples_greater = 0.0;

    for ( auto it = _match_container_begin; it != _match_container_end; ++it )
        {
            if ( ( *it )->numerical_value > _critical_value )
                {
                    activate_sample( *it );

                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            updates_stored().insert( ( *it )->ix_x_popul );
                            updates_current().insert( ( *it )->ix_x_popul );

                            ++num_samples_greater;
                        }
                }
            else
                {
                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            ++num_samples_smaller;
                        }
                }
        }

    if constexpr ( mode_ == enums::Mode::fit )
        {
            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    activate_samples_from_above(
        const std::vector<size_t> &_indptr,
        const containers::MatchPtrs::const_iterator &_matches_begin,
        const containers::MatchPtrs::const_iterator &_matches_end )
{
    assert_true( _matches_begin <= _matches_end );

    const auto sample_size =
        static_cast<size_t>( std::distance( _matches_begin, _matches_end ) );

    assert_true( _indptr.back() <= sample_size );

    const auto num_nans = static_cast<Float>( sample_size - _indptr.back() );

    for ( size_t i = 1; i < _indptr.size(); ++i )
        {
            assert_true( _indptr[i - 1] <= _indptr[i] );
            assert_true( _indptr[i] <= _indptr.back() );

            const auto begin = _matches_begin + _indptr[i - 1];
            const auto end = _matches_begin + _indptr[i];

            for ( auto it = begin; it != end; ++it )
                {
                    activate_sample( *it );

                    updates_stored().insert( ( *it )->ix_x_popul );
                    updates_current().insert( ( *it )->ix_x_popul );
                }

            const auto num_samples_greater = static_cast<Float>( _indptr[i] );

            const auto num_samples_smaller =
                static_cast<Float>( _indptr.back() - _indptr[i] ) + num_nans;

            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    activate_samples_from_below(
        const Float _critical_value,
        containers::MatchPtrs::const_iterator _match_container_begin,
        containers::MatchPtrs::const_iterator _match_container_end )
{
    Float num_samples_smaller = 0.0;

    Float num_samples_greater = 0.0;

    for ( auto it = _match_container_begin; it != _match_container_end; ++it )
        {
            if ( ( *it )->numerical_value <= _critical_value )
                {
                    activate_sample( *it );

                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            updates_stored().insert( ( *it )->ix_x_popul );
                            updates_current().insert( ( *it )->ix_x_popul );

                            ++num_samples_smaller;
                        }
                }
            else
                {
                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            ++num_samples_greater;
                        }
                }
        }

    if constexpr ( mode_ == enums::Mode::fit )
        {
            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    activate_samples_from_below(
        const std::vector<size_t> &_indptr,
        const containers::MatchPtrs::const_iterator &_matches_begin,
        const containers::MatchPtrs::const_iterator &_matches_end )
{
    assert_true( _indptr.size() > 0 );

    assert_true( _matches_begin <= _matches_end );

    const auto sample_size =
        static_cast<size_t>( std::distance( _matches_begin, _matches_end ) );

    assert_true( _indptr.back() <= sample_size );

    const auto num_nans = static_cast<Float>( sample_size - _indptr.back() );

    for ( size_t i = _indptr.size() - 1; i > 0; --i )
        {
            assert_true( _indptr[i - 1] <= _indptr[i] );
            assert_true( _indptr[i] <= _indptr.back() );

            const auto begin = _matches_begin + _indptr[i - 1];
            const auto end = _matches_begin + _indptr[i];

            for ( auto it = begin; it != end; ++it )
                {
                    activate_sample( *it );

                    updates_stored().insert( ( *it )->ix_x_popul );
                    updates_current().insert( ( *it )->ix_x_popul );
                }

            const auto num_samples_greater =
                static_cast<Float>( _indptr[i] ) + num_nans;

            const auto num_samples_smaller =
                static_cast<Float>( _indptr.back() - _indptr[i] );

            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    activate_samples_in_window(
        const Float _critical_value,
        const Float _delta_t,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end )
{
    Float num_samples_smaller = 0.0;

    Float num_samples_greater = 0.0;

    for ( auto it = _match_container_begin; it != _match_container_end; ++it )
        {
            if ( ( *it )->numerical_value > _critical_value - _delta_t &&
                 ( *it )->numerical_value <= _critical_value )
                {
                    activate_sample( *it );

                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            updates_stored().insert( ( *it )->ix_x_popul );
                            updates_current().insert( ( *it )->ix_x_popul );

                            ++num_samples_smaller;
                        }
                }
            else
                {
                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            ++num_samples_greater;
                        }
                }
        }

    if constexpr ( mode_ == enums::Mode::fit )
        {
            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    activate_samples_in_window(
        const std::vector<size_t> &_indptr,
        containers::MatchPtrs::iterator _matches_begin,
        containers::MatchPtrs::iterator _matches_end )
{
    // ------------------------------------------------------------------

    assert_true( _indptr.size() > 0 );

    assert_true( _matches_end > _matches_begin );

    const Float sample_size =
        static_cast<Float>( std::distance( _matches_begin, _matches_end ) );

    Float num_samples_smaller = 0.0;

    // ------------------------------------------------------------------

    for ( size_t i = 1; i < _indptr.size(); ++i )
        {
            assert_true( _indptr[i - 1] <= _indptr[i] );
            assert_true( _matches_begin <= _matches_end );
            assert_true(
                _indptr[i] <= static_cast<size_t>( std::distance(
                                  _matches_begin, _matches_end ) ) );
            assert_true( _indptr[i] <= _indptr.back() );

            const auto begin = _matches_begin + _indptr[i - 1];
            const auto end = _matches_begin + _indptr[i];

            for ( auto it = begin; it != end; ++it )
                {
                    activate_sample( *it );

                    updates_stored().insert( ( *it )->ix_x_popul );
                    updates_current().insert( ( *it )->ix_x_popul );

                    ++num_samples_smaller;
                }

            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, sample_size - num_samples_smaller );

            revert_to_commit();

            optimization_criterion()->revert_to_commit();

            num_samples_smaller = 0.0;
        }

    // ------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    activate_samples_outside_window(
        const Float _critical_value,
        const Float _delta_t,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end )
{
    Float num_samples_smaller = 0.0;

    Float num_samples_greater = 0.0;

    for ( auto it = _match_container_begin; it != _match_container_end; ++it )
        {
            if ( ( *it )->numerical_value <= _critical_value - _delta_t ||
                 ( *it )->numerical_value > _critical_value )
                {
                    activate_sample( *it );

                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            updates_stored().insert( ( *it )->ix_x_popul );
                            updates_current().insert( ( *it )->ix_x_popul );

                            ++num_samples_greater;
                        }
                }
            else
                {
                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            ++num_samples_smaller;
                        }
                }
        }

    if constexpr ( mode_ == enums::Mode::fit )
        {
            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    activate_samples_outside_window(
        const std::vector<size_t> &_indptr,
        containers::MatchPtrs::iterator _matches_begin,
        containers::MatchPtrs::iterator _matches_end )
{
    // ------------------------------------------------------------------

    assert_true( _indptr.size() > 0 );

    assert_true( _matches_end > _matches_begin );

    const Float sample_size =
        static_cast<Float>( std::distance( _matches_begin, _matches_end ) );

    Float num_samples_smaller = 0.0;

    // ------------------------------------------------------------------
    // Activate all samples

    for ( auto it = _matches_begin; it != _matches_end; ++it )
        {
            activate_sample( *it );

            updates_stored().insert( ( *it )->ix_x_popul );
            updates_current().insert( ( *it )->ix_x_popul );
        }

    // ------------------------------------------------------------------
    // Selectively deactivate those samples that are inside the window.

    for ( size_t i = 1; i < _indptr.size(); ++i )
        {
            assert_true( _indptr[i - 1] <= _indptr[i] );
            assert_true( _matches_begin <= _matches_end );
            assert_true(
                _indptr[i] <= static_cast<size_t>( std::distance(
                                  _matches_begin, _matches_end ) ) );
            assert_true( _indptr[i] <= _indptr.back() );

            const auto begin = _matches_begin + _indptr[i - 1];
            const auto end = _matches_begin + _indptr[i];

            for ( auto it = begin; it != end; ++it )
                {
                    deactivate_sample( *it );

                    updates_current().insert( ( *it )->ix_x_popul );

                    ++num_samples_smaller;
                }

            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, sample_size - num_samples_smaller );

            for ( auto it = begin; it != end; ++it )
                {
                    activate_sample( *it );

                    updates_current().insert( ( *it )->ix_x_popul );
                }

            num_samples_smaller = 0.0;
        }

    // ------------------------------------------------------------------
    // Revert to the original commit

    revert_to_commit();

    optimization_criterion()->revert_to_commit();

    // ------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    activate_samples_not_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end )
{
    Float num_samples_smaller = 0.0;

    Float num_samples_greater = 0.0;

    for ( auto it = _match_container_begin; it != _match_container_end; ++it )
        {
            auto activate = true;

            for ( auto cat = _categories_begin; cat < _categories_end; ++cat )
                {
                    assert_true(
                        cat == _categories_begin || *cat > *( cat - 1 ) );

                    if ( ( *it )->categorical_value == *cat )
                        {
                            activate = false;
                            break;
                        }
                    else if ( ( *it )->categorical_value < *cat )
                        {
                            break;
                        }
                }

            if ( activate )
                {
                    activate_sample( *it );

                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            updates_stored().insert( ( *it )->ix_x_popul );
                            updates_current().insert( ( *it )->ix_x_popul );

                            ++num_samples_greater;
                        }
                }
            else
                {
                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            ++num_samples_smaller;
                        }
                }
        }

    if constexpr ( mode_ == enums::Mode::fit )
        {
            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    activate_samples_not_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index )
{
    // ------------------------------------------------------------------
    // Activate all samples

    for ( auto it = _index.begin(); it != _index.end(); ++it )
        {
            activate_sample( *it );

            updates_stored().insert( ( *it )->ix_x_popul );
            updates_current().insert( ( *it )->ix_x_popul );
        }

    // ------------------------------------------------------------------
    // Selectively deactivate those samples that are not of the
    // particular category

    Float num_samples_smaller = 0.0;

    auto sample_size =
        static_cast<Float>( std::distance( _index.begin(), _index.end() ) );

    for ( auto cat = _categories_begin; cat < _categories_end; ++cat )
        {
            for ( auto it = _index.begin( *cat ); it < _index.end( *cat );
                  ++it )
                {
                    assert_true( ( *it )->categorical_value == *cat );

                    deactivate_sample( *it );

                    updates_current().insert( ( *it )->ix_x_popul );

                    ++num_samples_smaller;
                }

            if ( _revert != Revert::not_at_all )
                {
                    update_optimization_criterion_and_clear_updates_current(
                        num_samples_smaller,
                        sample_size - num_samples_smaller );
                }

            if ( _revert == Revert::after_each_category )
                {
                    for ( auto it = _index.begin( *cat );
                          it < _index.end( *cat );
                          ++it )
                        {
                            assert_true( ( *it )->categorical_value == *cat );

                            activate_sample( *it );

                            updates_current().insert( ( *it )->ix_x_popul );
                        }

                    num_samples_smaller = 0.0;
                }
        }

    // ------------------------------------------------------------------
    // Revert to the original commit

    if ( _revert != Revert::not_at_all )
        {
            revert_to_commit();
            optimization_criterion()->revert_to_commit();
        }
    else
        {
            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, sample_size - num_samples_smaller );
        }

    // ------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::clear()
{
    altered_samples().clear();

    value_to_be_aggregated().clear();

    value_to_be_aggregated_categorical().clear();

    value_to_be_compared().clear();

    updates_current().clear();

    updates_stored().clear();
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::commit()
{
    // --------------------------------------------------

    if constexpr ( needs_altered_samples_ )
        {
            altered_samples().clear();
        }

    // --------------------------------------------------

    if constexpr ( needs_count_ )
        {
            for ( auto i : updates_stored() )
                {
                    count_committed()[i] = count()[i];
                }
        }

    // --------------------------------------------------

    if constexpr ( needs_sample_ptr_ )
        {
            for ( auto i : updates_stored() )
                {
                    sample_ptr_committed()[i] = sample_ptr()[i];
                }
        }

    // --------------------------------------------------

    if constexpr ( needs_sum_ )
        {
            for ( auto i : updates_stored() )
                {
                    sum_committed()[i] = sum()[i];
                }
        }

    // --------------------------------------------------

    if constexpr ( needs_sum_cubed_ )
        {
            for ( auto i : updates_stored() )
                {
                    sum_cubed_committed()[i] = sum_cubed()[i];
                }
        }

    // --------------------------------------------------

    if constexpr ( needs_sum_squared_ )
        {
            for ( auto i : updates_stored() )
                {
                    sum_squared_committed()[i] = sum_squared()[i];
                }
        }

    // --------------------------------------------------

    for ( auto i : updates_stored() )
        {
            yhat_committed()[i] = yhat_stored()[i] = yhat_inline()[i];
        }

    // --------------------------------------------------

    updates_current().clear();

    updates_stored().clear();

    // --------------------------------------------------
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    deactivate_samples_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end )
{
    Float num_samples_smaller = 0.0;

    Float num_samples_greater = 0.0;

    for ( auto it = _match_container_begin; it != _match_container_end; ++it )
        {
            bool deactivate = false;

            for ( auto cat = _categories_begin; cat < _categories_end; ++cat )
                {
                    assert_true(
                        cat == _categories_begin || *cat > *( cat - 1 ) );

                    if ( ( *it )->categorical_value == *cat )
                        {
                            deactivate = true;
                            break;
                        }

                    else if ( ( *it )->categorical_value < *cat )
                        {
                            break;
                        }
                }

            if ( deactivate )
                {
                    deactivate_sample( *it );

                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            updates_stored().insert( ( *it )->ix_x_popul );
                            updates_current().insert( ( *it )->ix_x_popul );

                            ++num_samples_smaller;
                        }
                }
            else
                {
                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            ++num_samples_greater;
                        }
                }
        }

    if constexpr ( mode_ == enums::Mode::fit )
        {
            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    deactivate_samples_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index )
{
    // ------------------------------------------------------------------

    Float num_samples_smaller = 0.0;

    auto sample_size =
        static_cast<Float>( std::distance( _index.begin(), _index.end() ) );

    // ------------------------------------------------------------------

    for ( auto cat = _categories_begin; cat < _categories_end; ++cat )
        {
            for ( auto it = _index.begin( *cat ); it < _index.end( *cat );
                  ++it )
                {
                    assert_true( ( *it )->categorical_value == *cat );
                    deactivate_sample( *it );

                    updates_stored().insert( ( *it )->ix_x_popul );
                    updates_current().insert( ( *it )->ix_x_popul );

                    ++num_samples_smaller;
                }

            if ( _revert != Revert::not_at_all )
                {
                    update_optimization_criterion_and_clear_updates_current(
                        num_samples_smaller,
                        sample_size - num_samples_smaller );
                }

            if ( _revert == Revert::after_each_category )
                {
                    revert_to_commit();
                    optimization_criterion()->revert_to_commit();
                    num_samples_smaller = 0.0;
                }
        }

    // ------------------------------------------------------------------

    if ( _revert == Revert::after_all_categories )
        {
            revert_to_commit();
            optimization_criterion()->revert_to_commit();
        }
    else if ( _revert == Revert::not_at_all )
        {
            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, sample_size - num_samples_smaller );
        }

    // ------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    deactivate_samples_from_above(
        const Float _critical_value,
        containers::MatchPtrs::const_iterator _match_container_begin,
        containers::MatchPtrs::const_iterator _match_container_end )
{
    Float num_samples_smaller = 0.0;

    Float num_samples_greater = 0.0;

    for ( auto it = _match_container_begin; it != _match_container_end; ++it )
        {
            const Float val = ( *it )->numerical_value;

            if ( val > _critical_value || std::isnan( val ) ||
                 std::isinf( val ) )
                {
                    deactivate_sample( *it );

                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            updates_stored().insert( ( *it )->ix_x_popul );
                            updates_current().insert( ( *it )->ix_x_popul );

                            ++num_samples_greater;
                        }
                }
            else
                {
                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            ++num_samples_smaller;
                        }
                }
        }

    if constexpr ( mode_ == enums::Mode::fit )
        {
            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    deactivate_samples_from_above(
        const std::vector<size_t> &_indptr,
        const containers::MatchPtrs::const_iterator &_matches_begin,
        const containers::MatchPtrs::const_iterator &_matches_end )
{
    assert_true( _matches_begin <= _matches_end );

    const auto sample_size =
        static_cast<size_t>( std::distance( _matches_begin, _matches_end ) );

    assert_true( _indptr.back() <= sample_size );

    const auto num_nans = static_cast<Float>( sample_size - _indptr.back() );

    for ( size_t i = 1; i < _indptr.size(); ++i )
        {
            assert_true( _indptr[i - 1] <= _indptr[i] );
            assert_true( _indptr[i] <= _indptr.back() );

            const auto begin = _matches_begin + _indptr[i - 1];
            const auto end = _matches_begin + _indptr[i];

            for ( auto it = begin; it != end; ++it )
                {
                    deactivate_sample( *it );

                    updates_stored().insert( ( *it )->ix_x_popul );
                    updates_current().insert( ( *it )->ix_x_popul );
                }

            const auto num_samples_greater =
                static_cast<Float>( _indptr[i] ) + num_nans;

            const auto num_samples_smaller =
                static_cast<Float>( _indptr.back() - _indptr[i] );

            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    deactivate_samples_from_below(
        const Float _critical_value,
        containers::MatchPtrs::const_iterator _match_container_begin,
        containers::MatchPtrs::const_iterator _match_container_end )
{
    Float num_samples_smaller = 0.0;

    Float num_samples_greater = 0.0;

    for ( auto it = _match_container_begin; it != _match_container_end; ++it )
        {
            const Float val = ( *it )->numerical_value;

            if ( val <= _critical_value || std::isnan( val ) ||
                 std::isinf( val ) )
                {
                    deactivate_sample( *it );

                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            updates_stored().insert( ( *it )->ix_x_popul );
                            updates_current().insert( ( *it )->ix_x_popul );

                            ++num_samples_smaller;
                        }
                }
            else
                {
                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            ++num_samples_greater;
                        }
                }
        }

    if constexpr ( mode_ == enums::Mode::fit )
        {
            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    deactivate_samples_from_below(
        const std::vector<size_t> &_indptr,
        const containers::MatchPtrs::const_iterator &_matches_begin,
        const containers::MatchPtrs::const_iterator &_matches_end )
{
    assert_true( _indptr.size() > 0 );

    assert_true( _matches_begin <= _matches_end );

    const auto sample_size =
        static_cast<size_t>( std::distance( _matches_begin, _matches_end ) );

    assert_true( _indptr.back() <= sample_size );

    const auto num_nans = static_cast<Float>( sample_size - _indptr.back() );

    for ( size_t i = _indptr.size() - 1; i > 0; --i )
        {
            assert_true( _indptr[i - 1] <= _indptr[i] );
            assert_true( _indptr[i] <= _indptr.back() );

            const auto begin = _matches_begin + _indptr[i - 1];
            const auto end = _matches_begin + _indptr[i];

            for ( auto it = begin; it != end; ++it )
                {
                    deactivate_sample( *it );

                    updates_stored().insert( ( *it )->ix_x_popul );
                    updates_current().insert( ( *it )->ix_x_popul );
                }

            const auto num_samples_greater = static_cast<Float>( _indptr[i] );

            const auto num_samples_smaller =
                static_cast<Float>( _indptr.back() - _indptr[i] ) + num_nans;

            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    deactivate_samples_in_window(
        const Float _critical_value,
        const Float _delta_t,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end )
{
    Float num_samples_smaller = 0.0;

    Float num_samples_greater = 0.0;

    for ( auto it = _match_container_begin; it != _match_container_end; ++it )
        {
            if ( ( *it )->numerical_value > _critical_value - _delta_t &&
                 ( *it )->numerical_value <= _critical_value )
                {
                    deactivate_sample( *it );

                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            updates_stored().insert( ( *it )->ix_x_popul );
                            updates_current().insert( ( *it )->ix_x_popul );

                            ++num_samples_smaller;
                        }
                }
            else
                {
                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            ++num_samples_greater;
                        }
                }
        }

    if constexpr ( mode_ == enums::Mode::fit )
        {
            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    deactivate_samples_in_window(
        const std::vector<size_t> &_indptr,
        containers::MatchPtrs::iterator _matches_begin,
        containers::MatchPtrs::iterator _matches_end )
{
    // ------------------------------------------------------------------

    assert_true( _indptr.size() > 0 );

    assert_true( _matches_end > _matches_begin );

    const Float sample_size =
        static_cast<Float>( std::distance( _matches_begin, _matches_end ) );

    Float num_samples_smaller = 0.0;

    // ------------------------------------------------------------------

    for ( size_t i = 1; i < _indptr.size(); ++i )
        {
            assert_true( _indptr[i - 1] <= _indptr[i] );
            assert_true( _matches_begin <= _matches_end );
            assert_true(
                _indptr[i] <= static_cast<size_t>( std::distance(
                                  _matches_begin, _matches_end ) ) );
            assert_true( _indptr[i] <= _indptr.back() );

            const auto begin = _matches_begin + _indptr[i - 1];
            const auto end = _matches_begin + _indptr[i];

            for ( auto it = begin; it != end; ++it )
                {
                    deactivate_sample( *it );

                    updates_stored().insert( ( *it )->ix_x_popul );
                    updates_current().insert( ( *it )->ix_x_popul );

                    ++num_samples_smaller;
                }

            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, sample_size - num_samples_smaller );

            revert_to_commit();

            optimization_criterion()->revert_to_commit();

            num_samples_smaller = 0.0;
        }

    // ------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    deactivate_samples_outside_window(
        const Float _critical_value,
        const Float _delta_t,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end )
{
    Float num_samples_smaller = 0.0;

    Float num_samples_greater = 0.0;

    for ( auto it = _match_container_begin; it != _match_container_end; ++it )
        {
            if ( ( *it )->numerical_value <= _critical_value - _delta_t ||
                 ( *it )->numerical_value > _critical_value )
                {
                    deactivate_sample( *it );

                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            updates_stored().insert( ( *it )->ix_x_popul );
                            updates_current().insert( ( *it )->ix_x_popul );

                            ++num_samples_greater;
                        }
                }
            else
                {
                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            ++num_samples_smaller;
                        }
                }
        }

    if constexpr ( mode_ == enums::Mode::fit )
        {
            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    deactivate_samples_outside_window(
        const std::vector<size_t> &_indptr,
        containers::MatchPtrs::iterator _matches_begin,
        containers::MatchPtrs::iterator _matches_end )
{
    // ------------------------------------------------------------------

    assert_true( _indptr.size() > 0 );

    assert_true( _matches_end > _matches_begin );

    const Float sample_size =
        static_cast<Float>( std::distance( _matches_begin, _matches_end ) );

    Float num_samples_smaller = 0.0;

    // ------------------------------------------------------------------
    // Deactivate all samples

    for ( auto it = _matches_begin; it != _matches_end; ++it )
        {
            deactivate_sample( *it );

            updates_stored().insert( ( *it )->ix_x_popul );
            updates_current().insert( ( *it )->ix_x_popul );
        }

    // ------------------------------------------------------------------
    // Selectively activate those samples that are inside the window.

    for ( size_t i = 1; i < _indptr.size(); ++i )
        {
            assert_true( _indptr[i - 1] <= _indptr[i] );
            assert_true( _matches_begin <= _matches_end );
            assert_true(
                _indptr[i] <= static_cast<size_t>( std::distance(
                                  _matches_begin, _matches_end ) ) );
            assert_true( _indptr[i] <= _indptr.back() );

            const auto begin = _matches_begin + _indptr[i - 1];
            const auto end = _matches_begin + _indptr[i];

            for ( auto it = begin; it != end; ++it )
                {
                    activate_sample( *it );

                    updates_current().insert( ( *it )->ix_x_popul );

                    ++num_samples_smaller;
                }

            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, sample_size - num_samples_smaller );

            for ( auto it = begin; it != end; ++it )
                {
                    deactivate_sample( *it );

                    updates_current().insert( ( *it )->ix_x_popul );
                }

            num_samples_smaller = 0.0;
        }

    // ------------------------------------------------------------------
    // Revert to the original commit

    revert_to_commit();

    optimization_criterion()->revert_to_commit();

    // ------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    deactivate_samples_not_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end )
{
    Float num_samples_smaller = 0.0;

    Float num_samples_greater = 0.0;

    for ( auto it = _match_container_begin; it != _match_container_end; ++it )
        {
            auto deactivate = true;

            for ( auto cat = _categories_begin; cat < _categories_end; ++cat )
                {
                    assert_true(
                        cat == _categories_begin || *cat > *( cat - 1 ) );

                    if ( ( *it )->categorical_value == *cat )
                        {
                            deactivate = false;
                            break;
                        }
                    else if ( ( *it )->categorical_value < *cat )
                        {
                            break;
                        }
                }

            if ( deactivate )
                {
                    deactivate_sample( *it );

                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            updates_stored().insert( ( *it )->ix_x_popul );
                            updates_current().insert( ( *it )->ix_x_popul );

                            ++num_samples_greater;
                        }
                }
            else
                {
                    if constexpr ( mode_ == enums::Mode::fit )
                        {
                            ++num_samples_smaller;
                        }
                }
        }

    if constexpr ( mode_ == enums::Mode::fit )
        {
            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    deactivate_samples_not_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index )
{
    // ------------------------------------------------------------------
    // Deactivate all samples

    for ( auto it = _index.begin(); it != _index.end(); ++it )
        {
            deactivate_sample( *it );

            updates_stored().insert( ( *it )->ix_x_popul );
            updates_current().insert( ( *it )->ix_x_popul );
        }

    // ------------------------------------------------------------------
    // Selectively activate those samples that are not of the
    // particular category

    const auto sample_size =
        static_cast<Float>( std::distance( _index.begin(), _index.end() ) );

    Float num_samples_smaller = 0.0;

    for ( auto cat = _categories_begin; cat < _categories_end; ++cat )
        {
            for ( auto it = _index.begin( *cat ); it < _index.end( *cat );
                  ++it )
                {
                    assert_true( ( *it )->categorical_value == *cat );

                    activate_sample( *it );

                    updates_current().insert( ( *it )->ix_x_popul );

                    ++num_samples_smaller;
                }

            if ( _revert != Revert::not_at_all )
                {
                    update_optimization_criterion_and_clear_updates_current(
                        num_samples_smaller,
                        sample_size - num_samples_smaller );
                }

            if ( _revert == Revert::after_each_category )
                {
                    for ( auto it = _index.begin( *cat );
                          it < _index.end( *cat );
                          ++it )
                        {
                            assert_true( ( *it )->categorical_value == *cat );

                            deactivate_sample( *it );

                            updates_current().insert( ( *it )->ix_x_popul );
                        }

                    num_samples_smaller = 0.0;
                }
        }

    // ------------------------------------------------------------------
    // Revert to the original commit

    if ( _revert != Revert::not_at_all )
        {
            revert_to_commit();
            optimization_criterion()->revert_to_commit();
        }
    else
        {
            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, sample_size - num_samples_smaller );
        }

    // ------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    deactivate_samples_with_null_values(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _null_values_separator )
{
    assert_true( _null_values_separator >= _match_container_begin );

    for ( auto it = _match_container_begin; it != _null_values_separator; ++it )
        {
            deactivate_sample( *it );

            updates_stored().insert( ( *it )->ix_x_popul );
            updates_current().insert( ( *it )->ix_x_popul );
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    init_optimization_criterion(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end )
{
    debug_log( "init_optimization_criterion..." );

    optimization_criterion()->init_yhat( yhat_inline(), updates_stored() );

    auto num_samples = static_cast<Float>(
        std::distance( _match_container_begin, _match_container_end ) );

    optimization_criterion()->store_current_stage( num_samples, num_samples );

    optimization_criterion()->find_maximum();

    debug_log( "init_optimization_criterion...done" );
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
std::string
Aggregation<AggType, data_used_, mode_, is_population_>::intermediate_type()
    const
{
    // --------------------------------------------------
    // AVG, MAX, MEDIAN, MIN

    if ( avg_intermediate_ )
        {
            return "AVG";
        }

    // --------------------------------------------------
    // COUNT, COUNT_DISTINCT, COUNT_MINUS_COUNT_DISTINCT

    else if ( no_intermediate_ )
        {
            return "none";
        }

    // --------------------------------------------------
    // all others

    else
        {
            return AggType::type();
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
std::shared_ptr<optimizationcriteria::OptimizationCriterion>
Aggregation<AggType, data_used_, mode_, is_population_>::make_intermediate(
    std::shared_ptr<IntermediateAggregationImpl> _impl ) const
{
    // --------------------------------------------------

    debug_log( "make_intermediate..." );

    assert_true( !no_intermediate_ );

    // --------------------------------------------------
    // AVG, MAX, MEDIAN, MIN

    if ( avg_intermediate_ )
        {
            return std::make_shared<
                IntermediateAggregation<AggregationType::Avg>>( _impl );
        }

    // --------------------------------------------------
    // STDDEV

    else if ( std::is_same<AggType, AggregationType::Stddev>() )
        {
            return std::make_shared<
                IntermediateAggregation<AggregationType::Stddev>>( _impl );
        }

    // --------------------------------------------------
    // SKEWNESS

    else if ( std::is_same<AggType, AggregationType::Skewness>() )
        {
            return std::make_shared<
                IntermediateAggregation<AggregationType::Skewness>>( _impl );
        }

    // --------------------------------------------------
    // SUM

    else if ( std::is_same<AggType, AggregationType::Sum>() )
        {
            return std::make_shared<
                IntermediateAggregation<AggregationType::Sum>>( _impl );
        }

    // --------------------------------------------------
    // VAR

    else if ( std::is_same<AggType, AggregationType::Var>() )
        {
            return std::make_shared<
                IntermediateAggregation<AggregationType::Var>>( _impl );
        }

    // --------------------------------------------------

    else
        {
            assert_true(
                false &&
                "Unknown aggregation type in make_intermediate(...)!" );

            return std::shared_ptr<
                optimizationcriteria::OptimizationCriterion>();
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::revert_to_commit()
{
    // --------------------------------------------------

    if constexpr ( needs_altered_samples_ )
        {
            for ( containers::Match *sample : altered_samples() )
                {
                    sample->activated = !( sample->activated );
                }

            altered_samples().clear();
        }

    // --------------------------------------------------

    if constexpr ( needs_count_ )
        {
            for ( auto i : updates_stored() )
                {
                    count()[i] = count_committed()[i];
                }
        }

    // --------------------------------------------------

    if constexpr ( needs_sample_ptr_ )
        {
            for ( auto i : updates_stored() )
                {
                    sample_ptr()[i] = sample_ptr_committed()[i];
                }
        }

    // --------------------------------------------------

    if constexpr ( needs_sum_ )
        {
            for ( auto i : updates_stored() )
                {
                    sum()[i] = sum_committed()[i];
                }
        }

    // --------------------------------------------------

    if constexpr ( needs_sum_cubed_ )
        {
            for ( auto i : updates_stored() )
                {
                    sum_cubed()[i] = sum_cubed_committed()[i];
                }
        }

    // --------------------------------------------------

    if constexpr ( needs_sum_squared_ )
        {
            for ( auto i : updates_stored() )
                {
                    sum_squared()[i] = sum_squared_committed()[i];
                }
        }

    // --------------------------------------------------

    for ( auto i : updates_stored() )
        {
            yhat_inline()[i] = yhat_stored()[i] = yhat_committed()[i];
        }

    // --------------------------------------------------

    updates_current().clear();

    updates_stored().clear();

    // --------------------------------------------------
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
containers::Matches::iterator
Aggregation<AggType, data_used_, mode_, is_population_>::separate_null_values(
    containers::Matches *_matches )
{
    auto is_null = [this]( containers::Match &sample ) {
        Float val = value_to_be_aggregated( &sample );
        return ( std::isnan( val ) || std::isinf( val ) );
    };

    if ( std::is_partitioned( _matches->begin(), _matches->end(), is_null ) )
        {
            return std::partition_point(
                _matches->begin(), _matches->end(), is_null );
        }
    else
        {
            return std::stable_partition(
                _matches->begin(), _matches->end(), is_null );
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
containers::MatchPtrs::iterator
Aggregation<AggType, data_used_, mode_, is_population_>::separate_null_values(
    containers::MatchPtrs *_match_ptrs )
{
    auto is_null = [this]( containers::Match *sample ) {
        Float val = value_to_be_aggregated( sample );
        return ( std::isnan( val ) || std::isinf( val ) );
    };

    if ( std::is_partitioned(
             _match_ptrs->begin(), _match_ptrs->end(), is_null ) )
        {
            return std::partition_point(
                _match_ptrs->begin(), _match_ptrs->end(), is_null );
        }
    else
        {
            return std::stable_partition(
                _match_ptrs->begin(), _match_ptrs->end(), is_null );
        }
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::sort_matches(
    containers::Matches::iterator _matches_begin,
    containers::Matches::iterator _matches_end )
{
    // -----------------------------------

    assert_true( needs_sorting_ );

    // -----------------------------------

    if ( std::distance( _matches_begin, _matches_end ) == 0 )
        {
            return;
        }

    auto compare_op = [this](
                          const containers::Match &match1,
                          const containers::Match &match2 ) {
        if ( match1.ix_x_popul < match2.ix_x_popul )
            {
                return true;
            }
        else if ( match1.ix_x_popul > match2.ix_x_popul )
            {
                return false;
            }
        else
            {
                return value_to_be_aggregated( &match1 ) <
                       value_to_be_aggregated( &match2 );
            }
    };

    // -----------------------------------

    std::sort( _matches_begin, _matches_end, compare_op );

    // -----------------------------------
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::reset()
{
    // --------------------------------------------------

    if constexpr ( needs_altered_samples_ )
        {
            altered_samples().clear();
        }

    // --------------------------------------------------

    if constexpr ( needs_count_ )
        {
            std::fill( count().begin(), count().end(), 0.0 );

            std::fill(
                count_committed().begin(), count_committed().end(), 0.0 );
        }

    // --------------------------------------------------

    if constexpr ( needs_sum_ )
        {
            std::fill( sum().begin(), sum().end(), 0.0 );

            std::fill( sum_committed().begin(), sum_committed().end(), 0.0 );
        }

    // --------------------------------------------------

    if constexpr ( needs_sum_cubed_ )
        {
            std::fill( sum_cubed().begin(), sum_cubed().end(), 0.0 );

            std::fill(
                sum_cubed_committed().begin(),
                sum_cubed_committed().end(),
                0.0 );
        }

    // --------------------------------------------------

    if constexpr ( needs_sum_squared_ )
        {
            std::fill( sum_squared().begin(), sum_squared().end(), 0.0 );

            std::fill(
                sum_squared_committed().begin(),
                sum_squared_committed().end(),
                0.0 );
        }

    // --------------------------------------------------

    std::fill( yhat_inline().begin(), yhat_inline().end(), 0.0 );

    std::fill( yhat_committed().begin(), yhat_committed().end(), 0.0 );

    std::fill( yhat_stored().begin(), yhat_stored().end(), 0.0 );

    // --------------------------------------------------

    updates_current().clear();

    updates_stored().clear();

    // --------------------------------------------------
}

// ----------------------------------------------------------------------------

template <
    typename AggType,
    enums::DataUsed data_used_,
    enums::Mode mode_,
    bool is_population_>
void Aggregation<AggType, data_used_, mode_, is_population_>::
    update_optimization_criterion_and_clear_updates_current(
        const Float _num_samples_smaller, const Float _num_samples_greater )
{
    optimization_criterion()->update_samples(
        updates_current(),  // _indices
        yhat_inline(),      // _new_values
        yhat_stored()       // _old_values
    );

    for ( auto ix : updates_current() )
        {
            yhat_stored()[ix] = yhat_inline()[ix];
        }

    updates_current().clear();

    optimization_criterion()->store_current_stage(
        _num_samples_smaller, _num_samples_greater );
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace multirel

#endif  // MULTIREL_AGGREGATIONS_AGGREGATION_HPP_
