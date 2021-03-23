#ifndef MULTIREL_AGGREGATIONS_FITAGGREGATION_HPP_
#define MULTIREL_AGGREGATIONS_FITAGGREGATION_HPP_

namespace multirel
{
namespace aggregations
{
// ----------------------------------------------------------------------------

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
class FitAggregation : public AbstractFitAggregation
{
   public:
    FitAggregation(
        const descriptors::SameUnitsContainer &_same_units_discrete,
        const descriptors::SameUnitsContainer &_same_units_numerical,
        const descriptors::ColumnToBeAggregated &_column_to_be_aggregated,
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const containers::Subfeatures &_subfeatures,
        const std::shared_ptr<AggregationImpl> &_aggregation_impl,
        const std::shared_ptr<optimizationcriteria::OptimizationCriterion>
            &_optimization_criterion,
        containers::Matches *_matches )
        : AbstractFitAggregation(),
          aggregation_impl_( _aggregation_impl ),
          optimization_criterion_( _optimization_criterion ),
          value_container_(
              ValueContainerCreator<data_used_, is_population_>::create(
                  _same_units_discrete,
                  _same_units_numerical,
                  _column_to_be_aggregated,
                  _population,
                  _peripheral,
                  _subfeatures ) )
    {
        assert_true( aggregation_impl_ );

        assert_true( optimization_criterion_ );

        const auto null_value_separator =
            separate_null_values_for_matches( _matches );

        if constexpr ( needs_sorting_ )
            {
                sort_matches(
                    _peripheral, null_value_separator, _matches->end() );
            }

        const auto dist =
            std::distance( _matches->begin(), null_value_separator );

        assert_true( dist >= 0 );

        samples_begin_ = _matches->data() + dist;

        samples_end_ = _matches->data() + _matches->size();
    };

    ~FitAggregation() = default;

    // --------------------------------------

    /// Activates all matches
    void activate_all(
        const bool _init_opt,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Iterates through the categories and selectively
    /// activates matches.
    void activate_matches_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) final;

    /// Iterates through the words and selectively
    /// activates matches.
    /// Used for individual words only.
    void activate_matches_containing_words(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::WordIndex &_index ) final;

    /// Iterates through the matches and activates them
    /// starting with the greatest.
    void activate_matches_from_above(
        const std::vector<size_t> &_indptr,
        const containers::MatchPtrs::const_iterator &_matches_begin,
        const containers::MatchPtrs::const_iterator &_matches_end ) final;

    /// Iterates through the matches and activates them
    /// starting with the smallest.
    void activate_matches_from_below(
        const std::vector<size_t> &_indptr,
        const containers::MatchPtrs::const_iterator &_matches_begin,
        const containers::MatchPtrs::const_iterator &_matches_end ) final;

    /// Implements a lag functionality through moving time windows.
    void activate_matches_in_window(
        const std::vector<size_t> &_indptr,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Implements a lag functionality through moving time windows.
    void activate_matches_outside_window(
        const std::vector<size_t> &_indptr,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Iterates through the categories and selectively
    /// activates matches.
    void activate_matches_not_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) final;

    /// Iterates through the words and selectively
    /// activates matches.
    /// Used for individual words only.
    void activate_matches_not_containing_words(
        const std::vector<Int>::const_iterator _words_begin,
        const std::vector<Int>::const_iterator _words_end,
        const Revert _revert,
        const containers::WordIndex &_index ) final;

    /// Activates all matches between _separator and _match_container_end.
    void activate_partition_from_above(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _separator,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Activates all matches between _match_container_begin and _separator.
    void activate_partition_from_below(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _separator,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Gets rid of data that is no longer needed.
    void clear() final;

    /// Commits the current stage of the yhats contained in
    /// updates_stored.
    void commit() final;

    /// Iterates through the categories and selectively
    /// deactivates matches.
    void deactivate_matches_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) final;

    /// Iterates through the words and selectively
    /// activates matches.
    /// Used for individual words only.
    void deactivate_matches_containing_words(
        const std::vector<Int>::const_iterator _words_begin,
        const std::vector<Int>::const_iterator _words_end,
        const Revert _revert,
        const containers::WordIndex &_index ) final;

    /// Iterates through the matches and deactivates them
    /// starting with the greatest.
    void deactivate_matches_from_above(
        const std::vector<size_t> &_indptr,
        const containers::MatchPtrs::const_iterator &_matches_begin,
        const containers::MatchPtrs::const_iterator &_matches_end ) final;

    /// Iterates through the matches and deactivates them
    /// starting with the smallest.
    void deactivate_matches_from_below(
        const std::vector<size_t> &_indptr,
        const containers::MatchPtrs::const_iterator &_matches_begin,
        const containers::MatchPtrs::const_iterator &_matches_end ) final;

    /// Implements a lag functionality through moving time windows.
    void deactivate_matches_in_window(
        const std::vector<size_t> &_indptr,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Implements a lag functionality through moving time windows.
    void deactivate_matches_outside_window(
        const std::vector<size_t> &_indptr,
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Iterates through the categories and selectively
    /// deactivates matches.
    /// Used for training.
    void deactivate_matches_not_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) final;

    /// Iterates through the words and selectively
    /// deactivates matches.
    /// Used for individual words only.
    void deactivate_matches_not_containing_words(
        const std::vector<Int>::const_iterator _words_begin,
        const std::vector<Int>::const_iterator _words_end,
        const Revert _revert,
        const containers::WordIndex &_index ) final;

    /// Deactivates all matches where the numerical_value contains null values.
    /// Such matches must always be deactivated.
    void deactivate_matches_with_null_values(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _null_values_separator ) final;

    /// Deactivates all matches between _separator and _match_container_end.
    void deactivate_partition_from_above(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _separator,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Deactivates all matches between _match_container_begin and _separator.
    void deactivate_partition_from_below(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _separator,
        containers::MatchPtrs::iterator _match_container_end ) final;

    /// Initializes optimization criterion after all matches have been
    /// activated.
    void init_optimization_criterion(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _match_container_end );

    /// Resets yhat_, yhat_committed_ and yhat_stored_ and all variables
    /// related to the aggregations with 0.0.
    void reset() final;

    /// Reinstates the status of yhat the last time commit()
    /// had been called.
    void revert_to_commit() final;

    /// Separates the pointers to matches for which the value to be aggregated
    /// is NULL
    containers::MatchPtrs::iterator separate_null_values(
        containers::MatchPtrs *_match_ptrs ) const final;

    /// Updates the optimization criterion, makes it store its
    /// current stage and clears updates_current()
    void update_optimization_criterion_and_clear_updates_current(
        const Float _num_matches_smaller,
        const Float _num_matches_greater ) final;

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
    inline void activate_match( containers::Match *_match )
    {
        assert_true( _match->ix_x_popul >= 0 );
        assert_true(
            static_cast<size_t>( _match->ix_x_popul ) < yhat_inline().size() );

        assert_true( _match->ix_x_popul < static_cast<Int>( sum().size() ) );
        assert_true( _match->ix_x_popul < static_cast<Int>( count().size() ) );

        assert_true(
            value_to_be_aggregated( _match ) ==
            value_to_be_aggregated( _match ) );

        static_assert( needs_sum_, "sum needed" );
        static_assert( needs_count_, "count needed" );

        sum()[_match->ix_x_popul] += value_to_be_aggregated( _match );

        count()[_match->ix_x_popul] += 1.0;

        assert_true( count()[_match->ix_x_popul] > 0.0 );

        yhat_inline()[_match->ix_x_popul] =
            sum()[_match->ix_x_popul] / count()[_match->ix_x_popul];
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
    inline void deactivate_match( containers::Match *_match )
    {
        assert_true( _match->ix_x_popul >= 0 );
        assert_true(
            static_cast<size_t>( _match->ix_x_popul ) < yhat_inline().size() );

        assert_true( _match->ix_x_popul < static_cast<Int>( sum().size() ) );
        assert_true( _match->ix_x_popul < static_cast<Int>( count().size() ) );

        assert_true( count()[_match->ix_x_popul] > 0.0 );

        sum()[_match->ix_x_popul] -= value_to_be_aggregated( _match );

        count()[_match->ix_x_popul] -= 1.0;

        yhat_inline()[_match->ix_x_popul] =
            ( ( count()[_match->ix_x_popul] > 0.5 )
                  ? ( sum()[_match->ix_x_popul] / count()[_match->ix_x_popul] )
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
    inline void activate_match( containers::Match *_match )
    {
        yhat_inline()[_match->ix_x_popul] += 1.0;

        assert_true( yhat_inline()[_match->ix_x_popul] > 0.0 );
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
    inline void deactivate_match( containers::Match *_match )
    {
        assert_true( yhat_inline()[_match->ix_x_popul] > 0.0 );

        yhat_inline()[_match->ix_x_popul] -= 1.0;
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
    inline void activate_match( containers::Match *_match )
    {
        assert_true( _match->activated == false );

        assert_true( yhat_inline()[_match->ix_x_popul] > -0.5 );

        static_assert( needs_altered_matches_, "altered matches needed" );

        _match->activated = true;

        altered_matches().push_back( _match );

        // We need to figure out if there is another sample
        // that has the same value as _match. If there is
        // in fact another one, we should not increase the count.

        // Note that the matches are already ordered.

        const Float val = value_to_be_aggregated( _match );

        for ( auto it = _match - 1; it >= samples_begin_; --it )
            {
                if ( value_to_be_aggregated( it ) != val ||
                     it->ix_x_popul != _match->ix_x_popul )
                    {
                        break;
                    }

                if ( it->activated )
                    {
                        return;
                    }
            }

        for ( auto it = _match + 1; it < samples_end_; ++it )
            {
                if ( value_to_be_aggregated( it ) != val ||
                     it->ix_x_popul != _match->ix_x_popul )
                    {
                        break;
                    }

                if ( it->activated )
                    {
                        return;
                    }
            }

        // No matches have been found - we can increase the count!
        yhat_inline()[_match->ix_x_popul] += 1.0;
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
    inline void deactivate_match( containers::Match *_match )
    {
        assert_true( _match->activated );

        assert_true( yhat_inline()[_match->ix_x_popul] > 0.5 );

        _match->activated = false;

        altered_matches().push_back( _match );

        // We need to figure out if there is another sample
        // that has the same value as _match. If there is
        // in fact another one, we should not decrease the count.

        // Note that the matches are already ordered.

        const Float val = value_to_be_aggregated( _match );

        for ( auto it = _match - 1; it >= samples_begin_; --it )
            {
                if ( value_to_be_aggregated( it ) != val ||
                     it->ix_x_popul != _match->ix_x_popul )
                    {
                        break;
                    }

                if ( it->activated )
                    {
                        return;
                    }
            }

        for ( auto it = _match + 1; it < samples_end_; ++it )
            {
                if ( value_to_be_aggregated( it ) != val ||
                     it->ix_x_popul != _match->ix_x_popul )
                    {
                        break;
                    }

                if ( it->activated )
                    {
                        return;
                    }
            }

        // No matches have been found - we can decrease the count!
        yhat_inline()[_match->ix_x_popul] -= 1.0;
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
    inline void activate_match( containers::Match *_match )
    {
        assert_true( _match->activated == false );

        assert_true( yhat_inline()[_match->ix_x_popul] > -0.5 );

        static_assert( needs_altered_matches_, "altered matches needed" );

        _match->activated = true;

        altered_matches().push_back( _match );

        // We need to figure out if there is another sample
        // that has the same value as _match. If there is
        // in fact another one, we should not increase the count.

        // Note that the matches are already ordered.

        const Float val = value_to_be_aggregated( _match );

        for ( auto it = _match - 1; it >= samples_begin_; --it )
            {
                if ( value_to_be_aggregated( it ) != val ||
                     it->ix_x_popul != _match->ix_x_popul )
                    {
                        break;
                    }

                if ( it->activated )
                    {
                        yhat_inline()[_match->ix_x_popul] += 1.0;
                        return;
                    }
            }

        for ( auto it = _match + 1; it < samples_end_; ++it )
            {
                if ( value_to_be_aggregated( it ) != val ||
                     it->ix_x_popul != _match->ix_x_popul )
                    {
                        break;
                    }

                if ( it->activated )
                    {
                        yhat_inline()[_match->ix_x_popul] += 1.0;
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
    inline void deactivate_match( containers::Match *_match )
    {
        assert_true( _match->activated );

        assert_true( yhat_inline()[_match->ix_x_popul] > -0.5 );

        _match->activated = false;

        altered_matches_.push_back( _match );

        // We need to figure out if there is another sample
        // that has the same value as _match. If there is
        // in fact another one, we should not decrease the count.

        // Note that the matches are already ordered.

        const Float val = value_to_be_aggregated( _match );

        for ( auto it = _match - 1; it >= samples_begin_; --it )
            {
                if ( value_to_be_aggregated( it ) != val ||
                     it->ix_x_popul != _match->ix_x_popul )
                    {
                        break;
                    }

                if ( it->activated )
                    {
                        yhat_inline()[_match->ix_x_popul] -= 1.0;
                        return;
                    }
            }

        for ( auto it = _match + 1; it < samples_end_; ++it )
            {
                if ( value_to_be_aggregated( it ) != val ||
                     it->ix_x_popul != _match->ix_x_popul )
                    {
                        break;
                    }

                if ( it->activated )
                    {
                        yhat_inline()[_match->ix_x_popul] -= 1.0;
                        return;
                    }
            }
    }

    // --------------------------------------
    // MAX or LAST aggregation - only difference is
    // how the samples are sorted

    /// MAX or LAST aggregation:
    /// Activate sample - activating a sample means that
    /// it is now included in the aggregation, but has not
    /// been included in the aggregation before.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Max>::value ||
                std::is_same<Agg, AggregationType::Last>::value,
            int>::type = 0>
    inline void activate_match( containers::Match *_match )
    {
        assert_true( _match->activated == false );

        ++( count()[_match->ix_x_popul] );

        assert_true( count()[_match->ix_x_popul] > 0.0 );

        static_assert( needs_match_ptr_, "match_ptr needed" );
        static_assert( needs_count_, "count needed" );
        static_assert( needs_altered_matches_, "altered matches needed" );

        _match->activated = true;

        altered_matches().push_back( _match );

        if ( count()[_match->ix_x_popul] < 1.5 ||
             _match > match_ptr()[_match->ix_x_popul] )
            {
                match_ptr()[_match->ix_x_popul] = _match;

                yhat_inline()[_match->ix_x_popul] =
                    value_to_be_aggregated( _match );
            }
    }

    /// MAX or LAST aggregation:
    /// Deactivate sample - deactivating a sample means that
    /// it has been activated before, but it is now no longer
    /// included in the aggregation.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Max>::value ||
                std::is_same<Agg, AggregationType::Last>::value,
            int>::type = 0>
    void deactivate_match( containers::Match *_match )
    {
        assert_true( _match->activated );

        assert_true( count()[_match->ix_x_popul] > 0.0 );

        --( count()[_match->ix_x_popul] );

        _match->activated = false;

        altered_matches_.push_back( _match );

        // If there are no activated matches left, then
        // set to zero
        if ( count()[_match->ix_x_popul] < 0.5 )
            {
                yhat_inline()[_match->ix_x_popul] = 0.0;

                return;
            }

        // If the deactivated sample was the max value, find the
        // second biggest value
        if ( _match == match_ptr()[_match->ix_x_popul] )
            {
                // The first sample that has the same ix_popul it finds
                // must be the second biggest, because matches have been sorted
                auto it = find_next_smaller( _match );

                match_ptr()[it->ix_x_popul] = it;

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
    inline void activate_match( containers::Match *_match )
    {
        assert_true( _match->activated == false );

        ++( count()[_match->ix_x_popul] );

        assert_true( count()[_match->ix_x_popul] > 0.0 );

        assert_true( _match->activated == false );

        static_assert( needs_match_ptr_, "match_ptr needed" );
        static_assert( needs_count_, "count needed" );
        static_assert( needs_altered_matches_, "altered matches needed" );

        _match->activated = true;

        altered_matches().push_back( _match );

        // If this is the only activated sample, just take
        // this as the value and return.
        if ( count()[_match->ix_x_popul] < 1.5 )
            {
                match_ptr()[_match->ix_x_popul] = _match;

                yhat_inline()[_match->ix_x_popul] =
                    value_to_be_aggregated( _match );

                return;
            }

        const auto count =
            static_cast<Int>( this->count()[_match->ix_x_popul] );

        if ( count % 2 == 0 )
            {
                // Number of activated matches is now even,)
                // used to be odd.

                auto it_greater = match_ptr()[_match->ix_x_popul];

                auto it_smaller = it_greater;

                // Because we cannot take the average of two containers::Match*,
                // we always store the GREATER one by convention when
                // there is an even number of samples.
                if ( _match > it_greater )
                    {
                        it_greater = find_next_greater( it_greater );

                        match_ptr()[_match->ix_x_popul] = it_greater;
                    }
                else
                    {
                        it_smaller = find_next_smaller( it_smaller );

                        // If _match < match_ptr()[_match->ix_x_popul],
                        // then the new pair consists of the previous sample and
                        // the new one. But, by convention,
                        // match_ptr()[_match->ix_x_popul] must point to
                        // the greater one, so
                        // match_ptr()[_match->ix_x_popul] does not
                        // change.
                    }

                yhat_inline()[_match->ix_x_popul] =
                    ( value_to_be_aggregated( it_greater ) +
                      value_to_be_aggregated( it_smaller ) ) /
                    2.0;
            }
        else
            {
                // Number of activated matches is now odd,
                // used to be even.

                auto it = match_ptr()[_match->ix_x_popul];

                if ( _match < it )
                    {
                        it = find_next_smaller( it );

                        match_ptr()[_match->ix_x_popul] = it;
                    }

                // We always store the greater containers::Match*. So when the
                // _match > match_ptr()[_match->ix_x_popul],
                // just leave match_ptr()[_match->ix_x_popul]
                // as it is.

                yhat_inline()[_match->ix_x_popul] =
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
    void deactivate_match( containers::Match *_match )
    {
        assert_true( _match->activated );

        assert_true( count()[_match->ix_x_popul] > 0.0 );

        assert_true( _match->activated );

        --( count()[_match->ix_x_popul] );

        _match->activated = false;

        altered_matches().push_back( _match );

        // If there is no sample left, set to zero
        if ( count()[_match->ix_x_popul] < 0.5 )
            {
                yhat_inline()[_match->ix_x_popul] = 0.0;

                return;
            }

        const auto count =
            static_cast<Int>( this->count()[_match->ix_x_popul] );

        if ( count % 2 == 0 )
            {
                // Number of activated matches is now even,
                // used to be odd.

                auto it_greater = match_ptr()[_match->ix_x_popul];

                auto it_smaller = it_greater;

                // Because we cannot take the average of two containers::Match*,
                // we always store the GREATER one by convention when
                // there is an even number of samples.
                if ( _match < it_greater )
                    {
                        it_greater = find_next_greater( it_greater );

                        match_ptr()[_match->ix_x_popul] = it_greater;
                    }
                else if ( _match > it_greater )
                    {
                        it_smaller = find_next_smaller( it_smaller );
                    }
                else
                    {
                        it_greater = find_next_greater( it_greater );

                        it_smaller = find_next_smaller( it_smaller );

                        match_ptr()[_match->ix_x_popul] = it_greater;
                    }

                yhat_inline()[_match->ix_x_popul] =
                    ( value_to_be_aggregated( it_greater ) +
                      value_to_be_aggregated( it_smaller ) ) /
                    2.0;
            }
        else
            {
                // Number of activated matches is now odd,
                // used to be even.

                auto it = match_ptr()[_match->ix_x_popul];

                if ( _match >= it )
                    {
                        it = find_next_smaller( it );

                        match_ptr()[_match->ix_x_popul] = it;
                    }

                // If _match < it, just leave
                // match_ptr()[_match->ix_x_popul] as it is.

                yhat_inline()[_match->ix_x_popul] =
                    value_to_be_aggregated( it );
            }
    }

    // --------------------------------------
    // MAX or LAST aggregation - only difference is
    // how the samples are sorted

    /// MIN or FIRST aggregation:
    /// Activate sample - activating a sample means that
    /// it is now included in the aggregation, but has not
    /// been included in the aggregation before.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Min>::value ||
                std::is_same<Agg, AggregationType::First>::value,
            int>::type = 0>
    inline void activate_match( containers::Match *_match )
    {
        assert_true( _match->activated == false );

        ++( count()[_match->ix_x_popul] );

        assert_true( count()[_match->ix_x_popul] > 0.0 );

        static_assert( needs_match_ptr_, "match_ptr needed" );
        static_assert( needs_count_, "count needed" );
        static_assert( needs_altered_matches_, "altered matches needed" );

        _match->activated = true;

        altered_matches().push_back( _match );

        if ( count()[_match->ix_x_popul] < 1.5 ||
             _match < match_ptr()[_match->ix_x_popul] )
            {
                match_ptr()[_match->ix_x_popul] = _match;

                yhat_inline()[_match->ix_x_popul] =
                    value_to_be_aggregated( _match );
            }
    }

    /// MIN or FIRST aggregation:
    /// Deactivate sample - deactivating a sample means that
    /// it has been activated before, but it is now no longer
    /// included in the aggregation.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Min>::value ||
                std::is_same<Agg, AggregationType::First>::value,
            int>::type = 0>
    void deactivate_match( containers::Match *_match )
    {
        assert_true( _match->activated );

        assert_true( count()[_match->ix_x_popul] > 0.0 );

        --( count()[_match->ix_x_popul] );

        _match->activated = false;

        altered_matches().push_back( _match );

        // If there are no activated matches left, then
        // set to zero
        if ( count()[_match->ix_x_popul] < 0.5 )
            {
                yhat_inline()[_match->ix_x_popul] = 0.0;

                return;
            }

        // If the deactivated sample was the min value, find the
        // second smallest value
        if ( _match == match_ptr()[_match->ix_x_popul] )
            {
                // The first sample that has the same ix_popul it finds
                // must be the second smallest, because sample have been sorted
                auto it = find_next_greater( _match );

                match_ptr()[it->ix_x_popul] = it;

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
    inline void activate_match( containers::Match *_match )
    {
        const Float val = value_to_be_aggregated( _match );

        static_assert( needs_sum_, "sum needed" );
        static_assert( needs_sum_squared_, "sum_squared needed" );
        static_assert( needs_sum_cubed_, "sum_cubed needed" );
        static_assert( needs_count_, "count needed" );

        sum()[_match->ix_x_popul] += val;

        sum_squared()[_match->ix_x_popul] += val * val;

        sum_cubed()[_match->ix_x_popul] += val * val * val;

        count()[_match->ix_x_popul] += 1.0;

        assert_true( count()[_match->ix_x_popul] > 0.0 );

        calculate_skewness( _match );
    }

    /// SKEWNESS aggregation:
    /// Calculates the skewness of all activated samples.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Skewness>::value,
            int>::type = 0>
    inline void calculate_skewness( containers::Match *_match )
    {
        if ( count()[_match->ix_x_popul] == 0.0 )
            {
                yhat_inline()[_match->ix_x_popul] = 0.0;
            }
        else
            {
                const Float mean =
                    sum()[_match->ix_x_popul] / count()[_match->ix_x_popul];

                const Float stddev = std::sqrt(
                    sum_squared()[_match->ix_x_popul] /
                        count()[_match->ix_x_popul] -
                    mean * mean );

                const Float skewness = ( ( sum_cubed()[_match->ix_x_popul] /
                                           count()[_match->ix_x_popul] ) -
                                         ( 3.0 * mean * stddev * stddev ) -
                                         ( mean * mean * mean ) ) /
                                       ( stddev * stddev * stddev );

                yhat_inline()[_match->ix_x_popul] =
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
    inline void deactivate_match( containers::Match *_match )
    {
        const Float val = value_to_be_aggregated( _match );

        sum()[_match->ix_x_popul] -= val;

        sum_squared()[_match->ix_x_popul] -= val * val;

        sum_cubed()[_match->ix_x_popul] -= val * val * val;

        assert_true( count()[_match->ix_x_popul] > 0.0 );

        count()[_match->ix_x_popul] -= 1.0;

        calculate_skewness( _match );
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
    inline void activate_match( containers::Match *_match )
    {
        const Float val = value_to_be_aggregated( _match );

        static_assert( needs_sum_, "sum needed" );
        static_assert( needs_sum_squared_, "sum_squared needed" );
        static_assert( needs_count_, "count needed" );

        sum()[_match->ix_x_popul] += val;

        sum_squared()[_match->ix_x_popul] += val * val;

        count()[_match->ix_x_popul] += 1.0;

        assert_true( count()[_match->ix_x_popul] > 0.0 );

        const Float mean =
            sum()[_match->ix_x_popul] / count()[_match->ix_x_popul];

        yhat_inline()[_match->ix_x_popul] = std::sqrt(
            sum_squared()[_match->ix_x_popul] / count()[_match->ix_x_popul] -
            mean * mean );

        yhat_inline()[_match->ix_x_popul] =
            ( yhat_inline()[_match->ix_x_popul] !=
              yhat_inline()[_match->ix_x_popul] )
                ? ( 0.0 )
                : ( yhat_inline()[_match->ix_x_popul] );
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
    inline void deactivate_match( containers::Match *_match )
    {
        const Float val = value_to_be_aggregated( _match );

        sum()[_match->ix_x_popul] -= val;

        sum_squared()[_match->ix_x_popul] -= val * val;

        assert_true( count()[_match->ix_x_popul] > 0.0 );

        count()[_match->ix_x_popul] -= 1.0;

        if ( count()[_match->ix_x_popul] == 0.0 )
            {
                yhat_inline()[_match->ix_x_popul] = 0.0;
            }
        else
            {
                const Float mean =
                    sum()[_match->ix_x_popul] / count()[_match->ix_x_popul];

                yhat_inline()[_match->ix_x_popul] = std::sqrt(
                    sum_squared()[_match->ix_x_popul] /
                        count()[_match->ix_x_popul] -
                    mean * mean );

                yhat_inline()[_match->ix_x_popul] =
                    ( yhat_inline()[_match->ix_x_popul] !=
                      yhat_inline()[_match->ix_x_popul] )
                        ? ( 0.0 )
                        : ( yhat_inline()[_match->ix_x_popul] );
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
    inline void activate_match( containers::Match *_match )
    {
        yhat_inline()[_match->ix_x_popul] += value_to_be_aggregated( _match );
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
    inline void deactivate_match( containers::Match *_match )
    {
        yhat_inline()[_match->ix_x_popul] -= value_to_be_aggregated( _match );
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
    inline void activate_match( containers::Match *_match )
    {
        const Float val = value_to_be_aggregated( _match );

        static_assert( needs_sum_, "sum needed" );
        static_assert( needs_sum_squared_, "sum_squared needed" );
        static_assert( needs_count_, "count needed" );

        sum()[_match->ix_x_popul] += val;

        sum_squared()[_match->ix_x_popul] += val * val;

        count()[_match->ix_x_popul] += 1.0;

        assert_true( count()[_match->ix_x_popul] > 0.0 );

        const Float mean =
            sum()[_match->ix_x_popul] / count()[_match->ix_x_popul];

        yhat_inline()[_match->ix_x_popul] =
            sum_squared()[_match->ix_x_popul] / count()[_match->ix_x_popul] -
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
    inline void deactivate_match( containers::Match *_match )
    {
        const Float val = value_to_be_aggregated( _match );

        sum()[_match->ix_x_popul] -= val;

        sum_squared()[_match->ix_x_popul] -= val * val;

        assert_true( count()[_match->ix_x_popul] > 0.0 );

        count()[_match->ix_x_popul] -= 1.0;

        if ( count()[_match->ix_x_popul] == 0.0 )
            {
                yhat_inline()[_match->ix_x_popul] = 0.0;
            }
        else
            {
                const Float mean =
                    sum()[_match->ix_x_popul] / count()[_match->ix_x_popul];

                yhat_inline()[_match->ix_x_popul] =
                    sum_squared()[_match->ix_x_popul] /
                        count()[_match->ix_x_popul] -
                    mean * mean;
            }
    }

    // --------------------------------------

   public:
    /// Clear all extras
    void clear_extras() final { altered_matches().clear(); }

    /// Returns a string describing the type of the aggregation
    std::string type() const final { return AggType::type(); }

    /// Returns a reference to the predictions stored by the aggregation
    std::vector<Float> &yhat() final { return aggregation_impl_->yhat_; }

    // --------------------------------------

   private:
    /// Separates the matches for which the value to be aggregated is NULL
    containers::Matches::iterator separate_null_values_for_matches(
        containers::Matches *_matches ) const;

    /// Sorts the matches by value to be aggregated (within the element in
    /// population table)
    void sort_matches(
        const containers::DataFrame &_peripheral,
        containers::Matches::iterator _matches_begin,
        containers::Matches::iterator _matches_end ) const;

    /// Sorts the matches by the time stamp (needed for FIRST and LAST).
    void sort_matches_by_ts(
        const containers::DataFrame &_peripheral,
        containers::Matches::iterator _matches_begin,
        containers::Matches::iterator _matches_end ) const;

    // --------------------------------------

   private:
    /// Trivial accessor
    std::vector<containers::Match *> &altered_matches()
    {
        return altered_matches_;
    }

    /// Trivial accessor
    inline std::vector<Float> &count() { return aggregation_impl_->count_; }

    /// Trivial accessor
    inline std::vector<Float> &count_committed()
    {
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
    inline std::vector<containers::Match *> &match_ptr()
    {
        return aggregation_impl_->match_ptr_;
    }

    /// Trivial accessor
    inline std::vector<containers::Match *> &match_ptr_committed()
    {
        return aggregation_impl_->match_ptr_committed_;
    }

    /// Trivial accessor.
    inline optimizationcriteria::OptimizationCriterion *optimization_criterion()
    {
        return optimization_criterion_.get();
    }

    /// Trivial accessor
    inline std::vector<Float> &sum() { return aggregation_impl_->sum_; }

    /// Trivial accessor
    inline std::vector<Float> &sum_committed()
    {
        return aggregation_impl_->sum_committed_;
    }

    /// Trivial accessor
    inline std::vector<Float> &sum_cubed()
    {
        return aggregation_impl_->sum_cubed_;
    }

    /// Trivial accessor
    inline std::vector<Float> &sum_cubed_committed()
    {
        return aggregation_impl_->sum_cubed_committed_;
    }

    /// Trivial accessor
    inline std::vector<Float> &sum_squared()
    {
        return aggregation_impl_->sum_squared_;
    }

    /// Trivial accessor
    inline std::vector<Float> &sum_squared_committed()
    {
        return aggregation_impl_->sum_squared_committed_;
    }

    /// Trivial accessor
    inline containers::IntSet &updates_current()
    {
        return aggregation_impl_->updates_current_;
    }

    /// Trivial accessor
    inline containers::IntSet &updates_stored()
    {
        return aggregation_impl_->updates_stored_;
    }

    /// Returns the value to be aggregated for this particular match.
    inline Float value_to_be_aggregated( const containers::Match *_match ) const
    {
        using ReturnType =
            ValueContainer<data_used_, is_population_>::ReturnType;

        if constexpr ( std::is_same<ReturnType, Float>() )
            {
                return value_container_.value_to_be_aggregated( _match );
            }

        if constexpr ( !std::is_same<ReturnType, Float>() )
            {
                static_assert(
                    std::is_same<AggType, AggregationType::Count>(),
                    "Only Count does not return!" );
                return 0.0;
            }
    }

    /// Accessor for the value to be compared - this is needed for the
    /// time_stamps and the same_units.
    inline std::vector<Float> &yhat_committed()
    {
        return aggregation_impl_->yhat_committed_;
    }

    /// Trivial accessor
    inline std::vector<Float> &yhat_inline()
    {
        return aggregation_impl_->yhat_;
    }

    /// Trivial accessor
    inline std::vector<Float> &yhat_stored()
    {
        return aggregation_impl_->yhat_stored_;
    }

    // --------------------------------------

   private:
    /// Whether this is the FIRST or LAST aggregation.
    constexpr static bool is_first_or_last_ =
        std::is_same<AggType, AggregationType::First>() ||
        std::is_same<AggType, AggregationType::Last>();

    /// Whether the aggregation requires recording which matches have been
    /// altered.
    constexpr static bool needs_altered_matches_ =
        std::is_same<AggType, AggregationType::CountDistinct>() ||
        std::is_same<AggType, AggregationType::CountMinusCountDistinct>() ||
        std::is_same<AggType, AggregationType::First>() ||
        std::is_same<AggType, AggregationType::Last>() ||
        std::is_same<AggType, AggregationType::Max>() ||
        std::is_same<AggType, AggregationType::Median>() ||
        std::is_same<AggType, AggregationType::Min>();

    /// Whether the aggregation relies on count()
    constexpr static bool needs_count_ =
        std::is_same<AggType, AggregationType::Avg>() ||
        std::is_same<AggType, AggregationType::First>() ||
        std::is_same<AggType, AggregationType::Last>() ||
        std::is_same<AggType, AggregationType::Max>() ||
        std::is_same<AggType, AggregationType::Median>() ||
        std::is_same<AggType, AggregationType::Min>() ||
        std::is_same<AggType, AggregationType::Skewness>() ||
        std::is_same<AggType, AggregationType::Stddev>() ||
        std::is_same<AggType, AggregationType::Var>();

    /// Whether the aggregation relies on sum()
    constexpr static bool needs_match_ptr_ =
        std::is_same<AggType, AggregationType::First>() ||
        std::is_same<AggType, AggregationType::Last>() ||
        std::is_same<AggType, AggregationType::Max>() ||
        std::is_same<AggType, AggregationType::Median>() ||
        std::is_same<AggType, AggregationType::Min>();

    /// Whether the aggregation requires sorting.
    constexpr static bool needs_sorting_ =
        std::is_same<AggType, AggregationType::CountDistinct>() ||
        std::is_same<AggType, AggregationType::CountMinusCountDistinct>() ||
        std::is_same<AggType, AggregationType::First>() ||
        std::is_same<AggType, AggregationType::Last>() ||
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

    // --------------------------------------

   private:
    /// Pimpl for aggregation
    const std::shared_ptr<AggregationImpl> aggregation_impl_;

    /// Contains pointers to matches that have been changed
    /// since the last commit
    std::vector<containers::Match *> altered_matches_;

    /// Pointer to the optimization criterion used
    const std::shared_ptr<optimizationcriteria::OptimizationCriterion>
        optimization_criterion_;

    /// Pointer to the first element in matches - some aggregations
    /// like min and max need to know this
    containers::Match *samples_begin_;

    /// Pointer to the element behind the last element in matches -
    /// some aggregations like min and max need to know this
    containers::Match *samples_end_;

    /// Denotes whether the updates since the last commit had
    /// been activated or deactivated. revert_to_commit() needs this
    /// piece of information.
    bool updates_have_been_activated_;

    /// Contains the value to be aggregated.
    ValueContainer<data_used_, is_population_> value_container_;
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::activate_all(
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
            activate_match( *it );
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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    activate_matches_containing_categories(
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
                    activate_match( *it );

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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    activate_matches_containing_words(
        const std::vector<Int>::const_iterator _words_begin,
        const std::vector<Int>::const_iterator _words_end,
        const Revert _revert,
        const containers::WordIndex &_index )
{
    // ------------------------------------------------------------------

    assert_true( _revert != Revert::after_all_categories );

    // ------------------------------------------------------------------

    containers::MatchPtrs matches;

    // ------------------------------------------------------------------

    Float num_samples_smaller = 0.0;

    const auto sample_size =
        static_cast<Float>( std::distance( _index.begin(), _index.end() ) );

    // ------------------------------------------------------------------

    for ( auto word = _words_begin; word < _words_end; ++word )
        {
            _index.range( *word, &matches );

            for ( const auto m : matches )
                {
                    activate_match( m );

                    updates_stored().insert( m->ix_x_popul );
                    updates_current().insert( m->ix_x_popul );

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

    if ( _revert == Revert::not_at_all )
        {
            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, sample_size - num_samples_smaller );
        }
}

// ----------------------------------------------------------------------------

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    activate_matches_from_above(
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
                    activate_match( *it );

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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    activate_matches_from_below(
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
                    activate_match( *it );

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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    activate_matches_in_window(
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
                    activate_match( *it );

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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    activate_matches_outside_window(
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
            activate_match( *it );

            updates_stored().insert( ( *it )->ix_x_popul );
            updates_current().insert( ( *it )->ix_x_popul );
        }

    // ------------------------------------------------------------------
    // Selectively deactivate those matches that are inside the window.

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
                    deactivate_match( *it );

                    updates_current().insert( ( *it )->ix_x_popul );

                    ++num_samples_smaller;
                }

            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, sample_size - num_samples_smaller );

            for ( auto it = begin; it != end; ++it )
                {
                    activate_match( *it );

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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    activate_matches_not_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index )
{
    // ------------------------------------------------------------------
    // Activate all samples

    for ( auto it = _index.begin(); it != _index.end(); ++it )
        {
            activate_match( *it );

            updates_stored().insert( ( *it )->ix_x_popul );
            updates_current().insert( ( *it )->ix_x_popul );
        }

    // ------------------------------------------------------------------
    // Selectively deactivate those matches that are not of the
    // particular category

    Float num_samples_smaller = 0.0;

    auto sample_size =
        static_cast<Float>( std::distance( _index.begin(), _index.end() ) );

    for ( auto cat = _categories_begin; cat < _categories_end; ++cat )
        {
            for ( auto it = _index.begin( *cat ); it < _index.end( *cat );
                  ++it )
                {
                    deactivate_match( *it );

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
                            activate_match( *it );

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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    activate_matches_not_containing_words(
        const std::vector<Int>::const_iterator _words_begin,
        const std::vector<Int>::const_iterator _words_end,
        const Revert _revert,
        const containers::WordIndex &_index )
{
    // ------------------------------------------------------------------

    assert_true( _revert != Revert::after_all_categories );

    // ------------------------------------------------------------------
    // Activate all matches

    for ( const auto m : _index )
        {
            activate_match( m );

            updates_stored().insert( m->ix_x_popul );
            updates_current().insert( m->ix_x_popul );
        }

    // ------------------------------------------------------------------

    containers::MatchPtrs matches;

    // ------------------------------------------------------------------
    // Selectively deactivate those matches that are not of the
    // particular category

    Float num_samples_smaller = 0.0;

    auto sample_size =
        static_cast<Float>( std::distance( _index.begin(), _index.end() ) );

    for ( auto word = _words_begin; word < _words_end; ++word )
        {
            _index.range( *word, &matches );

            for ( const auto m : matches )
                {
                    deactivate_match( m );

                    updates_current().insert( m->ix_x_popul );

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
                    for ( const auto m : matches )
                        {
                            activate_match( m );

                            updates_current().insert( m->ix_x_popul );
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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    activate_partition_from_above(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _separator,
        containers::MatchPtrs::iterator _match_container_end )
{
    assert_true( _separator >= _match_container_begin );

    assert_true( _match_container_end >= _separator );

    for ( auto it = _separator; it != _match_container_end; ++it )
        {
            activate_match( *it );

            updates_stored().insert( ( *it )->ix_x_popul );
            updates_current().insert( ( *it )->ix_x_popul );
        }

    const auto num_samples_smaller = static_cast<Float>(
        std::distance( _match_container_begin, _separator ) );

    const auto num_samples_greater =
        static_cast<Float>( std::distance( _separator, _match_container_end ) );

    update_optimization_criterion_and_clear_updates_current(
        num_samples_smaller, num_samples_greater );
}

// ----------------------------------------------------------------------------

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    activate_partition_from_below(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _separator,
        containers::MatchPtrs::iterator _match_container_end )
{
    assert_true( _separator >= _match_container_begin );

    assert_true( _match_container_end >= _separator );

    for ( auto it = _match_container_begin; it != _separator; ++it )
        {
            activate_match( *it );

            updates_stored().insert( ( *it )->ix_x_popul );
            updates_current().insert( ( *it )->ix_x_popul );
        }

    const auto num_samples_smaller = static_cast<Float>(
        std::distance( _match_container_begin, _separator ) );

    const auto num_samples_greater =
        static_cast<Float>( std::distance( _separator, _match_container_end ) );

    update_optimization_criterion_and_clear_updates_current(
        num_samples_smaller, num_samples_greater );
}

// ----------------------------------------------------------------------------

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::clear()
{
    altered_matches().clear();

    updates_current().clear();

    updates_stored().clear();
}

// ----------------------------------------------------------------------------

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::commit()
{
    // --------------------------------------------------

    if constexpr ( needs_altered_matches_ )
        {
            altered_matches().clear();
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

    if constexpr ( needs_match_ptr_ )
        {
            for ( auto i : updates_stored() )
                {
                    match_ptr_committed()[i] = match_ptr()[i];
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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    deactivate_matches_containing_categories(
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
                    deactivate_match( *it );

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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    deactivate_matches_containing_words(
        const std::vector<Int>::const_iterator _words_begin,
        const std::vector<Int>::const_iterator _words_end,
        const Revert _revert,
        const containers::WordIndex &_index )
{
    // ------------------------------------------------------------------

    assert_true( _revert != Revert::after_all_categories );

    // ------------------------------------------------------------------

    containers::MatchPtrs matches;

    // ------------------------------------------------------------------

    Float num_samples_smaller = 0.0;

    auto sample_size =
        static_cast<Float>( std::distance( _index.begin(), _index.end() ) );

    // ------------------------------------------------------------------

    for ( auto word = _words_begin; word < _words_end; ++word )
        {
            _index.range( *word, &matches );

            for ( const auto m : matches )
                {
                    deactivate_match( m );

                    updates_stored().insert( m->ix_x_popul );
                    updates_current().insert( m->ix_x_popul );

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

    if ( _revert == Revert::not_at_all )
        {
            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, sample_size - num_samples_smaller );
        }

    // ------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    deactivate_matches_from_above(
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
                    deactivate_match( *it );

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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    deactivate_matches_from_below(
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
                    deactivate_match( *it );

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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    deactivate_matches_in_window(
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
                    deactivate_match( *it );

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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    deactivate_matches_outside_window(
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
            deactivate_match( *it );

            updates_stored().insert( ( *it )->ix_x_popul );
            updates_current().insert( ( *it )->ix_x_popul );
        }

    // ------------------------------------------------------------------
    // Selectively activate those matches that are inside the window.

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
                    activate_match( *it );

                    updates_current().insert( ( *it )->ix_x_popul );

                    ++num_samples_smaller;
                }

            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, sample_size - num_samples_smaller );

            for ( auto it = begin; it != end; ++it )
                {
                    deactivate_match( *it );

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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    deactivate_matches_not_containing_categories(
        const std::vector<Int>::const_iterator _categories_begin,
        const std::vector<Int>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index )
{
    // ------------------------------------------------------------------
    // Deactivate all samples

    for ( auto it = _index.begin(); it != _index.end(); ++it )
        {
            deactivate_match( *it );

            updates_stored().insert( ( *it )->ix_x_popul );
            updates_current().insert( ( *it )->ix_x_popul );
        }

    // ------------------------------------------------------------------
    // Selectively activate those matches that are not of the
    // particular category

    const auto sample_size =
        static_cast<Float>( std::distance( _index.begin(), _index.end() ) );

    Float num_samples_smaller = 0.0;

    for ( auto cat = _categories_begin; cat < _categories_end; ++cat )
        {
            for ( auto it = _index.begin( *cat ); it < _index.end( *cat );
                  ++it )
                {
                    activate_match( *it );

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
                            deactivate_match( *it );

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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    deactivate_matches_not_containing_words(
        const std::vector<Int>::const_iterator _words_begin,
        const std::vector<Int>::const_iterator _words_end,
        const Revert _revert,
        const containers::WordIndex &_index )
{
    // ------------------------------------------------------------------

    assert_true( _revert != Revert::after_all_categories );

    // ------------------------------------------------------------------
    // Deactivate all matches

    for ( const auto m : _index )
        {
            deactivate_match( m );

            updates_stored().insert( m->ix_x_popul );
            updates_current().insert( m->ix_x_popul );
        }

    // ------------------------------------------------------------------

    containers::MatchPtrs matches;

    // ------------------------------------------------------------------
    // Selectively activate those matches that are not of the
    // particular category

    const auto sample_size =
        static_cast<Float>( std::distance( _index.begin(), _index.end() ) );

    Float num_samples_smaller = 0.0;

    for ( auto word = _words_begin; word < _words_end; ++word )
        {
            _index.range( *word, &matches );

            for ( const auto m : matches )
                {
                    activate_match( m );

                    updates_current().insert( m->ix_x_popul );

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
                    for ( const auto m : matches )
                        {
                            deactivate_match( m );

                            updates_current().insert( m->ix_x_popul );
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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    deactivate_matches_with_null_values(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _null_values_separator )
{
    assert_true( _null_values_separator >= _match_container_begin );

    for ( auto it = _match_container_begin; it != _null_values_separator; ++it )
        {
            deactivate_match( *it );

            updates_stored().insert( ( *it )->ix_x_popul );
            updates_current().insert( ( *it )->ix_x_popul );
        }
}

// ----------------------------------------------------------------------------

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    deactivate_partition_from_above(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _separator,
        containers::MatchPtrs::iterator _match_container_end )
{
    assert_true( _separator >= _match_container_begin );

    assert_true( _match_container_end >= _separator );

    for ( auto it = _separator; it != _match_container_end; ++it )
        {
            deactivate_match( *it );

            updates_stored().insert( ( *it )->ix_x_popul );
            updates_current().insert( ( *it )->ix_x_popul );
        }

    const auto num_samples_smaller = static_cast<Float>(
        std::distance( _match_container_begin, _separator ) );

    const auto num_samples_greater =
        static_cast<Float>( std::distance( _separator, _match_container_end ) );

    update_optimization_criterion_and_clear_updates_current(
        num_samples_smaller, num_samples_greater );
}

// ----------------------------------------------------------------------------

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
    deactivate_partition_from_below(
        containers::MatchPtrs::iterator _match_container_begin,
        containers::MatchPtrs::iterator _separator,
        containers::MatchPtrs::iterator _match_container_end )
{
    assert_true( _separator >= _match_container_begin );

    assert_true( _match_container_end >= _separator );

    for ( auto it = _match_container_begin; it != _separator; ++it )
        {
            deactivate_match( *it );

            updates_stored().insert( ( *it )->ix_x_popul );
            updates_current().insert( ( *it )->ix_x_popul );
        }

    const auto num_samples_smaller = static_cast<Float>(
        std::distance( _match_container_begin, _separator ) );

    const auto num_samples_greater =
        static_cast<Float>( std::distance( _separator, _match_container_end ) );

    update_optimization_criterion_and_clear_updates_current(
        num_samples_smaller, num_samples_greater );
}

// ----------------------------------------------------------------------------

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::revert_to_commit()
{
    // --------------------------------------------------

    if constexpr ( needs_altered_matches_ )
        {
            for ( containers::Match *sample : altered_matches() )
                {
                    sample->activated = !( sample->activated );
                }

            altered_matches().clear();
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

    if constexpr ( needs_match_ptr_ )
        {
            for ( auto i : updates_stored() )
                {
                    match_ptr()[i] = match_ptr_committed()[i];
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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
containers::Matches::iterator
FitAggregation<AggType, data_used_, is_population_>::
    separate_null_values_for_matches( containers::Matches *_matches ) const
{
    auto is_null = [this]( containers::Match &sample ) {
        const auto val = value_to_be_aggregated( &sample );
        return ( std::isnan( val ) || std::isinf( val ) );
    };

    return std::stable_partition( _matches->begin(), _matches->end(), is_null );
}

// ----------------------------------------------------------------------------

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
containers::MatchPtrs::iterator
FitAggregation<AggType, data_used_, is_population_>::separate_null_values(
    containers::MatchPtrs *_match_ptrs ) const
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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::sort_matches(
    const containers::DataFrame &_peripheral,
    containers::Matches::iterator _matches_begin,
    containers::Matches::iterator _matches_end ) const
{
    // -----------------------------------

    if ( std::distance( _matches_begin, _matches_end ) == 0 )
        {
            return;
        }

    // -----------------------------------

    if constexpr ( is_first_or_last_ )
        {
            sort_matches_by_ts( _peripheral, _matches_begin, _matches_end );
            return;
        }

    // -----------------------------------

    const auto compare_op = [this](
                                const containers::Match &match1,
                                const containers::Match &match2 ) -> bool {
        if ( match1.ix_x_popul < match2.ix_x_popul )
            {
                return true;
            }

        if ( match1.ix_x_popul > match2.ix_x_popul )
            {
                return false;
            }

        return value_to_be_aggregated( &match1 ) <
               value_to_be_aggregated( &match2 );
    };

    // -----------------------------------

    std::sort( _matches_begin, _matches_end, compare_op );

    // -----------------------------------
}

// ----------------------------------------------------------------------------

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::sort_matches_by_ts(
    const containers::DataFrame &_peripheral,
    containers::Matches::iterator _matches_begin,
    containers::Matches::iterator _matches_end ) const
{
    // -----------------------------------

    assert_true( is_first_or_last_ );

    // -----------------------------------

    assert_true( _peripheral.num_time_stamps() > 0 );

    const auto &ts_col = _peripheral.time_stamp_col();

    const auto retrieve_ts =
        [&ts_col]( const containers::Match &match ) -> Float {
        assert_true( match.ix_x_perip < ts_col.nrows_ );
        return ts_col[match.ix_x_perip];
    };

    // -----------------------------------

    const auto compare_op = [retrieve_ts](
                                const containers::Match &match1,
                                const containers::Match &match2 ) -> bool {
        if ( match1.ix_x_popul < match2.ix_x_popul )
            {
                return true;
            }

        if ( match1.ix_x_popul > match2.ix_x_popul )
            {
                return false;
            }

        return retrieve_ts( match1 ) < retrieve_ts( match2 );
    };

    // -----------------------------------

    std::sort( _matches_begin, _matches_end, compare_op );

    // -----------------------------------
}

// ----------------------------------------------------------------------------

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::reset()
{
    // --------------------------------------------------

    if constexpr ( needs_altered_matches_ )
        {
            altered_matches().clear();
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

template <typename AggType, enums::DataUsed data_used_, bool is_population_>
void FitAggregation<AggType, data_used_, is_population_>::
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
