#ifndef AUTOSQL_AGGREGATIONS_AGGREGATION_HPP_
#define AUTOSQL_AGGREGATIONS_AGGREGATION_HPP_

namespace autosql
{
namespace aggregations
{
// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
class Aggregation : public AggregationBase
{
   public:
    Aggregation() : AggregationBase(), aggregation_impl_( nullptr ){};

    ~Aggregation() = default;

    // --------------------------------------

    /// Activates all samples
    void activate_all(
        const bool _init_opt,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end ) final;

    /// Activates all samples that contain any category between
    /// _categories_begin and _categories_end. Used for prediction.
    void activate_samples_containing_categories(
        const std::vector<AUTOSQL_INT>::const_iterator _categories_begin,
        const std::vector<AUTOSQL_INT>::const_iterator _categories_end,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end ) final;

    /// Iterates through the categories and selectively
    /// activates samples.
    /// Used for training.
    void activate_samples_containing_categories(
        const std::vector<AUTOSQL_INT>::const_iterator _categories_begin,
        const std::vector<AUTOSQL_INT>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) final;

    /// Activates all samples that do not contain any category between
    /// _categories_begin and _categories_end. Used for prediction.
    void activate_samples_not_containing_categories(
        const std::vector<AUTOSQL_INT>::const_iterator _categories_begin,
        const std::vector<AUTOSQL_INT>::const_iterator _categories_end,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end ) final;

    /// Iterates through the categories and selectively
    /// activates samples.
    /// Used for training.
    void activate_samples_not_containing_categories(
        const std::vector<AUTOSQL_INT>::const_iterator _categories_begin,
        const std::vector<AUTOSQL_INT>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) final;

    /// Iterates through the samples and activates those.
    /// samples that are greater than the critical value.
    void activate_samples_from_above(
        const AUTOSQL_FLOAT _critical_value,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end ) final;

    /// Iterates through the samples and activates them
    /// starting with the greatest.
    void activate_samples_from_above(
        const containers::Matrix<AUTOSQL_FLOAT> &_critical_values,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end ) final;

    /// Iterates through the samples and activates those
    /// samples that smaller than or equal to the critical value.
    void activate_samples_from_below(
        const AUTOSQL_FLOAT _critical_value,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end ) final;

    /// Iterates through the samples and activates them
    /// starting with the smallest.
    void activate_samples_from_below(
        const containers::Matrix<AUTOSQL_FLOAT> &_critical_values,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end ) final;

    /// Gets rid of data that is no longer needed.
    void clear() final;

    /// Commits the current stage of the yhats contained in
    /// updates_stored.
    void commit() final;

    /// Deactivates all samples that contain any category between
    /// _categories_begin and _categories_end. Used for prediction.
    void deactivate_samples_containing_categories(
        const std::vector<AUTOSQL_INT>::const_iterator _categories_begin,
        const std::vector<AUTOSQL_INT>::const_iterator _categories_end,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end ) final;

    /// Iteratres through the categories and selectively
    /// deactivates samples.
    /// Used for training.
    void deactivate_samples_containing_categories(
        const std::vector<AUTOSQL_INT>::const_iterator _categories_begin,
        const std::vector<AUTOSQL_INT>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) final;

    /// Iterates through the samples and deactivates those
    /// samples that are greater than the critical value.
    void deactivate_samples_from_above(
        const AUTOSQL_FLOAT _critical_value,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end ) final;

    /// Iterates through the samples and deactivates them
    /// starting with the greatest.
    void deactivate_samples_from_above(
        const containers::Matrix<AUTOSQL_FLOAT> &_critical_values,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end ) final;

    /// Iterates through the samples and deactivates those
    /// samples that smaller than or equal to the critical value.
    void deactivate_samples_from_below(
        const AUTOSQL_FLOAT _critical_value,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end ) final;

    /// Iterates through the samples and deactivates them
    /// starting with the smallest.
    void deactivate_samples_from_below(
        const containers::Matrix<AUTOSQL_FLOAT> &_critical_values,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end ) final;

    /// Deactivates all samples that do not contain any category between
    /// _categories_begin and _categories_end. Used for prediction.
    void deactivate_samples_not_containing_categories(
        const std::vector<AUTOSQL_INT>::const_iterator _categories_begin,
        const std::vector<AUTOSQL_INT>::const_iterator _categories_end,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end ) final;

    /// Iterates through the categories and selectively
    /// deactivates samples.
    /// Used for training.
    void deactivate_samples_not_containing_categories(
        const std::vector<AUTOSQL_INT>::const_iterator _categories_begin,
        const std::vector<AUTOSQL_INT>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index ) final;

    /// Deactivates all samples where the numerical_value contains null values.
    /// Such samples must always be deactivated.
    void deactivate_samples_with_null_values(
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _null_values_separator ) final;

    /// Initializes optimization criterion after all samples have been
    /// activated.
    void init_optimization_criterion(
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end );

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
    AUTOSQL_SAMPLES::iterator separate_null_values( AUTOSQL_SAMPLES &_samples );

    /// Separates the pointers to samples for which the value to be aggregated
    /// is NULL
    AUTOSQL_SAMPLE_CONTAINER::iterator separate_null_values(
        AUTOSQL_SAMPLE_CONTAINER &_samples );

    /// Sorts the samples by value to be aggregated (within the element in
    /// population table)
    void sort_samples(
        AUTOSQL_SAMPLES::iterator _samples_begin,
        AUTOSQL_SAMPLES::iterator _samples_end );

    /// Updates the optimization criterion, makes it store its
    /// current stage and clears updates_current()
    void update_optimization_criterion_and_clear_updates_current(
        const AUTOSQL_FLOAT _num_samples_smaller,
        const AUTOSQL_FLOAT _num_samples_greater ) final;

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
    inline void activate_sample( Sample *_sample )
    {
        assert( _sample->ix_x_popul >= 0 );
        assert( _sample->ix_x_popul < yhat_inline().nrows() );

        assert( _sample->ix_x_popul < static_cast<AUTOSQL_INT>( sum().size() ) );
        assert(
            _sample->ix_x_popul < static_cast<AUTOSQL_INT>( count().size() ) );

        assert(
            value_to_be_aggregated( _sample ) ==
            value_to_be_aggregated( _sample ) );

        static_assert( needs_sum_, "sum needed" );
        static_assert( needs_count_, "count needed" );

        sum()[_sample->ix_x_popul] += value_to_be_aggregated( _sample );

        count()[_sample->ix_x_popul] += 1.0;

        assert( count()[_sample->ix_x_popul] > 0.0 );

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
    inline void deactivate_sample( Sample *_sample )
    {
        assert( _sample->ix_x_popul >= 0 );
        assert( _sample->ix_x_popul < yhat_inline().nrows() );

        assert( _sample->ix_x_popul < static_cast<AUTOSQL_INT>( sum().size() ) );
        assert(
            _sample->ix_x_popul < static_cast<AUTOSQL_INT>( count().size() ) );

        assert( count()[_sample->ix_x_popul] > 0.0 );

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
    inline void activate_sample( Sample *_sample )
    {
        yhat_inline()[_sample->ix_x_popul] += 1.0;

        assert( yhat_inline()[_sample->ix_x_popul] > 0.0 );
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
    inline void deactivate_sample( Sample *_sample )
    {
        assert( yhat_inline()[_sample->ix_x_popul] > 0.0 );

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
    inline void activate_sample( Sample *_sample )
    {
        assert( _sample->activated == false );

        assert( yhat_inline()[_sample->ix_x_popul] > -0.5 );

        static_assert( needs_altered_samples_, "altered samples needed" );

        _sample->activated = true;

        altered_samples().push_back( _sample );

        // We need to figure out if there is another sample
        // that has the same value as _sample. If there is
        // in fact another one, we should not increase the count.

        // Note that the samples are already ordered.

        const AUTOSQL_FLOAT val = value_to_be_aggregated( _sample );

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
    inline void deactivate_sample( Sample *_sample )
    {
        assert( _sample->activated );

        assert( yhat_inline()[_sample->ix_x_popul] > 0.5 );

        _sample->activated = false;

        altered_samples().push_back( _sample );

        // We need to figure out if there is another sample
        // that has the same value as _sample. If there is
        // in fact another one, we should not decrease the count.

        // Note that the samples are already ordered.

        const AUTOSQL_FLOAT val = value_to_be_aggregated( _sample );

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
    inline void activate_sample( Sample *_sample )
    {
        assert( _sample->activated == false );

        assert( yhat_inline()[_sample->ix_x_popul] > -0.5 );

        static_assert( needs_altered_samples_, "altered samples needed" );

        _sample->activated = true;

        altered_samples().push_back( _sample );

        // We need to figure out if there is another sample
        // that has the same value as _sample. If there is
        // in fact another one, we should not increase the count.

        // Note that the samples are already ordered.

        const AUTOSQL_FLOAT val = value_to_be_aggregated( _sample );

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
    inline void deactivate_sample( Sample *_sample )
    {
        assert( _sample->activated );

        assert( yhat_inline()[_sample->ix_x_popul] > -0.5 );

        _sample->activated = false;

        altered_samples_.push_back( _sample );

        // We need to figure out if there is another sample
        // that has the same value as _sample. If there is
        // in fact another one, we should not decrease the count.

        // Note that the samples are already ordered.

        const AUTOSQL_FLOAT val = value_to_be_aggregated( _sample );

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
    inline void activate_sample( Sample *_sample )
    {
        assert( _sample->activated == false );

        ++( count()[_sample->ix_x_popul] );

        assert( count()[_sample->ix_x_popul] > 0.0 );

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
    void deactivate_sample( Sample *_sample )
    {
        assert( _sample->activated );

        assert( count()[_sample->ix_x_popul] > 0.0 );

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
    inline void activate_sample( Sample *_sample )
    {
        assert( _sample->activated == false );

        ++( count()[_sample->ix_x_popul] );

        assert( count()[_sample->ix_x_popul] > 0.0 );

        assert( _sample->activated == false );

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
            static_cast<AUTOSQL_INT>( this->count()[_sample->ix_x_popul] );

        if ( count % 2 == 0 )
            {
                // Number of activated samples is now even,)
                // used to be odd.

                auto it_greater = sample_ptr()[_sample->ix_x_popul];

                auto it_smaller = it_greater;

                // Because we cannot take the average of two Sample*,
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

                // We always store the greater Sample*. So when the
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
    void deactivate_sample( Sample *_sample )
    {
        assert( _sample->activated );

        assert( count()[_sample->ix_x_popul] > 0.0 );

        assert( _sample->activated );

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
            static_cast<AUTOSQL_INT>( this->count()[_sample->ix_x_popul] );

        if ( count % 2 == 0 )
            {
                // Number of activated samples is now even,
                // used to be odd.

                auto it_greater = sample_ptr()[_sample->ix_x_popul];

                auto it_smaller = it_greater;

                // Because we cannot take the average of two Sample*,
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
    inline void activate_sample( Sample *_sample )
    {
        assert( _sample->activated == false );

        ++( count()[_sample->ix_x_popul] );

        assert( count()[_sample->ix_x_popul] > 0.0 );

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
    void deactivate_sample( Sample *_sample )
    {
        assert( _sample->activated );

        assert( count()[_sample->ix_x_popul] > 0.0 );

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
    inline void activate_sample( Sample *_sample )
    {
        const AUTOSQL_FLOAT val = value_to_be_aggregated( _sample );

        static_assert( needs_sum_, "sum needed" );
        static_assert( needs_sum_squared_, "sum_squared needed" );
        static_assert( needs_sum_cubed_, "sum_cubed needed" );
        static_assert( needs_count_, "count needed" );

        sum()[_sample->ix_x_popul] += val;

        sum_squared()[_sample->ix_x_popul] += val * val;

        sum_cubed()[_sample->ix_x_popul] += val * val * val;

        count()[_sample->ix_x_popul] += 1.0;

        assert( count()[_sample->ix_x_popul] > 0.0 );

        calculate_skewness( _sample );
    }

    /// SKEWNESS aggregation:
    /// Calculates the skewness of all activated samples.
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Skewness>::value,
            int>::type = 0>
    inline void calculate_skewness( Sample *_sample )
    {
        if ( count()[_sample->ix_x_popul] == 0.0 )
            {
                yhat_inline()[_sample->ix_x_popul] = 0.0;
            }
        else
            {
                const AUTOSQL_FLOAT mean =
                    sum()[_sample->ix_x_popul] / count()[_sample->ix_x_popul];

                const AUTOSQL_FLOAT stddev = std::sqrt(
                    sum_squared()[_sample->ix_x_popul] /
                        count()[_sample->ix_x_popul] -
                    mean * mean );

                const AUTOSQL_FLOAT skewness =
                    ( ( sum_cubed()[_sample->ix_x_popul] /
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
    inline void deactivate_sample( Sample *_sample )
    {
        const AUTOSQL_FLOAT val = value_to_be_aggregated( _sample );

        sum()[_sample->ix_x_popul] -= val;

        sum_squared()[_sample->ix_x_popul] -= val * val;

        sum_cubed()[_sample->ix_x_popul] -= val * val * val;

        assert( count()[_sample->ix_x_popul] > 0.0 );

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
    inline void activate_sample( Sample *_sample )
    {
        const AUTOSQL_FLOAT val = value_to_be_aggregated( _sample );

        static_assert( needs_sum_, "sum needed" );
        static_assert( needs_sum_squared_, "sum_squared needed" );
        static_assert( needs_count_, "count needed" );

        sum()[_sample->ix_x_popul] += val;

        sum_squared()[_sample->ix_x_popul] += val * val;

        count()[_sample->ix_x_popul] += 1.0;

        assert( count()[_sample->ix_x_popul] > 0.0 );

        const AUTOSQL_FLOAT mean =
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
    inline void deactivate_sample( Sample *_sample )
    {
        const AUTOSQL_FLOAT val = value_to_be_aggregated( _sample );

        sum()[_sample->ix_x_popul] -= val;

        sum_squared()[_sample->ix_x_popul] -= val * val;

        assert( count()[_sample->ix_x_popul] > 0.0 );

        count()[_sample->ix_x_popul] -= 1.0;

        if ( count()[_sample->ix_x_popul] == 0.0 )
            {
                yhat_inline()[_sample->ix_x_popul] = 0.0;
            }
        else
            {
                const AUTOSQL_FLOAT mean =
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
    inline void activate_sample( Sample *_sample )
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
    inline void deactivate_sample( Sample *_sample )
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
    inline void activate_sample( Sample *_sample )
    {
        const AUTOSQL_FLOAT val = value_to_be_aggregated( _sample );

        static_assert( needs_sum_, "sum needed" );
        static_assert( needs_sum_squared_, "sum_squared needed" );
        static_assert( needs_count_, "count needed" );

        sum()[_sample->ix_x_popul] += val;

        sum_squared()[_sample->ix_x_popul] += val * val;

        count()[_sample->ix_x_popul] += 1.0;

        assert( count()[_sample->ix_x_popul] > 0.0 );

        const AUTOSQL_FLOAT mean =
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
    inline void deactivate_sample( Sample *_sample )
    {
        const AUTOSQL_FLOAT val = value_to_be_aggregated( _sample );

        sum()[_sample->ix_x_popul] -= val;

        sum_squared()[_sample->ix_x_popul] -= val * val;

        assert( count()[_sample->ix_x_popul] > 0.0 );

        count()[_sample->ix_x_popul] -= 1.0;

        if ( count()[_sample->ix_x_popul] == 0.0 )
            {
                yhat_inline()[_sample->ix_x_popul] = 0.0;
            }
        else
            {
                const AUTOSQL_FLOAT mean =
                    sum()[_sample->ix_x_popul] / count()[_sample->ix_x_popul];

                yhat_inline()[_sample->ix_x_popul] =
                    sum_squared()[_sample->ix_x_popul] /
                        count()[_sample->ix_x_popul] -
                    mean * mean;
            }
    }

    // --------------------------------------

    /// Clear all extras
    void clear_extras() final { altered_samples().clear(); }

    /// Whether this is an aggregation that requires the samples to be sorted
    bool needs_sorting() const final { return needs_sorting_; }

    /// Trivial setter
    void set_aggregation_impl(
        containers::Optional<AggregationImpl> &_aggregation_impl )
    {
        aggregation_impl_ = _aggregation_impl.get();
    }

    /// Trivial setter
    void set_optimization_criterion( optimizationcriteria::OptimizationCriterion
                                         *_optimization_criterion ) final
    {
        optimization_criterion_ = _optimization_criterion;
    }

    /// Trivial setter
    void set_samples_begin_end(
        Sample *_samples_begin, Sample *_samples_end ) final
    {
        samples_begin_ = _samples_begin;
        samples_end_ = _samples_end;
    }

    /// Trivial setter
    void set_value_to_be_aggregated(
        const containers::Matrix<AUTOSQL_FLOAT> &_value_to_be_aggregated,
        const AUTOSQL_INT _ix_column_used ) final
    {
        value_to_be_aggregated() = containers::
            ColumnView<AUTOSQL_FLOAT, std::map<AUTOSQL_INT, AUTOSQL_INT>>(
                _value_to_be_aggregated, _ix_column_used );
    }

    /// Trivial setter
    void set_value_to_be_aggregated(
        const containers::Matrix<AUTOSQL_INT> &_value_to_be_aggregated,
        const AUTOSQL_INT _ix_column_used ) final
    {
        value_to_be_aggregated_categorical() = containers::
            ColumnView<AUTOSQL_INT, std::map<AUTOSQL_INT, AUTOSQL_INT>>(
                _value_to_be_aggregated, _ix_column_used );
    }

    /// Trivial setter
    void set_value_to_be_aggregated(
        const containers::ColumnView<
            AUTOSQL_FLOAT,
            std::map<AUTOSQL_INT, AUTOSQL_INT>> &_value_to_be_aggregated ) final
    {
        value_to_be_aggregated() = _value_to_be_aggregated;
    }

    /// Trivial setter
    void set_value_to_be_compared(
        const containers::ColumnView<AUTOSQL_FLOAT, std::vector<AUTOSQL_INT>>
            &_value_to_be_compared ) final
    {
        value_to_be_compared() = _value_to_be_compared;
    }

    /// Returns a string describing the type of the aggregation
    std::string type() const final { return AggType::type(); }

    /// Returns a reference to the predictions stored by the aggregation
    containers::Matrix<AUTOSQL_FLOAT> &yhat() final
    {
        assert( aggregation_impl_ != nullptr );
        return aggregation_impl_->yhat_;
    }

    // --------------------------------------

   private:
    /// Trivial accessor
    std::vector<Sample *> &altered_samples() { return altered_samples_; }

    /// Trivial accessor
    inline std::vector<AUTOSQL_FLOAT> &count()
    {
        assert( aggregation_impl_ != nullptr );
        return aggregation_impl_->count_;
    }

    /// Trivial accessor
    inline std::vector<AUTOSQL_FLOAT> &count_committed()
    {
        assert( aggregation_impl_ != nullptr );
        return aggregation_impl_->count_committed_;
    }

    /// Finds the next greater sample that is activated.
    /// Assumes that there is at least one activated sample
    /// greater than _begin.
    inline Sample *find_next_greater( Sample *_begin )
    {
        auto it = _begin + 1;

        while ( it->activated == false )
            {
                assert( it < samples_end_ );
                assert( it->ix_x_popul == _begin->ix_x_popul );
                ++it;
            }

        return it;
    }

    /// Finds the next smaller sample that is activated.
    /// Assumes that there is at least one activated sample
    /// smaller than _begin.
    inline Sample *find_next_smaller( Sample *_begin )
    {
        auto it = _begin - 1;

        while ( it->activated == false )
            {
                assert( it >= samples_begin_ );
                assert( it->ix_x_popul == _begin->ix_x_popul );
                --it;
            }

        return it;
    }

    /// Trivial accessor
    inline optimizationcriteria::OptimizationCriterion *optimization_criterion()
    {
        return optimization_criterion_;
    }

    /// Trivial accessor
    inline std::vector<Sample *> &sample_ptr()
    {
        assert( aggregation_impl_ != nullptr );
        return aggregation_impl_->sample_ptr_;
    }

    /// Trivial accessor
    inline std::vector<Sample *> &sample_ptr_committed()
    {
        assert( aggregation_impl_ != nullptr );
        return aggregation_impl_->sample_ptr_committed_;
    }

    /// Trivial accessor
    inline std::vector<AUTOSQL_FLOAT> &sum()
    {
        assert( aggregation_impl_ != nullptr );
        return aggregation_impl_->sum_;
    }

    /// Trivial accessor
    inline std::vector<AUTOSQL_FLOAT> &sum_committed()
    {
        assert( aggregation_impl_ != nullptr );
        return aggregation_impl_->sum_committed_;
    }

    /// Trivial accessor
    inline std::vector<AUTOSQL_FLOAT> &sum_cubed()
    {
        assert( aggregation_impl_ != nullptr );
        return aggregation_impl_->sum_cubed_;
    }

    /// Trivial accessor
    inline std::vector<AUTOSQL_FLOAT> &sum_cubed_committed()
    {
        assert( aggregation_impl_ != nullptr );
        return aggregation_impl_->sum_cubed_committed_;
    }

    /// Trivial accessor
    inline std::vector<AUTOSQL_FLOAT> &sum_squared()
    {
        assert( aggregation_impl_ != nullptr );
        return aggregation_impl_->sum_squared_;
    }

    /// Trivial accessor
    inline std::vector<AUTOSQL_FLOAT> &sum_squared_committed()
    {
        assert( aggregation_impl_ != nullptr );
        return aggregation_impl_->sum_squared_committed_;
    }

    /// Trivial accessor
    inline containers::IntSet &updates_current()
    {
        assert( aggregation_impl_ != nullptr );
        return aggregation_impl_->updates_current_;
    }

    /// Trivial accessor
    inline containers::IntSet &updates_stored()
    {
        assert( aggregation_impl_ != nullptr );
        return aggregation_impl_->updates_stored_;
    }

    /// Trivial accessor
    inline containers::
        ColumnView<AUTOSQL_FLOAT, std::map<AUTOSQL_INT, AUTOSQL_INT>>
            &value_to_be_aggregated()
    {
        assert( aggregation_impl_ != nullptr );
        return aggregation_impl_->value_to_be_aggregated_;
    }

    /// Trivial accessor
    inline containers::ColumnView<AUTOSQL_INT, std::map<AUTOSQL_INT, AUTOSQL_INT>>
        &value_to_be_aggregated_categorical()
    {
        assert( aggregation_impl_ != nullptr );
        return aggregation_impl_->value_to_be_aggregated_categorical_;
    }

    /// Accessor for the value to be aggregated to be used for all
    /// aggregations that ...
    /// 1. ...are NOT applied to categorical data
    /// 2. ...ARE a comparison
    /// 3. ...the value to be compared IS in the population table
    template <
        DataUsed data_used = data_used_,
        bool is_population = is_population_,
        typename std::enable_if<
            !AggregationType::IsCategorical<data_used>::value &&
                AggregationType::IsComparison<data_used>::value &&
                is_population,
            int>::type = 0>
    inline const AUTOSQL_FLOAT value_to_be_aggregated( const Sample *_sample )
    {
        return value_to_be_compared()( _sample->ix_x_popul ) -
               value_to_be_aggregated()[_sample->ix_x_perip];
    }

    /// Accessor for the value to be aggregated to be used for all
    /// aggregations that ...
    /// 1. ...are NOT applied to categorical data
    /// 2. ...ARE a comparison
    /// 3. ...the value to be compared is NOT in the population table
    template <
        DataUsed data_used = data_used_,
        bool is_population = is_population_,
        typename std::enable_if<
            !AggregationType::IsCategorical<data_used>::value &&
                AggregationType::IsComparison<data_used>::value &&
                !is_population,
            int>::type = 0>
    inline const AUTOSQL_FLOAT value_to_be_aggregated( const Sample *_sample )
    {
        return value_to_be_compared()[_sample->ix_x_perip] -
               value_to_be_aggregated()[_sample->ix_x_perip];
    }

    /// Accessor for the value to be aggregated to be used for all
    /// aggregations that ...
    /// 1. ...are NOT applied to categorical data
    /// 2. ...are NOT a comparison
    /// 3. ...are NOT applied to to subfeatures
    template <
        DataUsed data_used = data_used_,
        typename std::enable_if<
            !AggregationType::IsCategorical<data_used>::value &&
                !AggregationType::IsComparison<data_used>::value &&
                data_used != DataUsed::x_subfeature,
            int>::type = 0>
    inline const AUTOSQL_FLOAT value_to_be_aggregated( const Sample *_sample )
    {
        return value_to_be_aggregated()[_sample->ix_x_perip];
    }

    /// Accessor for the value to be aggregated to be used for all
    /// aggregations that ...
    /// 1. ...are NOT applied to categorical data
    /// 2. ...are NOT a comparison
    /// 3. ...ARE applied to to subfeatures
    template <
        DataUsed data_used = data_used_,
        typename std::enable_if<
            !AggregationType::IsCategorical<data_used>::value &&
                !AggregationType::IsComparison<data_used>::value &&
                data_used == DataUsed::x_subfeature,
            int>::type = 0>
    inline const AUTOSQL_FLOAT value_to_be_aggregated( const Sample *_sample )
    {
        return value_to_be_aggregated()( _sample->ix_x_perip );
    }

    /// Accessor for the value to be aggregated to be used for all
    /// aggregations that ...
    /// 1. ...ARE applied to categorical data
    template <
        DataUsed data_used = data_used_,
        typename std::enable_if<
            AggregationType::IsCategorical<data_used>::value,
            int>::type = 0>
    inline const AUTOSQL_FLOAT value_to_be_aggregated( const Sample *_sample )
    {
        return static_cast<AUTOSQL_FLOAT>(
            value_to_be_aggregated_categorical()[_sample->ix_x_perip] );
    }

    /// Accessor for the value to be compared - this is needed for the
    /// time_stamps and the same_units.
    inline containers::ColumnView<AUTOSQL_FLOAT, std::vector<AUTOSQL_INT>>
        &value_to_be_compared()
    {
        assert( aggregation_impl_ != nullptr );
        return aggregation_impl_->value_to_be_compared_;
    }

    /// Accessor for the value to be compared - this is needed for the
    /// time_stamps and the same_units.
    inline std::vector<AUTOSQL_FLOAT> &yhat_committed()
    {
        assert( aggregation_impl_ != nullptr );
        return aggregation_impl_->yhat_committed_;
    }

    /// Trivial accessor
    inline containers::Matrix<AUTOSQL_FLOAT> &yhat_inline()
    {
        assert( aggregation_impl_ != nullptr );
        return aggregation_impl_->yhat_;
    }

    /// Trivial accessor
    inline std::vector<AUTOSQL_FLOAT> &yhat_stored()
    {
        assert( aggregation_impl_ != nullptr );
        return aggregation_impl_->yhat_stored_;
    }

    // --------------------------------------

   private:
    /// Pimpl for aggregation
    AggregationImpl *aggregation_impl_;

    /// Contains pointers to samples that have been changed
    /// since the last commit
    std::vector<Sample *> altered_samples_;

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
    Sample *samples_begin_;

    /// Pointer to the element behind the last element in samples -
    /// some aggregations like min and max need to know this
    Sample *samples_end_;

    /// Denotes whether the updates since the last commit had
    /// been activated or deactivated. revert_to_commit() needs this
    /// piece of information.
    bool updates_have_been_activated_;
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::activate_all(
    const bool _init_opt,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
    AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    debug_message( "activate_all..." );

    for ( auto it = _sample_container_begin; it != _sample_container_end; ++it )
        {
            ( *it )->activated = false;
        }

    for ( auto it = _sample_container_begin; it != _sample_container_end; ++it )
        {
            activate_sample( *it );
        }

    if ( _init_opt )
        {
            updates_stored().clear();

            for ( auto it = _sample_container_begin;
                  it != _sample_container_end;
                  ++it )
                {
                    updates_stored().insert( ( *it )->ix_x_popul );
                }

            init_optimization_criterion(
                _sample_container_begin, _sample_container_end );
        }

    debug_message( "activate_all...done" );
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::
    activate_samples_containing_categories(
        const std::vector<AUTOSQL_INT>::const_iterator _categories_begin,
        const std::vector<AUTOSQL_INT>::const_iterator _categories_end,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    for ( auto it = _sample_container_begin; it != _sample_container_end; ++it )
        {
            for ( auto cat = _categories_begin; cat < _categories_end; ++cat )
                {
                    assert( cat == _categories_begin || *cat > *( cat - 1 ) );

                    if ( ( *it )->categorical_value == *cat )
                        {
                            activate_sample( *it );
                            break;
                        }
                    else if ( ( *it )->categorical_value < *cat )
                        {
                            break;
                        }
                }
        }
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::
    activate_samples_containing_categories(
        const std::vector<AUTOSQL_INT>::const_iterator _categories_begin,
        const std::vector<AUTOSQL_INT>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index )
{
    // ------------------------------------------------------------------

    AUTOSQL_FLOAT num_samples_smaller = 0.0;

    auto sample_size = static_cast<AUTOSQL_FLOAT>(
        std::distance( _index.begin(), _index.end() ) );

    // ------------------------------------------------------------------

    for ( auto cat = _categories_begin; cat < _categories_end; ++cat )
        {
            for ( auto it = _index.begin( *cat ); it < _index.end( *cat );
                  ++it )
                {
                    assert( ( *it )->categorical_value == *cat );
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

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::
    activate_samples_from_above(
        const AUTOSQL_FLOAT _critical_value,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    for ( auto it = _sample_container_begin; it != _sample_container_end; ++it )
        {
            if ( ( *it )->numerical_value > _critical_value )
                {
                    activate_sample( *it );
                }
        }
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::
    activate_samples_from_above(
        const containers::Matrix<AUTOSQL_FLOAT> &_critical_values,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    assert( _sample_container_end > _sample_container_begin );
    assert( _critical_values.nrows() > 0 );

    auto it = _sample_container_end - 1;

    for ( AUTOSQL_INT i = _critical_values.nrows() - 1; i >= 0; --i )
        {
            while ( it >= _sample_container_begin )
                {
                    if ( ( *it )->numerical_value <= _critical_values[i] )
                        {
                            break;
                        }

                    activate_sample( *it );

                    updates_stored().insert( ( *it )->ix_x_popul );
                    updates_current().insert( ( *it )->ix_x_popul );

                    --it;
                }

            auto num_samples_smaller = static_cast<AUTOSQL_FLOAT>(
                std::distance( _sample_container_begin, it ) );

            auto num_samples_greater = static_cast<AUTOSQL_FLOAT>(
                std::distance( it, _sample_container_end ) );

            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::
    activate_samples_from_below(
        const AUTOSQL_FLOAT _critical_value,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    for ( auto it = _sample_container_begin; it != _sample_container_end; ++it )
        {
            if ( ( *it )->numerical_value <= _critical_value )
                {
                    activate_sample( *it );
                }
        }
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::
    activate_samples_from_below(
        const containers::Matrix<AUTOSQL_FLOAT> &_critical_values,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    auto it = _sample_container_begin;

    for ( AUTOSQL_INT i = 0; i < _critical_values.nrows(); ++i )
        {
            while ( it != _sample_container_end )
                {
                    if ( ( *it )->numerical_value > _critical_values[i] )
                        {
                            break;
                        }

                    activate_sample( *it );

                    updates_stored().insert( ( *it )->ix_x_popul );
                    updates_current().insert( ( *it )->ix_x_popul );

                    ++it;
                }

            auto num_samples_smaller = static_cast<AUTOSQL_FLOAT>(
                std::distance( _sample_container_begin, it ) );

            auto num_samples_greater = static_cast<AUTOSQL_FLOAT>(
                std::distance( it, _sample_container_end ) );

            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::
    activate_samples_not_containing_categories(
        const std::vector<AUTOSQL_INT>::const_iterator _categories_begin,
        const std::vector<AUTOSQL_INT>::const_iterator _categories_end,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    for ( auto it = _sample_container_begin; it != _sample_container_end; ++it )
        {
            auto activate = true;

            for ( auto cat = _categories_begin; cat < _categories_end; ++cat )
                {
                    assert( cat == _categories_begin || *cat > *( cat - 1 ) );

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
                }
        }
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::
    activate_samples_not_containing_categories(
        const std::vector<AUTOSQL_INT>::const_iterator _categories_begin,
        const std::vector<AUTOSQL_INT>::const_iterator _categories_end,
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

    AUTOSQL_FLOAT num_samples_smaller = 0.0;

    auto sample_size = static_cast<AUTOSQL_FLOAT>(
        std::distance( _index.begin(), _index.end() ) );

    for ( auto cat = _categories_begin; cat < _categories_end; ++cat )
        {
            for ( auto it = _index.begin( *cat ); it < _index.end( *cat );
                  ++it )
                {
                    assert( ( *it )->categorical_value == *cat );
                    deactivate_sample( *it );

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
                            assert( ( *it )->categorical_value == *cat );
                            activate_sample( *it );
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

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::clear()
{
    altered_samples().clear();

    value_to_be_aggregated().clear();

    value_to_be_aggregated_categorical().clear();

    value_to_be_compared().clear();

    updates_current().clear();

    updates_stored().clear();
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::commit()
{
    // --------------------------------------------------

    if ( needs_altered_samples_ )
        {
            altered_samples().clear();
        }

    // --------------------------------------------------

    if ( needs_count_ )
        {
            for ( auto i : updates_stored() )
                {
                    count_committed()[i] = count()[i];
                }
        }

    // --------------------------------------------------

    if ( needs_sample_ptr_ )
        {
            for ( auto i : updates_stored() )
                {
                    sample_ptr_committed()[i] = sample_ptr()[i];
                }
        }

    // --------------------------------------------------

    if ( needs_sum_ )
        {
            for ( auto i : updates_stored() )
                {
                    sum_committed()[i] = sum()[i];
                }
        }

    // --------------------------------------------------

    if ( needs_sum_cubed_ )
        {
            for ( auto i : updates_stored() )
                {
                    sum_cubed_committed()[i] = sum_cubed()[i];
                }
        }

    // --------------------------------------------------

    if ( needs_sum_squared_ )
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

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::
    deactivate_samples_containing_categories(
        const std::vector<AUTOSQL_INT>::const_iterator _categories_begin,
        const std::vector<AUTOSQL_INT>::const_iterator _categories_end,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    for ( auto it = _sample_container_begin; it != _sample_container_end; ++it )
        {
            for ( auto cat = _categories_begin; cat < _categories_end; ++cat )
                {
                    assert( cat == _categories_begin || *cat > *( cat - 1 ) );

                    if ( ( *it )->categorical_value == *cat )
                        {
                            deactivate_sample( *it );
                            break;
                        }

                    else if ( ( *it )->categorical_value < *cat )
                        {
                            break;
                        }
                }
        }
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::
    deactivate_samples_containing_categories(
        const std::vector<AUTOSQL_INT>::const_iterator _categories_begin,
        const std::vector<AUTOSQL_INT>::const_iterator _categories_end,
        const Revert _revert,
        const containers::CategoryIndex &_index )
{
    // ------------------------------------------------------------------

    AUTOSQL_FLOAT num_samples_smaller = 0.0;

    auto sample_size = static_cast<AUTOSQL_FLOAT>(
        std::distance( _index.begin(), _index.end() ) );

    // ------------------------------------------------------------------

    for ( auto cat = _categories_begin; cat < _categories_end; ++cat )
        {
            for ( auto it = _index.begin( *cat ); it < _index.end( *cat );
                  ++it )
                {
                    assert( ( *it )->categorical_value == *cat );
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

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::
    deactivate_samples_from_above(
        const AUTOSQL_FLOAT _critical_value,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    for ( auto it = _sample_container_begin; it != _sample_container_end; ++it )
        {
            const AUTOSQL_FLOAT val = ( *it )->numerical_value;

            if ( val > _critical_value || val != val )
                {
                    deactivate_sample( *it );
                }
        }
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::
    deactivate_samples_from_above(
        const containers::Matrix<AUTOSQL_FLOAT> &_critical_values,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    assert( _sample_container_end > _sample_container_begin );
    assert( _critical_values.nrows() > 0 );

    auto it = _sample_container_end - 1;

    for ( AUTOSQL_INT i = _critical_values.nrows() - 1; i >= 0; --i )
        {
            while ( it >= _sample_container_begin )
                {
                    if ( ( *it )->numerical_value <= _critical_values[i] )
                        {
                            break;
                        }

                    deactivate_sample( *it );

                    updates_stored().insert( ( *it )->ix_x_popul );
                    updates_current().insert( ( *it )->ix_x_popul );

                    --it;
                }

            auto num_samples_smaller = static_cast<AUTOSQL_FLOAT>(
                std::distance( _sample_container_begin, it ) );

            auto num_samples_greater = static_cast<AUTOSQL_FLOAT>(
                std::distance( it, _sample_container_end ) );

            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::
    deactivate_samples_from_below(
        const AUTOSQL_FLOAT _critical_value,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    for ( auto it = _sample_container_begin; it != _sample_container_end; ++it )
        {
            const AUTOSQL_FLOAT val = ( *it )->numerical_value;

            if ( val <= _critical_value || val != val )
                {
                    deactivate_sample( *it );
                }
        }
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::
    deactivate_samples_from_below(
        const containers::Matrix<AUTOSQL_FLOAT> &_critical_values,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    auto it = _sample_container_begin;

    for ( AUTOSQL_INT i = 0; i < _critical_values.nrows(); ++i )
        {
            while ( it != _sample_container_end )
                {
                    if ( ( *it )->numerical_value > _critical_values[i] )
                        {
                            break;
                        }

                    deactivate_sample( *it );

                    updates_stored().insert( ( *it )->ix_x_popul );
                    updates_current().insert( ( *it )->ix_x_popul );

                    ++it;
                }

            auto num_samples_smaller = static_cast<AUTOSQL_FLOAT>(
                std::distance( _sample_container_begin, it ) );

            auto num_samples_greater = static_cast<AUTOSQL_FLOAT>(
                std::distance( it, _sample_container_end ) );

            update_optimization_criterion_and_clear_updates_current(
                num_samples_smaller, num_samples_greater );
        }
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::
    deactivate_samples_not_containing_categories(
        const std::vector<AUTOSQL_INT>::const_iterator _categories_begin,
        const std::vector<AUTOSQL_INT>::const_iterator _categories_end,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    for ( auto it = _sample_container_begin; it != _sample_container_end; ++it )
        {
            auto deactivate = true;

            for ( auto cat = _categories_begin; cat < _categories_end; ++cat )
                {
                    assert( cat == _categories_begin || *cat > *( cat - 1 ) );

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
                }
        }
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::
    deactivate_samples_not_containing_categories(
        const std::vector<AUTOSQL_INT>::const_iterator _categories_begin,
        const std::vector<AUTOSQL_INT>::const_iterator _categories_end,
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

    auto sample_size = static_cast<AUTOSQL_FLOAT>(
        std::distance( _index.begin(), _index.end() ) );

    AUTOSQL_FLOAT num_samples_smaller = 0.0;

    for ( auto cat = _categories_begin; cat < _categories_end; ++cat )
        {
            for ( auto it = _index.begin( *cat ); it < _index.end( *cat );
                  ++it )
                {
                    assert( ( *it )->categorical_value == *cat );
                    activate_sample( *it );

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
                            assert( ( *it )->categorical_value == *cat );
                            deactivate_sample( *it );
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

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::
    deactivate_samples_with_null_values(
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _null_values_separator )
{
    assert( _null_values_separator >= _sample_container_begin );

    for ( auto it = _sample_container_begin; it != _null_values_separator;
          ++it )
        {
            deactivate_sample( *it );

            updates_stored().insert( ( *it )->ix_x_popul );
            updates_current().insert( ( *it )->ix_x_popul );
        }
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::
    init_optimization_criterion(
        AUTOSQL_SAMPLE_ITERATOR _sample_container_begin,
        AUTOSQL_SAMPLE_ITERATOR _sample_container_end )
{
    debug_message( "init_optimization_criterion..." );

    optimization_criterion()->set_storage_size( 1 );

    optimization_criterion()->init_yhat( yhat_inline(), updates_stored() );

    auto num_samples = static_cast<AUTOSQL_FLOAT>(
        std::distance( _sample_container_begin, _sample_container_end ) );

    optimization_criterion()->store_current_stage( num_samples, num_samples );

    optimization_criterion()->find_maximum();

    debug_message( "init_optimization_criterion...done" );
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
std::string
Aggregation<AggType, data_used_, is_population_>::intermediate_type() const
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

template <typename AggType, DataUsed data_used_, bool is_population_>
std::shared_ptr<optimizationcriteria::OptimizationCriterion>
Aggregation<AggType, data_used_, is_population_>::make_intermediate(
    std::shared_ptr<IntermediateAggregationImpl> _impl ) const
{
    // --------------------------------------------------

    debug_message( "make_intermediate..." );

    assert( !no_intermediate_ );

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
            assert(
                false &&
                "Unknown aggregation type in make_intermediate(...)!" );

            return std::shared_ptr<
                optimizationcriteria::OptimizationCriterion>();
        }
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::revert_to_commit()
{
    // --------------------------------------------------

    if ( needs_altered_samples_ )
        {
            for ( Sample *sample : altered_samples() )
                {
                    sample->activated = !( sample->activated );
                }

            altered_samples().clear();
        }

    // --------------------------------------------------

    if ( needs_count_ )
        {
            for ( auto i : updates_stored() )
                {
                    count()[i] = count_committed()[i];
                }
        }

    // --------------------------------------------------

    if ( needs_sample_ptr_ )
        {
            for ( auto i : updates_stored() )
                {
                    sample_ptr()[i] = sample_ptr_committed()[i];
                }
        }

    // --------------------------------------------------

    if ( needs_sum_ )
        {
            for ( auto i : updates_stored() )
                {
                    sum()[i] = sum_committed()[i];
                }
        }

    // --------------------------------------------------

    if ( needs_sum_cubed_ )
        {
            for ( auto i : updates_stored() )
                {
                    sum_cubed()[i] = sum_cubed_committed()[i];
                }
        }

    // --------------------------------------------------

    if ( needs_sum_squared_ )
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

template <typename AggType, DataUsed data_used_, bool is_population_>
AUTOSQL_SAMPLES::iterator
Aggregation<AggType, data_used_, is_population_>::separate_null_values(
    AUTOSQL_SAMPLES &_samples )
{
    auto is_null = [this]( Sample &sample ) {
        AUTOSQL_FLOAT val = value_to_be_aggregated( &sample );
        return val != val;
    };

    if ( std::is_partitioned( _samples.begin(), _samples.end(), is_null ) )
        {
            return std::partition_point(
                _samples.begin(), _samples.end(), is_null );
        }
    else
        {
            return std::stable_partition(
                _samples.begin(), _samples.end(), is_null );
        }
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
AUTOSQL_SAMPLE_CONTAINER::iterator
Aggregation<AggType, data_used_, is_population_>::separate_null_values(
    AUTOSQL_SAMPLE_CONTAINER &_samples )
{
    auto is_null = [this]( Sample *sample ) {
        AUTOSQL_FLOAT val = value_to_be_aggregated( sample );
        return val != val;
    };

    if ( std::is_partitioned( _samples.begin(), _samples.end(), is_null ) )
        {
            return std::partition_point(
                _samples.begin(), _samples.end(), is_null );
        }
    else
        {
            return std::stable_partition(
                _samples.begin(), _samples.end(), is_null );
        }
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::sort_samples(
    AUTOSQL_SAMPLES::iterator _samples_begin,
    AUTOSQL_SAMPLES::iterator _samples_end )
{
    // -----------------------------------

    assert( needs_sorting_ );

    // -----------------------------------

    if ( std::distance( _samples_begin, _samples_end ) == 0 )
        {
            return;
        }

    auto compare_op = [this]( const Sample &sample1, const Sample &sample2 ) {
        if ( sample1.ix_x_popul < sample2.ix_x_popul )
            {
                return true;
            }
        else if ( sample1.ix_x_popul > sample2.ix_x_popul )
            {
                return false;
            }
        else
            {
                return value_to_be_aggregated( &sample1 ) <
                       value_to_be_aggregated( &sample2 );
            }
    };

    // -----------------------------------

    std::sort( _samples_begin, _samples_end, compare_op );

    // -----------------------------------
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::
    update_optimization_criterion_and_clear_updates_current(
        const AUTOSQL_FLOAT _num_samples_smaller,
        const AUTOSQL_FLOAT _num_samples_greater )
{
    optimization_criterion()->update_samples(
        updates_current(),  // _indices
        yhat_inline(),      // _new_values
        yhat_stored()       // _old_values
    );

    for ( AUTOSQL_INT ix : updates_current() )
        {
            yhat_stored()[ix] = yhat_inline()[ix];
        }

    updates_current().clear();

    optimization_criterion()->store_current_stage(
        _num_samples_smaller, _num_samples_greater );
}

// ----------------------------------------------------------------------------

template <typename AggType, DataUsed data_used_, bool is_population_>
void Aggregation<AggType, data_used_, is_population_>::reset()
{
    // --------------------------------------------------

    if ( needs_altered_samples_ )
        {
            altered_samples().clear();
        }

    // --------------------------------------------------

    if ( needs_count_ )
        {
            std::fill( count().begin(), count().end(), 0.0 );

            std::fill(
                count_committed().begin(), count_committed().end(), 0.0 );
        }

    // --------------------------------------------------

    if ( needs_sum_ )
        {
            std::fill( sum().begin(), sum().end(), 0.0 );

            std::fill( sum_committed().begin(), sum_committed().end(), 0.0 );
        }

    // --------------------------------------------------

    if ( needs_sum_cubed_ )
        {
            std::fill( sum_cubed().begin(), sum_cubed().end(), 0.0 );

            std::fill(
                sum_cubed_committed().begin(),
                sum_cubed_committed().end(),
                0.0 );
        }

    // --------------------------------------------------

    if ( needs_sum_squared_ )
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
}  // namespace aggregations
}  // namespace autosql

#endif  // AUTOSQL_AGGREGATIONS_AGGREGATION_HPP_
