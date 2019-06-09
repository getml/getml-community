#ifndef AUTOSQL_AGGREGATIONS_INTERMEDIATEAGGREGATION_HPP_
#define AUTOSQL_AGGREGATIONS_INTERMEDIATEAGGREGATION_HPP_

namespace autosql
{
namespace aggregations
{
// ----------------------------------------------------------------------------

/// Intermediate aggregations are used as in between the currently used
/// aggregations and the optimization function
template <typename AggType>
class IntermediateAggregation
    : public optimizationcriteria::OptimizationCriterion
{
    // --------------------------------------

   public:
    IntermediateAggregation(
        std::shared_ptr<IntermediateAggregationImpl> _impl )
        : impl_( _impl )
    {
    }

    ~IntermediateAggregation() = default;

    // --------------------------------------

    /// Commits the current stage, accepting it as the new state of the
    /// tree
    void commit() final;

    /// Initializes yhat
    void init_yhat(
        const std::vector<AUTOSQL_FLOAT>& _yhat,
        const containers::IntSet& _indices ) final;

    /// Resets sufficient statistics to zero
    void reset() final;

    /// Commits the current stage, accepting it as the new state of the
    /// tree
    void revert_to_commit() final;

    /// Updates all samples designated by _indices
    void update_samples(
        const containers::IntSet& _indices,
        const std::vector<AUTOSQL_FLOAT>& _new_values,
        const std::vector<AUTOSQL_FLOAT>& _old_values ) final;

    // --------------------------------------

    /// Sorts a specific subsection of the values defined by _begin and _end.
    /// Returns the indices from greatest to smallest. This is useful for
    /// combining categories.
    std::vector<AUTOSQL_INT> argsort(
        const AUTOSQL_INT _begin, const AUTOSQL_INT _end ) const final
    {
        return parent().argsort( _begin, _end );
    }

    /// Calculates statistics that have to be calculated only once
    void init(
        const std::vector<std::vector<AUTOSQL_FLOAT>>& _y,
        const std::vector<AUTOSQL_FLOAT>& _sample_weights ) final
    {
        assert(
            false &&
            "IntermediateAggregation::init(...) should never be called!" );
    }

    /// Finds the index associated with the maximum of the optimization
    /// criterion
    AUTOSQL_INT find_maximum() final { return parent().find_maximum(); }

    /// Trivial setter
    void set_comm( multithreading::Communicator* _comm ) final
    {
        parent().set_comm( _comm );
    }

    /// An intermediate aggregation has no storage, so it
    /// is redelegated to the parent.
    const AUTOSQL_INT storage_ix() const final { return parent().storage_ix(); }

    /// Stores the current stage
    void store_current_stage(
        const AUTOSQL_FLOAT _num_samples_smaller,
        const AUTOSQL_FLOAT _num_samples_greater )
    {
        parent().store_current_stage(
            _num_samples_smaller, _num_samples_greater );
    }

    /// Trivial getter
    AUTOSQL_FLOAT value() final { return parent().value(); }

    /// Trivial getter
    AUTOSQL_FLOAT values_stored( const size_t _i ) final
    {
        return parent().values_stored( _i );
    }

    // --------------------------------------

   private:
    /// Calculates the counts designated by _indices_agg, if necessary.
    /// Remember - counts don't change.
    void calculate_counts( const std::vector<AUTOSQL_INT>& _indices_agg );

    // --------------------------------------

   private:
    /// Trivial accessor
    inline std::vector<AUTOSQL_FLOAT>& count() { return impl().count_; }

    /// Trivial accessor
    inline const AggregationIndex& index() const { return impl().index(); }

    /// Trivial accessor
    inline IntermediateAggregationImpl& impl()
    {
        assert( impl_ );
        return *impl_;
    }

    /// Trivial accessor
    inline const IntermediateAggregationImpl& impl() const
    {
        assert( impl_ );
        return *impl_;
    }

    /// Trivial accessor
    inline optimizationcriteria::OptimizationCriterion& parent()
    {
        return *impl().parent_;
    }

    /// Trivial accessor
    inline const optimizationcriteria::OptimizationCriterion& parent() const
    {
        return *impl().parent_;
    }

    /// update_sample for AVG aggregation
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Avg>::value,
            int>::type = 0>
    inline void update_sample(
        const AUTOSQL_INT _ix_agg,
        const AUTOSQL_FLOAT& _new_value,
        const AUTOSQL_FLOAT& _old_value )
    {
        assert( _ix_agg >= 0 );
        assert( static_cast<size_t>( _ix_agg ) < yhat().size() );
        assert( count().size() == yhat().size() );
        assert( count()[_ix_agg] > 0.0 );

        static_assert( needs_count_, "COUNT needed!" );

        yhat()[_ix_agg] +=
            _new_value / count()[_ix_agg] - _old_value / count()[_ix_agg];
    }

    /// update_sample for SUM aggregation
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Sum>::value,
            int>::type = 0>
    inline void update_sample(
        const AUTOSQL_INT _ix_agg,
        const AUTOSQL_FLOAT& _new_value,
        const AUTOSQL_FLOAT& _old_value )
    {
        assert( _ix_agg >= 0 );
        assert( static_cast<size_t>( _ix_agg ) < yhat().size() );

        yhat()[_ix_agg] += _new_value - _old_value;
    }

    /// update_sample for SKEWNESS aggregation
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Skewness>::value,
            int>::type = 0>
    inline void update_sample(
        const AUTOSQL_INT _ix_agg,
        const AUTOSQL_FLOAT& _new_value,
        const AUTOSQL_FLOAT& _old_value )
    {
        assert( _ix_agg >= 0 );
        assert( static_cast<size_t>( _ix_agg ) < yhat().size() );
        assert( sum().size() == yhat().size() );
        assert( sum_squared().size() == yhat().size() );
        assert( sum_cubed().size() == yhat().size() );
        assert( count().size() == yhat().size() );
        assert( count()[_ix_agg] > 0.0 );

        static_assert( needs_count_, "count needed!" );
        static_assert( needs_sum_, "sum needed!" );
        static_assert( needs_sum_squared_, "sum_squared needed!" );
        static_assert( needs_sum_cubed_, "sum_cubed needed!" );

        sum()[_ix_agg] += _new_value - _old_value;

        sum_squared()[_ix_agg] +=
            _new_value * _new_value - _old_value * _old_value;

        sum_cubed()[_ix_agg] += _new_value * _new_value * _new_value -
                                _old_value * _old_value * _old_value;

        const AUTOSQL_FLOAT mean = sum()[_ix_agg] / count()[_ix_agg];

        const AUTOSQL_FLOAT stddev = std::sqrt(
            sum_squared()[_ix_agg] / count()[_ix_agg] - mean * mean );

        yhat()[_ix_agg] =
            ( ( sum_cubed()[_ix_agg] / count()[_ix_agg] ) -
              ( 3.0 * mean * stddev * stddev ) - ( mean * mean * mean ) ) /
            ( stddev * stddev * stddev );
    }

    /// update_sample for VAR aggregation
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Var>::value,
            int>::type = 0>
    inline void update_sample(
        const AUTOSQL_INT _ix_agg,
        const AUTOSQL_FLOAT& _new_value,
        const AUTOSQL_FLOAT& _old_value )
    {
        assert( _ix_agg >= 0 );
        assert( static_cast<size_t>( _ix_agg ) < yhat().size() );
        assert( sum().size() == yhat().size() );
        assert( sum_squared().size() == yhat().size() );
        assert( count().size() == yhat().size() );
        assert( count()[_ix_agg] > 0.0 );

        static_assert( needs_count_, "count needed!" );
        static_assert( needs_sum_, "sum needed!" );
        static_assert( needs_sum_squared_, "sum_squared needed!" );

        sum()[_ix_agg] += _new_value - _old_value;

        sum_squared()[_ix_agg] +=
            _new_value * _new_value - _old_value * _old_value;

        const AUTOSQL_FLOAT mean = sum()[_ix_agg] / count()[_ix_agg];

        yhat()[_ix_agg] =
            sum_squared()[_ix_agg] / count()[_ix_agg] - mean * mean;
    }

    /// update_sample for STDDEV aggregation
    template <
        typename Agg = AggType,
        typename std::enable_if<
            std::is_same<Agg, AggregationType::Stddev>::value,
            int>::type = 0>
    inline void update_sample(
        const AUTOSQL_INT _ix_agg,
        const AUTOSQL_FLOAT& _new_value,
        const AUTOSQL_FLOAT& _old_value )
    {
        update_sample<AggregationType::Var, 0>(
            _ix_agg, _new_value, _old_value );

        yhat()[_ix_agg] = std::sqrt( yhat()[_ix_agg] );
    }

    /// Trivial accessor
    inline containers::IntSet& updates_current()
    {
        return impl().updates_current_;
    }

    /// Trivial accessor
    inline containers::IntSet& updates_stored()
    {
        return impl().updates_stored_;
    }

    /// Trivial accessor
    inline std::vector<AUTOSQL_FLOAT>& sum() { return impl().sum_; }

    /// Trivial accessor
    inline std::vector<AUTOSQL_FLOAT>& sum_committed()
    {
        return impl().sum_committed_;
    }

    /// Trivial accessor
    inline std::vector<AUTOSQL_FLOAT>& sum_cubed() { return impl().sum_cubed_; }

    /// Trivial accessor
    inline std::vector<AUTOSQL_FLOAT>& sum_cubed_committed()
    {
        return impl().sum_cubed_committed_;
    }

    /// Trivial accessor
    inline std::vector<AUTOSQL_FLOAT>& sum_squared()
    {
        return impl().sum_squared_;
    }

    /// Trivial accessor
    inline std::vector<AUTOSQL_FLOAT>& sum_squared_committed()
    {
        return impl().sum_squared_committed_;
    }

    /// Trivial accessor
    inline std::vector<AUTOSQL_FLOAT>& yhat() { return impl().yhat_; }

    /// Trivial accessor
    inline std::vector<AUTOSQL_FLOAT>& yhat_committed()
    {
        return impl().yhat_committed_;
    }

    /// Trivial accessor
    inline std::vector<AUTOSQL_FLOAT>& yhat_stored()
    {
        return impl().yhat_stored_;
    }

    // --------------------------------------

   private:
    /// Contains all of the necessary data
    const std::shared_ptr<IntermediateAggregationImpl> impl_;

    /// Whether the intermediate aggregation relies on counts()
    constexpr static bool needs_count_ =
        std::is_same<AggType, AggregationType::Avg>() ||
        std::is_same<AggType, AggregationType::Skewness>() ||
        std::is_same<AggType, AggregationType::Stddev>() ||
        std::is_same<AggType, AggregationType::Var>();

    /// Whether the intermediate aggregation relies on sum()
    constexpr static bool needs_sum_ =
        std::is_same<AggType, AggregationType::Skewness>() ||
        std::is_same<AggType, AggregationType::Stddev>() ||
        std::is_same<AggType, AggregationType::Var>();

    /// Whether the intermediate aggregation relies on sum_cubed()
    constexpr static bool needs_sum_cubed_ =
        std::is_same<AggType, AggregationType::Skewness>();

    /// Whether the intermediate aggregation relies on sum_squared()
    constexpr static bool needs_sum_squared_ =
        std::is_same<AggType, AggregationType::Skewness>() ||
        std::is_same<AggType, AggregationType::Stddev>() ||
        std::is_same<AggType, AggregationType::Var>();
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <typename AggType>
void IntermediateAggregation<AggType>::calculate_counts(
    const std::vector<AUTOSQL_INT>& _indices_agg )
{
    for ( auto ix_agg : _indices_agg )
        {
            // Since count() cannot change and also cannot
            // be zero, it needs to be recalculated if
            // and only if it is zero.
            if ( count()[ix_agg] == 0.0 )
                {
                    count()[ix_agg] = index().get_count( ix_agg );
                }
        }
}

// ----------------------------------------------------------------------------

template <typename AggType>
void IntermediateAggregation<AggType>::commit()
{
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
            yhat_committed()[i] = yhat_stored()[i] = yhat()[i];
        }

    // --------------------------------------------------

    assert( updates_current().size() == 0 );

    updates_stored().clear();

    parent().commit();

    // --------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename AggType>
void IntermediateAggregation<AggType>::init_yhat(
    const std::vector<AUTOSQL_FLOAT>& _yhat,
    const containers::IntSet& _indices )
{
    debug_log( "IntermediateAgg: init_yhat..." );

    assert( updates_current().size() == 0 );

    for ( auto ix_input : _indices )
        {
            const std::vector<AUTOSQL_INT> indices_agg =
                index().transform( ix_input );

            if ( needs_count_ )
                {
                    calculate_counts( indices_agg );
                }

            for ( auto ix_agg : indices_agg )
                {
                    update_sample( ix_agg, _yhat[ix_input], 0.0 );
                }

            for ( auto ix_agg : indices_agg )
                {
                    updates_stored().insert( ix_agg );
                }
        }

    parent().init_yhat( yhat(), updates_stored() );

    for ( AUTOSQL_INT ix_agg : updates_stored() )
        {
            yhat_stored()[ix_agg] = yhat()[ix_agg];
        }

    debug_log( "IntermediateAgg: init_yhat...done" );
}

// ----------------------------------------------------------------------------

template <typename AggType>
void IntermediateAggregation<AggType>::reset()
{
    // --------------------------------------------------

    if ( needs_count_ )
        {
            std::fill( count().begin(), count().end(), 0.0 );
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

    std::fill( yhat().begin(), yhat().end(), 0.0 );

    std::fill( yhat_stored().begin(), yhat_stored().end(), 0.0 );

    std::fill( yhat_committed().begin(), yhat_committed().end(), 0.0 );

    // --------------------------------------------------

    updates_current().clear();

    updates_stored().clear();

    parent().reset();

    // --------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename AggType>
void IntermediateAggregation<AggType>::revert_to_commit()
{
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
            yhat()[i] = yhat_stored()[i] = yhat_committed()[i];
        }

    // --------------------------------------------------

    assert( updates_current().size() == 0 );

    updates_stored().clear();

    parent().revert_to_commit();

    // --------------------------------------------------
}

// ----------------------------------------------------------------------------

template <typename AggType>
void IntermediateAggregation<AggType>::update_samples(
    const containers::IntSet& _indices,
    const std::vector<AUTOSQL_FLOAT>& _new_values,
    const std::vector<AUTOSQL_FLOAT>& _old_values )
{
    for ( auto ix_input : _indices )
        {
            const std::vector<AUTOSQL_INT> indices_agg =
                index().transform( ix_input );

            for ( auto ix_agg : indices_agg )
                {
                    update_sample(
                        ix_agg, _new_values[ix_input], _old_values[ix_input] );
                }

            for ( auto ix_agg : indices_agg )
                {
                    updates_current().insert( ix_agg );
                    updates_stored().insert( ix_agg );
                }
        }

    parent().update_samples( updates_current(), yhat(), yhat_stored() );

    for ( AUTOSQL_INT ix_agg : updates_current() )
        {
            yhat_stored()[ix_agg] = yhat()[ix_agg];
        }

    updates_current().clear();
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace autosql

#endif  // AUTOSQL_INTERMEDIATEAGGREGATION_HPP_
