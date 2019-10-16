#ifndef RELBOOST_AGGREGATIONS_INTERMEDIATEAGGREGATIONIMPL_HPP_
#define RELBOOST_AGGREGATIONS_INTERMEDIATEAGGREGATIONIMPL_HPP_

namespace relboost
{
namespace aggregations
{
// -------------------------------------------------------------------------

/// IntermediateAggregationImpl implements helper functions that are needed
/// by all aggregations when they are used as IntermediateAggregations.
class IntermediateAggregationImpl
{
    // -----------------------------------------------------------------

   public:
    IntermediateAggregationImpl(
        const std::shared_ptr<AggregationIndex>& _agg_index )
        : agg_index_( _agg_index ),
          indices_( containers::IntSet( 0 ) ),
          indices_current_( containers::IntSet( 0 ) )
    {
        eta1_ = std::vector<Float>( agg_index().nrows() );
        eta1_2_null_ = std::vector<Float>( agg_index().nrows() );
        eta1_2_null_old_ = std::vector<Float>( agg_index().nrows() );
        eta1_old_ = std::vector<Float>( agg_index().nrows() );

        eta2_ = std::vector<Float>( agg_index().nrows() );
        eta2_1_null_ = std::vector<Float>( agg_index().nrows() );
        eta2_1_null_old_ = std::vector<Float>( agg_index().nrows() );
        eta2_old_ = std::vector<Float>( agg_index().nrows() );

        w_fixed_1_ = std::vector<Float>( agg_index().nrows() );
        w_fixed_1_old_ = std::vector<Float>( agg_index().nrows() );
        w_fixed_2_ = std::vector<Float>( agg_index().nrows() );
        w_fixed_2_old_ = std::vector<Float>( agg_index().nrows() );

        indices_ = containers::IntSet( agg_index().nrows() );
        indices_current_ = containers::IntSet( agg_index().nrows() );
    }

    ~IntermediateAggregationImpl() = default;

    // -----------------------------------------------------------------

   public:
    /// Calculates the etas needed.
    std::tuple<
        const std::vector<Float>*,
        const std::vector<Float>*,
        const std::vector<Float>*,
        const std::vector<Float>*>
    calc_etas(
        const enums::Aggregation _agg,
        const Float _old_weight,
        const std::vector<size_t>& _indices,
        const std::vector<size_t>& _indices_current,
        const std::vector<Float>& _eta1,
        const std::vector<Float>& _eta1_old,
        const std::vector<Float>& _eta2,
        const std::vector<Float>& _eta2_old );

    /// Update the "old" values.
    void update_etas_old( const enums::Aggregation _agg );

    // -----------------------------------------------------------------

   public:
    /// Trivial (const) accessor.
    const std::vector<size_t>& indices() const
    {
        return indices_.unique_integers();
    }

    /// Trivial (const) accessor.
    const std::vector<size_t>& indices_current() const
    {
        return indices_current_.unique_integers();
    }

    // -----------------------------------------------------------------

   private:
    /// Update the output.
    void update_etas(
        const std::vector<size_t>& _indices_current,
        const std::vector<Float>& _eta1_input,
        const std::vector<Float>& _eta1_input_old,
        const std::vector<Float>& _eta2_input,
        const std::vector<Float>& _eta2_input_old,
        std::vector<Float>* _eta1_output,
        std::vector<Float>* _eta2_output );

    // -----------------------------------------------------------------

   private:
    /// Trivial (private) accessor
    AggregationIndex& agg_index()
    {
        assert_true( agg_index_ );
        return *agg_index_;
    }

    // -----------------------------------------------------------------

   private:
    std::shared_ptr<AggregationIndex> agg_index_;

    /// Parameters for weight 1.
    std::vector<Float> eta1_;

    /// Parameters for weight 1 when weight 2 is NULL.
    std::vector<Float> eta1_2_null_;

    /// Parameters for weight 1 when weight 2 is NULL as of the last split.
    std::vector<Float> eta1_2_null_old_;

    /// Parameters for weight 1 as of the last split.
    std::vector<Float> eta1_old_;

    /// Parameters for weight 2.
    std::vector<Float> eta2_;

    /// Parameters for weight 2 when weight 1 is NULL.
    std::vector<Float> eta2_1_null_;

    /// Parameters for weight 1 when weight 2 is NULL as of the last split.
    std::vector<Float> eta2_1_null_old_;

    /// Parameters for weight 2 as of the last split.
    std::vector<Float> eta2_old_;

    /// Keeps track of the samples that have been altered.
    containers::IntSet indices_;

    /// Keeps track of the samples that have been altered since the last split.
    containers::IntSet indices_current_;

    /// The fixed weights when weight 2 is NULL.
    std::vector<Float> w_fixed_1_;

    /// The fixed weights when weight 2 is NULL as of the last split.
    std::vector<Float> w_fixed_1_old_;

    /// The fixed weights when weight 1 is NULL.
    std::vector<Float> w_fixed_2_;

    /// The fixed weights when weight 1 is NULL as of the last split.
    std::vector<Float> w_fixed_2_old_;
};

// -------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relboost

#endif  // RELBOOST_AGGREGATIONS_INTERMEDIATEAGGREGATIONIMPL_HPP_
