#ifndef RELBOOST_AGGREGATIONS_AGGREGATIONIMPL_HPP_
#define RELBOOST_AGGREGATIONS_AGGREGATIONIMPL_HPP_

namespace relboost
{
namespace aggregations
{
// -------------------------------------------------------------------------

/// AggregationImpl implements helper functions that are needed
/// by all aggregations.
class AggregationImpl
{
    // -----------------------------------------------------------------

   public:
    AggregationImpl(
        lossfunctions::LossFunction* _child,
        std::vector<Float>* _eta1,
        std::vector<Float>* _eta2,
        containers::IntSet* _indices,
        containers::IntSet* _indices_current )
        : child_( _child ),
          eta1_( *_eta1 ),
          eta2_( *_eta2 ),
          indices_( *_indices ),
          indices_current_( *_indices_current )
    {
    }

    ~AggregationImpl() = default;

    // -----------------------------------------------------------------

   public:
    /// Commits the _weights.
    void commit( const std::array<Float, 3>& _weights );

    /// Returns the loss reduction associated with a split.
    Float evaluate_split(
        const Float _old_intercept,
        const Float _old_weight,
        const std::array<Float, 3>& _weights );

    /// Resets the critical resources to zero.
    void reset();

    /// Resizes critical resources.
    void resize( size_t _size );

    /// Reverts the weights to the last time commit has been called.
    void revert_to_commit();

    // -----------------------------------------------------------------

   private:
    /// Either The next higher level of aggregation or the loss function.
    lossfunctions::LossFunction* const child_;

    /// Parameters for weight 1.
    std::vector<Float>& eta1_;

    /// Parameters for weight 2.
    std::vector<Float>& eta2_;

    /// Keeps track of the samples that have been altered.
    containers::IntSet& indices_;

    /// Keeps track of the samples that have been altered since the last split.
    containers::IntSet& indices_current_;
};

// -------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relboost

#endif  // RELBOOST_AGGREGATIONS_AGGREGATIONIMPL_HPP_
