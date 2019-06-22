#ifndef RELBOOST_DECISIONTREES_DECISIONTREE_HPP_
#define RELBOOST_DECISIONTREES_DECISIONTREE_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace decisiontrees
{
// ------------------------------------------------------------------------

class DecisionTree
{
   public:
    DecisionTree(
        const std::shared_ptr<const std::vector<std::string>>& _encoding,
        const std::shared_ptr<const Hyperparameters>& _hyperparameters,
        const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
        const size_t _peripheral_used,
        multithreading::Communicator* _comm );

    DecisionTree(
        const std::shared_ptr<const std::vector<std::string>>& _encoding,
        const std::shared_ptr<const Hyperparameters>& _hyperparameters,
        const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
        const Poco::JSON::Object& _obj );

    ~DecisionTree() = default;

    // -----------------------------------------------------------------

    /// Fits the decision tree.
    void fit(
        const containers::DataFrameView& _output,
        const containers::DataFrame& _input,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end );

    /// Expresses DecisionTree as Poco::JSON::Object.
    Poco::JSON::Object to_json_obj() const;

    /// Transforms the data to form a prediction.
    std::vector<Float> transform(
        const containers::DataFrameView& _output,
        const containers::DataFrame& _input ) const;

    /// Expresses the decision tree as SQL code.
    std::string to_sql(
        const std::string _feature_num, const bool _use_timestamps ) const;

    // -----------------------------------------------------------------

    /// Calculates the update rate.
    void calc_update_rate(
        const std::vector<Float>& _yhat_old,
        const std::vector<Float>& _predictions )
    {
        update_rate_ =
            loss_function().calc_update_rate( _yhat_old, _predictions );
    }

    /// Trivial getter
    const Float intercept() const { return intercept_; }

    /// Trivial getter
    const size_t peripheral_used() const { return peripheral_used_; }

    /// Trivial setter.
    void set_comm( multithreading::Communicator* _comm )
    {
        comm_ = _comm;
        if ( root_ )
            {
                root_->set_comm( _comm );
            }
    }

    /// Trivial getter
    const Float update_rate() const { return update_rate_; }

    // -----------------------------------------------------------------

   private:
    /// Trivial (private) accessor.
    multithreading::Communicator& comm()
    {
        assert( comm_ != nullptr );
        return *comm_;
    }

    /// Trivial (private) accessor
    const Hyperparameters& hyperparameters()
    {
        assert( hyperparameters_ );
        return *hyperparameters_;
    }

    /// Trivial (private) accessor
    const containers::Schema& input() const
    {
        assert( input_ );
        return *input_;
    }

    /// Trivial (private) accessor
    lossfunctions::LossFunction& loss_function()
    {
        assert( loss_function_ );
        return *loss_function_;
    }

    /// Trivial (private) accessor
    const lossfunctions::LossFunction& loss_function() const
    {
        assert( loss_function_ );
        return *loss_function_;
    }

    /// Trivial (private) accessor
    const containers::Schema& output() const
    {
        assert( output_ );
        return *output_;
    }
    // -----------------------------------------------------------------

   private:
    /// raw pointer to the communicator.
    multithreading::Communicator* comm_;

    /// Encoding for the categorical data, maps integers to underlying category.
    std::shared_ptr<const std::vector<std::string>> encoding_;

    /// Hyperparameters used to train the relboost model
    std::shared_ptr<const Hyperparameters> hyperparameters_;

    /// The input table used (we keep it, because we need the colnames)
    containers::Optional<containers::Schema> input_;

    /// The intercept term that is added after aggregation.
    Float intercept_;

    /// Hyperparameters used to train the relboost model
    std::shared_ptr<lossfunctions::LossFunction> loss_function_;

    /// The output table used (we keep it, because we need the colnames)
    containers::Optional<containers::Schema> output_;

    /// The peripheral table used.
    size_t peripheral_used_;

    /// The root of the decision tree.
    containers::Optional<DecisionTreeNode> root_;

    /// The update rate that is used when this tree is added to the prediction.
    Float update_rate_;

    // -----------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_DECISIONTREES_DECISIONTREE_HPP_