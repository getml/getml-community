#ifndef RELBOOST_ENSEMBLE_DECISIONTREEENSEMBLEIMPL_HPP_
#define RELBOOST_ENSEMBLE_DECISIONTREEENSEMBLEIMPL_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace ensemble
{
// ------------------------------------------------------------------------

struct DecisionTreeEnsembleImpl
{
    DecisionTreeEnsembleImpl(
        const std::shared_ptr<const std::vector<std::string>>& _encoding,
        const std::shared_ptr<const Hyperparameters>& _hyperparameters,
        const std::shared_ptr<const std::vector<std::string>>&
            _peripheral_names,
        const std::shared_ptr<const Placeholder>& _placeholder )
        : comm_( nullptr ),
          encoding_( _encoding ),
          hyperparameters_( _hyperparameters ),
          initial_prediction_( 0.0 ),
          peripheral_names_( _peripheral_names ),
          placeholder_( _placeholder ),
          sampler_( utils::Sampler( _hyperparameters->seed_ ) )
    {
        check_placeholder( *placeholder_ );
    }

    ~DecisionTreeEnsembleImpl() = default;

    // ------------------------------------------------------------------------

    /// Makes sure that all peripheral tables exist.
    void check_placeholder( const Placeholder& _placeholder ) const;

    // ------------------------------------------------------------------------

    /// raw pointer to the communicator.
    multithreading::Communicator* comm_;

    /// Encoding for the categorical data, maps integers to underlying
    /// category.
    std::shared_ptr<const std::vector<std::string>> encoding_;

    /// Hyperparameters used to train the relboost model.
    std::shared_ptr<const Hyperparameters> hyperparameters_;

    /// The prediction that we start with before there are any trees (identical
    /// to the average of the targets in the training set).
    RELBOOST_FLOAT initial_prediction_;

    /// Names of the peripheral tables, as they are referred in placeholder
    std::shared_ptr<const std::vector<std::string>> peripheral_names_;

    /// Placeholder object used to define the data schema.
    std::shared_ptr<const Placeholder> placeholder_;

    /// The sampler used for making the sample weights.
    utils::Sampler sampler_;

    /// Contains information on how this ensemble has been scored
    containers::Scores scores_;

    /// Trees underlying the model.
    std::vector<decisiontrees::DecisionTree> trees_;

    /// Prediction of all previous tress in the ensemble
    std::vector<RELBOOST_FLOAT> yhat_old_;
};

// ------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_ENSEMBLE_DECISIONTREEENSEMBLEIMPL_HPP_