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
        const std::shared_ptr<const Hyperparameters>& _hyperparameters,
        const std::shared_ptr<const std::vector<std::string>>& _peripheral,
        const std::shared_ptr<const containers::Placeholder>& _placeholder,
        const std::shared_ptr<const std::vector<containers::Placeholder>>&
            _peripheral_schema,
        const std::shared_ptr<const containers::Placeholder>&
            _population_schema )
        : allow_http_( false ),
          comm_( nullptr ),
          hyperparameters_( _hyperparameters ),
          initial_prediction_( 0.0 ),
          peripheral_( _peripheral ),
          peripheral_schema_( _peripheral_schema ),
          placeholder_( _placeholder ),
          population_schema_( _population_schema )
    {
        if ( placeholder_ )
            {
                check_placeholder( *placeholder_ );
            }
    }

    ~DecisionTreeEnsembleImpl() = default;

    // ------------------------------------------------------------------------

    /// Makes sure that all peripheral tables exist.
    void check_placeholder( const containers::Placeholder& _placeholder ) const;

    // ------------------------------------------------------------------------

    /// Whether we want to allow this model to be used as an http endpoint.
    bool allow_http_;

    /// raw pointer to the communicator.
    multithreading::Communicator* comm_;

    /// Hyperparameters used to train the relboost model.
    std::shared_ptr<const Hyperparameters> hyperparameters_;

    /// The prediction that we start with before there are any trees (identical
    /// to the average of the targets in the training set).
    Float initial_prediction_;

    /// Used to map columns onto the average target value.
    std::shared_ptr<const helpers::MappingContainer> mappings_;

    /// Names of the peripheral tables, as they are referred in placeholder
    std::shared_ptr<const std::vector<std::string>> peripheral_;

    /// Schema of the peripheral tables.
    std::shared_ptr<const std::vector<containers::Placeholder>>
        peripheral_schema_;

    /// Placeholder object used to define the data schema.
    std::shared_ptr<const containers::Placeholder> placeholder_;

    /// Schema of the population table.
    std::shared_ptr<const containers::Placeholder> population_schema_;

    /// Trees underlying the model.
    std::vector<decisiontrees::DecisionTree> trees_;

    // The vocabulary used to analyze the text fields.
    std::shared_ptr<const helpers::VocabularyContainer> vocabulary_;

    /// Prediction of all previous tress in the ensemble
    std::vector<Float> yhat_old_;
};

// ------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_ENSEMBLE_DECISIONTREEENSEMBLEIMPL_HPP_
