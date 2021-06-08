#ifndef MULTIREL_ENSEMBLE_DECISIONTREEENSEMBLEIMPL_HPP_
#define MULTIREL_ENSEMBLE_DECISIONTREEENSEMBLEIMPL_HPP_

namespace multirel
{
namespace ensemble
{
// ----------------------------------------------------------------------------
// The purpose of this impl struct is to reduce maintenance cost
// for the default copy constructors and move constructors

struct DecisionTreeEnsembleImpl
{
    DecisionTreeEnsembleImpl(
        const std::shared_ptr<const descriptors::Hyperparameters>
            &_hyperparameters,
        const std::shared_ptr<const std::vector<std::string>> &_peripheral,
        const std::shared_ptr<const containers::Placeholder> &_placeholder )
        : allow_http_( false ),
          comm_( nullptr ),
          hyperparameters_( _hyperparameters ),
          peripheral_( _peripheral ),
          placeholder_( _placeholder )
    {
    }

    ~DecisionTreeEnsembleImpl() = default;

    // --------------------------------------

    /// Whether we want to allow this model to be used as an http endpoint.
    bool allow_http_;

    /// MPI Communicator or self-defined communicator object (for
    /// multithreading)
    multithreading::Communicator *comm_;

    /// The hyperparameters used in this ensemble
    std::shared_ptr<const descriptors::Hyperparameters> hyperparameters_;

    /// Names of the peripheral tables.
    std::shared_ptr<const std::vector<std::string>> peripheral_;

    /// containers::Placeholder for the peripheral tables (only used
    /// for the schema)
    std::shared_ptr<const std::vector<helpers::Schema>> peripheral_schema_;

    /// containers::Placeholder for the population table (contains the
    /// relational tree)
    std::shared_ptr<const containers::Placeholder> placeholder_;

    /// The schema of the population table.
    std::shared_ptr<const helpers::Schema> population_schema_;

    /// Random number generator for creating sample weights and
    /// the like.
    containers::Optional<std::mt19937> random_number_generator_;

    /// Names of the target variables
    std::vector<std::string> targets_;

    /// The decision trees that are part of this ensemble -
    /// each represents one feature
    std::vector<decisiontrees::DecisionTree> trees_;
};

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace multirel

#endif  // MULTIREL_ENSEMBLE_DECISIONTREEENSEMBLEIMPL_HPP_
