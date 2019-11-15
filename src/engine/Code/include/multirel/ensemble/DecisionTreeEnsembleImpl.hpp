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
        const std::shared_ptr<const std::vector<strings::String>> &_categories,
        const std::shared_ptr<const descriptors::Hyperparameters>
            &_hyperparameters,
        const std::vector<std::string> &_placeholder_peripheral,
        const decisiontrees::Placeholder &_placeholder_population )
        : categories_( _categories ),
          comm_( nullptr ),
          hyperparameters_( _hyperparameters ),
          placeholder_peripheral_( _placeholder_peripheral ),
          placeholder_population_(
              new decisiontrees::Placeholder( _placeholder_population ) ){};

    ~DecisionTreeEnsembleImpl() = default;

    // --------------------------------------

    /// Pimpl for aggregation
    containers::Optional<aggregations::AggregationImpl> aggregation_impl_;

    /// Vector containing the names of the categories. It is used
    /// for generating the SQL code, because categorical data is
    /// stored in the form of integers, whereas we want actual categories
    /// in our code.
    std::shared_ptr<const std::vector<strings::String>> categories_;

    /// MPI Communicator or self-defined communicator object (for
    /// multithreading)
    multithreading::Communicator *comm_;

    /// The hyperparameters used in this ensemble
    std::shared_ptr<const descriptors::Hyperparameters> hyperparameters_;

    /// Schema of the peripheral tables.
    std::shared_ptr<const std::vector<containers::Schema>> peripheral_schema_;

    /// decisiontrees::Placeholder for the peripheral tables
    std::vector<std::string> placeholder_peripheral_;

    /// decisiontrees::Placeholder for the population table
    containers::Optional<decisiontrees::Placeholder> placeholder_population_;

    /// Schema of the population table.
    std::shared_ptr<const containers::Schema> population_schema_;

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
