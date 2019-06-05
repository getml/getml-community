#ifndef AUTOSQL_DECISIONTREES_DECISIONTREEENSEMBLEIMPL_HPP_
#define AUTOSQL_DECISIONTREES_DECISIONTREEENSEMBLEIMPL_HPP_

namespace autosql
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------
// The purpose of this impl struct is to reduce maintenance cost
// for the default copy constructors and move constructors

struct DecisionTreeEnsembleImpl
{
    DecisionTreeEnsembleImpl()
        : categories_( std::make_shared<containers::Encoding>() ),
          comm_( nullptr ){};

    DecisionTreeEnsembleImpl(
        const std::shared_ptr<containers::Encoding> &_categories )
        : categories_( _categories ), comm_( nullptr ){};

    DecisionTreeEnsembleImpl(
        const std::shared_ptr<containers::Encoding> &_categories,
        const std::vector<std::string> &_placeholder_peripheral,
        const Placeholder &_placeholder_population )
        : categories_( _categories ),
          placeholder_peripheral_( _placeholder_peripheral ),
          placeholder_population_(
              new Placeholder( _placeholder_population ) ){};

    ~DecisionTreeEnsembleImpl() = default;

    // --------------------------------------

    /// Pimpl for aggregation
    containers::Optional<aggregations::AggregationImpl> aggregation_impl_;

    /// Vector containing the names of the categories. It is used
    /// for generating the SQL code, because categorical data is
    /// stored in the form of integers, whereas we want actual categories
    /// in our code.
    std::shared_ptr<containers::Encoding> categories_;

    /// MPI Communicator or self-defined communicator object (for
    /// multithreading)
    AUTOSQL_COMMUNICATOR *comm_;

    /// The linear regressions that are being used to map the features
    /// onto the targets for the gradient-boosting-like functionality
    std::vector<LinearRegression> linear_regressions_;

    /// The hyperparameters used in this ensemble
    containers::Optional<descriptors::Hyperparameters> hyperparameters_;

    /// Number of categorical colimns in peripheral table
    std::vector<AUTOSQL_INT> num_columns_peripheral_categorical_;

    /// Number of discrete colimns in peripheral table
    std::vector<AUTOSQL_INT> num_columns_peripheral_discrete_;

    /// Number of numerical colimns in peripheral table
    std::vector<AUTOSQL_INT> num_columns_peripheral_numerical_;

    /// Number of categorical colimns in population table
    AUTOSQL_INT num_columns_population_categorical_;

    /// Number of discrete colimns in population table
    AUTOSQL_INT num_columns_population_discrete_;

    /// Number of numerical colimns in population table
    AUTOSQL_INT num_columns_population_numerical_;

    /// Placeholder for the peripheral tables
    std::vector<std::string> placeholder_peripheral_;

    /// Placeholder for the population table
    containers::Optional<Placeholder> placeholder_population_;

    /// Predictors to be trained on the features (one for every target)
    std::vector<std::shared_ptr<predictors::Predictor>> predictors_;

    /// Random number generator for creating sample weights and
    /// the like.
    containers::Optional<std::mt19937> random_number_generator_;

    /// Contains information on how this ensemble has been scored
    descriptors::Scores scores_;

    /// Names of the target variables
    std::vector<std::string> targets_;

    /// The decision trees that are part of this ensemble -
    /// each represents one feature
    std::vector<DecisionTree> trees_;
};

// ----------------------------------------------------------------------------
}
}
#endif  // AUTOSQL_DECISIONTREES_DECISIONTREEENSEMBLEIMPL_HPP_
