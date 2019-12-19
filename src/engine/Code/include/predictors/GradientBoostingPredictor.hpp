#ifndef PREDICTORS_GRADIENTBOOSTINGPREDICTOR_HPP_
#define PREDICTORS_GRADIENTBOOSTINGPREDICTOR_HPP_

namespace predictors
{
// ----------------------------------------------------------------------

/// Implements a gradient boosting algorithm using
/// relboost
class GradientBoostingPredictor : public Predictor
{
    // -----------------------------------------

   public:
    typedef relboost::ensemble::DecisionTreeEnsemble::DataFrameType
        DataFrameType;

    // -----------------------------------------

   public:
    GradientBoostingPredictor(
        const Poco::JSON::Object& _hyperparams,
        const std::shared_ptr<const PredictorImpl>& _impl,
        const std::shared_ptr<const std::vector<strings::String>>& _categories )
        : impl_( _impl ),
          model_( relboost::ensemble::DecisionTreeEnsemble(
              _categories,
              std::make_shared<const relboost::Hyperparameters>( _hyperparams ),
              nullptr,
              nullptr,
              nullptr,
              nullptr ) )
    {
    }

    ~GradientBoostingPredictor() = default;

    // -----------------------------------------

   public:
    /// Returns an importance measure for the individual features
    std::vector<Float> feature_importances(
        const size_t _num_features ) const final;

    /// Loads the predictor
    void load( const std::string& _fname ) final;

    /// Implements the fit(...) method in scikit-learn style
    std::string fit(
        const std::shared_ptr<const logging::AbstractLogger> _logger,
        const std::vector<CIntColumn>& _X_categorical,
        const std::vector<CFloatColumn>& _X_numerical,
        const CFloatColumn& _y ) final;

    /// Implements the predict(...) method in scikit-learn style
    CFloatColumn predict(
        const std::vector<CIntColumn>& _X_categorical,
        const std::vector<CFloatColumn>& _X_numerical ) const final;

    /// Saves the predictor
    void save( const std::string& _fname ) const final;

    // -------------------------------------------------------------------------

   private:
    /// Turns the input columns into a data frame that can be processed
    /// by the underlying algorithm.
    DataFrameType extract_df(
        const std::vector<CIntColumn>& _X_categorical,
        const std::vector<CFloatColumn>& _X_numerical,
        const std::optional<CFloatColumn>& _y ) const;

    /// Loads a JSON object.
    Poco::JSON::Object load_json_obj( const std::string& _fname ) const;

    // -------------------------------------------------------------------------

   public:
    /// Whether the predictor accepts null values.
    bool accepts_null() const final { return true; }

    // -----------------------------------------

   private:
    /// Trivial (private) accessor.
    const PredictorImpl& impl() const
    {
        assert_true( impl_ );
        return *impl_;
    }

    /// Trivial (private) accessor.
    relboost::ensemble::DecisionTreeEnsemble& model() { return model_; }

    /// Trivial (private) accessor.
    const relboost::ensemble::DecisionTreeEnsemble& model() const
    {
        return model_;
    }

    // -----------------------------------------

   private:
    /// Implementation class for member functions common to most predictors.
    std::shared_ptr<const PredictorImpl> impl_;

    /// The underlying relboost model
    relboost::ensemble::DecisionTreeEnsemble model_;
};

// ------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_GRADIENTBOOSTINGPREDICTOR_HPP_

