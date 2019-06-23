#ifndef PREDICTORS_XGBOOSTPREDICTOR_HPP_
#define PREDICTORS_XGBOOSTPREDICTOR_HPP_

namespace predictors
{
// ----------------------------------------------------------------------

/// Abstract base class for a predictor
class XGBoostPredictor : public Predictor
{
    // -----------------------------------------

    /// Defines type BoosterDestructor as a function pointer
    /// to a function of type void( BoosterHandle* ).
    typedef void ( *BoosterDestructor )( BoosterHandle* );

    /// Defines type DMatrixDestructor as a function pointer
    /// to a function of type void( DMatrixHandle* ).
    typedef void ( *DMatrixDestructor )( DMatrixHandle* );

    // -----------------------------------------

   public:
    XGBoostPredictor( const XGBoostHyperparams& _hyperparams )
        : hyperparams_( _hyperparams )
    {
    }

    ~XGBoostPredictor() = default;

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
        const std::vector<CFloatColumn>& _X,
        const CFloatColumn& _y ) final;

    /// Implements the predict(...) method in scikit-learn style
    CFloatColumn predict( const std::vector<CFloatColumn>& _X ) const final;

    /// Saves the predictor
    void save( const std::string& _fname ) const final;

    // -----------------------------------------

   private:
    /// Frees a Booster pointer
    static void delete_booster( DMatrixHandle* _ptr )
    {
        XGBoosterFree( *_ptr );
        delete _ptr;
    };

    /// Frees a DMatrixHandle pointer
    static void delete_dmatrix( DMatrixHandle* _ptr )
    {
        XGDMatrixFree( *_ptr );
        delete _ptr;
    };

    /// Returns size of the underlying model
    const bst_ulong len() const
    {
        return static_cast<bst_ulong>( model_.size() );
    }

    /// Returns reference to the underlying model
    const char* model() const
    {
        assert( model_.size() > 0 );
        return model_.data();
    }

    // -----------------------------------------

    /// Allocates the booster
    std::unique_ptr<BoosterHandle, XGBoostPredictor::BoosterDestructor>
    allocate_booster( const DMatrixHandle _dmats[], bst_ulong _len ) const;

    /// Convert matrix _mat to a DMatrixHandle
    std::unique_ptr<DMatrixHandle, DMatrixDestructor> convert_to_dmatrix(
        const std::vector<CFloatColumn>& _X ) const;

    /// Extracts feature importances from XGBoost dump
    void parse_dump(
        const std::string& _dump,
        std::vector<Float>* _feature_importances ) const;

    // -----------------------------------------

   private:
    /// The underlying XGBoost model, expressed in bytes
    std::vector<char> model_;

    /// Hyperparameters for XGBoostPredictor
    const XGBoostHyperparams hyperparams_;
};

// ------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_XGBOOSTPREDICTOR_HPP_
