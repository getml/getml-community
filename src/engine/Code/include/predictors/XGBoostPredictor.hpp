#ifndef PREDICTORS_XGBOOSTPREDICTOR_HPP_
#define PREDICTORS_XGBOOSTPREDICTOR_HPP_

namespace predictors
{
// ----------------------------------------------------------------------

/// Implements the XGBoostPredictors
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
    XGBoostPredictor(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<const PredictorImpl>& _impl,
        const std::vector<Poco::JSON::Object::Ptr>& _dependencies )
        : cmd_( _cmd ),
          dependencies_( _dependencies ),
          hyperparams_( XGBoostHyperparams( _cmd ) ),
          impl_( _impl )
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

    /// Returns the fingerprint of the predictor (necessary to build
    /// the dependency graphs).
    Poco::JSON::Object::Ptr fingerprint() const final;

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

   public:
    /// Whether the predictor accepts null values.
    bool accepts_null() const final { return false; }

    /// Returns a deep copy.
    std::shared_ptr<Predictor> clone() const final
    {
        return std::make_shared<XGBoostPredictor>( *this );
    }

    /// Whether the predictor is used for classification;
    bool is_classification() const final
    {
        return hyperparams_.objective_ == "reg:logistic" ||
               hyperparams_.objective_ == "binary:logistic" ||
               hyperparams_.objective_ == "binary:logitraw";
    }

    /// Whether the predictor has been fitted.
    bool is_fitted() const final { return len() > 0; }

    /// Whether we want the predictor to be silent.
    bool silent() const final { return hyperparams_.silent_; }

    // -----------------------------------------

   private:
    /// Frees a Booster pointer
    static void delete_booster( BoosterHandle* _ptr )
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

    /// Trivial (private) accessor.
    const PredictorImpl& impl() const
    {
        assert_true( impl_ );
        return *impl_;
    }

    /// Returns size of the underlying model
    const bst_ulong len() const
    {
        return static_cast<bst_ulong>( model_.size() );
    }

    /// Returns reference to the underlying model
    const char* model() const
    {
        assert_true( model_.size() > 0 );
        return model_.data();
    }

    // -----------------------------------------

    /// Allocates the booster
    std::unique_ptr<BoosterHandle, XGBoostPredictor::BoosterDestructor>
    allocate_booster( const DMatrixHandle _dmats[], bst_ulong _len ) const;

    /// Convert matrix _mat to a DMatrixHandle
    std::unique_ptr<DMatrixHandle, DMatrixDestructor> convert_to_dmatrix(
        const std::vector<CIntColumn>& _X_categorical,
        const std::vector<CFloatColumn>& _X_numerical ) const;

    /// Convert matrix _mat to a dense DMatrixHandle
    std::unique_ptr<DMatrixHandle, DMatrixDestructor> convert_to_dmatrix_dense(
        const std::vector<CFloatColumn>& _X_numerical ) const;

    /// Convert matrix _mat to a sparse DMatrixHandle
    std::unique_ptr<DMatrixHandle, DMatrixDestructor> convert_to_dmatrix_sparse(
        const std::vector<CIntColumn>& _X_categorical,
        const std::vector<CFloatColumn>& _X_numerical ) const;

    /// Extracts feature importances from XGBoost dump
    void parse_dump(
        const std::string& _dump,
        std::vector<Float>* _feature_importances ) const;

    // -----------------------------------------

   private:
    /// The JSON command used to construct this predictor.
    const Poco::JSON::Object cmd_;

    /// The dependencies used to build the fingerprint.
    std::vector<Poco::JSON::Object::Ptr> dependencies_;

    /// Hyperparameters for XGBoostPredictor
    const XGBoostHyperparams hyperparams_;

    /// Implementation class for member functions common to most predictors.
    std::shared_ptr<const PredictorImpl> impl_;

    /// The underlying XGBoost model, expressed in bytes
    std::vector<char> model_;
};

// ------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_XGBOOSTPREDICTOR_HPP_
