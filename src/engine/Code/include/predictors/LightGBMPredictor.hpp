#ifndef PREDICTORS_LIGHTGBMPREDICTOR_HPP_
#define PREDICTORS_LIGHTGBMPREDICTOR_HPP_

namespace predictors
{
// ----------------------------------------------------------------------

/// Implements the LightGBMPredictors
class LightGBMPredictor : public Predictor
{
    // -----------------------------------------

    /// Defines type BoosterDestructor as a function pointer
    /// to a function of type void( BoosterHandle* ).
    typedef void ( *BoosterDestructor )( BoosterHandle* );

    /// Defines type DatasetDestructor as a function pointer
    /// to a function of type void( DatasetHandle* ).
    typedef void ( *DatasetDestructor )( DatasetHandle* );

    // -----------------------------------------

   public:
    LightGBMPredictor(
        const Poco::JSON::Object& _hyperparams,
        const std::shared_ptr<const PredictorImpl>& _impl )
        : hyperparams_( LightGBMHyperparams( _hyperparams ) ), impl_( _impl )
    {
        hyperparam_string_ = make_hyperparam_string();
    }

    ~LightGBMPredictor() = default;

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

   public:
    /// Whether the predictor accepts null values.
    bool accepts_null() const final { return false; }

    // -----------------------------------------

   private:
    /// Frees a Booster pointer
    static void delete_booster( BoosterHandle* _ptr )
    {
        LGBM_BoosterFree( *_ptr );
        delete _ptr;
    };

    /// Frees a DMatrixHandle pointer
    static void delete_dmatrix( DatasetHandle* _ptr )
    {
        LGBM_DatasetFree( *_ptr );
        delete _ptr;
    };

    /// Trivial (private) accessor.
    const PredictorImpl& impl() const
    {
        assert_true( impl_ );
        return *impl_;
    }

    // -----------------------------------------

   private:
    /// Allocates the booster
    std::shared_ptr<BoosterHandle> allocate_booster(
        const DatasetHandle& _training_set, bst_ulong _len ) const;

    /// Converts the dataset to a set of dense matrices.
    std::vector<float> convert_to_dense_matrix(
        const std::vector<CFloatColumn>& _X_numerical ) const;

    /// Convert matrix _mat to a DatasetHandle
    /*std::unique_ptr<DatasetHandle, LightGBMPredictor::DatasetDestructor>
    convert_to_dataset(
        const std::vector<CIntColumn>& _X_categorical,
        const std::vector<CFloatColumn>& _X_numerical ) const;*/

    /// Convert matrix _mat to a dense DatasetHandle
    std::unique_ptr<DatasetHandle, LightGBMPredictor::DatasetDestructor>
    convert_to_dataset_dense(
        const std::vector<CFloatColumn>& _X_numerical,
        const std::vector<float>& mat ) const;

    /// Convert matrix _mat to a sparse DatasetHandle
    std::unique_ptr<DatasetHandle, LightGBMPredictor::DatasetDestructor>
    convert_to_dataset_sparse(
        const std::vector<CIntColumn>& _X_categorical,
        const std::vector<CFloatColumn>& _X_numerical ) const;

    /// Recreates an actual booster from model_.
    std::unique_ptr<BoosterHandle, LightGBMPredictor::BoosterDestructor>
    load_booster_from_string() const;

    /// Turns the hyperparameters into a string that can be read by the LightGBM
    /// model.
    std::string make_hyperparam_string() const;

    // -----------------------------------------

   private:
    /// Hyperparameters for LightGBMPredictor
    const LightGBMHyperparams hyperparams_;

    /// The hyperparameters expressed as a string.
    std::string hyperparam_string_;

    /// Implementation class for member functions common to most predictors.
    std::shared_ptr<const PredictorImpl> impl_;

    /// The underlying LightGBM model.
    std::shared_ptr<BoosterHandle> model_;
};

// ------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_LIGHTGBMPREDICTOR_HPP_
