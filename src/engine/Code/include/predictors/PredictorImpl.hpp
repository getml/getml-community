#ifndef PREDICTORS_PREDICTORIMPL_HPP_
#define PREDICTORS_PREDICTORIMPL_HPP_

namespace predictors
{
// ----------------------------------------------------------------------------

/// Impl class for a predictor.
class PredictorImpl
{
    // -----------------------------------------

   public:
    PredictorImpl(
        const std::vector<std::string>& _categorical_colnames,
        const std::vector<std::string>& _discrete_colnames,
        const std::vector<std::string>& _numerical_colnames,
        const size_t _num_autofeatures )
        : categorical_colnames_( _categorical_colnames ),
          discrete_colnames_( _discrete_colnames ),
          numerical_colnames_( _numerical_colnames ),
          num_autofeatures_( _num_autofeatures ){};

    PredictorImpl( const Poco::JSON::Object& _obj )
        : categorical_colnames_( JSON::array_to_vector<std::string>(
              JSON::get_array( _obj, "categorical_colnames_" ) ) ),
          discrete_colnames_( JSON::array_to_vector<std::string>(
              JSON::get_array( _obj, "discrete_colnames_" ) ) ),
          numerical_colnames_( JSON::array_to_vector<std::string>(
              JSON::get_array( _obj, "numerical_colnames_" ) ) ),
          num_autofeatures_(
              JSON::get_value<size_t>( _obj, "num_autofeatures_" ) )
    {
        auto arr = JSON::get_array( _obj, "encodings_" );

        for ( size_t i = 0; i < arr->size(); ++i )
            {
                encodings_.push_back( Encoding( *arr->getObject( static_cast<unsigned int>( i ) ) ) );
            }
    };

    ~PredictorImpl() = default;

    // -----------------------------------------

   public:
    /// Compresses importances calculated for a CSR Matrix to aggregated
    /// importances for each categorical column,
    void compress_importances(
        const std::vector<Float>& _all_feature_importances,
        std::vector<Float>* _feature_importances ) const;

    /// Makes sure that input columns passed by the user are plausible.
    size_t check_plausibility(
        const std::vector<CIntColumn>& _X_categorical,
        const std::vector<CFloatColumn>& _X_numerical ) const;

    /// Makes sure that input columns passed by the user are plausible.
    void check_plausibility(
        const std::vector<CIntColumn>& _X_categorical,
        const std::vector<CFloatColumn>& _X_numerical,
        const CFloatColumn& _y ) const;

    /// Fits the encodings.
    void fit_encodings( const std::vector<CIntColumn>& _X_categorical );

    /// Generates a CSRMatrix from the categorical and numerical columns.
    template <typename DataType, typename IndicesType, typename IndptrType>
    CSRMatrix<DataType, IndicesType, IndptrType> make_csr(
        const std::vector<CIntColumn>& _X_categorical,
        const std::vector<CFloatColumn>& _X_numerical ) const;

    /// Select the columns that have made the cut during the feature selection.
    void select_cols(
        const size_t _n_selected,
        const size_t _n_autofeatures,
        const std::vector<size_t>& _index );

    /// Saves the predictor impl as a JSON.
    void save( const std::string& _fname ) const;

    /// Transform impl to JSON object.
    Poco::JSON::Object to_json_obj() const;

    /// Transforms the columns using the encodings.
    std::vector<CIntColumn> transform_encodings(
        const std::vector<CIntColumn>& _X_categorical ) const;

    // -----------------------------------------

   public:
    /// Trivial (const) getter.
    const std::vector<std::string>& categorical_colnames() const
    {
        return categorical_colnames_;
    }

    /// Trivial (const) getter.
    const std::vector<std::string>& discrete_colnames() const
    {
        return discrete_colnames_;
    }

    /// Trivial (const) getter.
    const std::vector<std::string>& numerical_colnames() const
    {
        return numerical_colnames_;
    }

    /// Number of encodings available.
    size_t n_encodings() const { return encodings_.size(); }

    /// Trivial (const) getter.
    Int n_unique( const size_t _i ) const
    {
        assert( _i < encodings_.size() );
        return encodings_[_i].n_unique();
    }

    /// The number of columns in CSR Matrix resulting from this Impl.
    size_t ncols_csr() const
    {
        size_t ncols = num_autofeatures_ + discrete_colnames_.size() +
                       numerical_colnames_.size();

        for ( const auto& enc : encodings_ )
            {
                ncols += enc.n_unique();
            }

        return ncols;
    }

    /// Trivial (const) getter.
    const size_t num_autofeatures() const { return num_autofeatures_; }

    /// Trivial (const) getter.
    const size_t num_columns() const
    {
        return categorical_colnames_.size() + discrete_colnames_.size() +
               numerical_colnames_.size();
    }

    // -----------------------------------------

   private:
    /// Select columns that have made the cut during the feature selection.
    void select_cols(
        const size_t _n_selected,
        const std::vector<size_t>& _index,
        const size_t _ix_begin,
        std::vector<std::string>* _colnames ) const;

    /// Extracts the impl as a JSON.
    std::string to_json() const { return JSON::stringify( to_json_obj() ); }

    // -----------------------------------------

   private:
    /// Names of the categorical columns taken from the population table as
    /// features.
    std::vector<std::string> categorical_colnames_;

    /// Encodings used for the categorical columns.
    std::vector<Encoding> encodings_;

    /// Names of the discrete columns taken from the population table as
    /// features.
    std::vector<std::string> discrete_colnames_;

    /// Names of the numerical columns taken from the population table as
    /// features.
    std::vector<std::string> numerical_colnames_;

    /// The number of autofeatures used.
    size_t num_autofeatures_;

    // -----------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace predictors

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

namespace predictors
{
// ----------------------------------------------------------------------------

template <typename DataType, typename IndicesType, typename IndptrType>
CSRMatrix<DataType, IndicesType, IndptrType> PredictorImpl::make_csr(
    const std::vector<CIntColumn>& _X_categorical,
    const std::vector<CFloatColumn>& _X_numerical ) const
{
    auto csr_mat = CSRMatrix<DataType, IndicesType, IndptrType>();

    for ( const auto col : _X_numerical )
        {
            csr_mat.add( col );
        }

    for ( size_t i = 0; i < _X_categorical.size(); ++i )
        {
            csr_mat.add( _X_categorical[i], n_unique( i ) );
        }

    return csr_mat;
}
// ----------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_PREDICTORIMPL_HPP_
