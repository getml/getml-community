#include "predictors/predictors.hpp"

namespace predictors
{
// -----------------------------------------------------------------------------

void PredictorImpl::compress_importances(
    const std::vector<Float>& _all_feature_importances,
    std::vector<Float>* _feature_importances ) const
{
    assert_true( _all_feature_importances.size() == ncols_csr() );

    assert_true(
        _feature_importances->size() ==
        num_autofeatures() + num_manual_features() );

    const auto n_dense = num_autofeatures() + numerical_colnames_.size();

    std::copy(
        _all_feature_importances.begin(),
        _all_feature_importances.begin() + n_dense,
        _feature_importances->begin() );

    auto begin = _all_feature_importances.begin() + n_dense;

    assert_true(
        encodings_.size() == categorical_colnames_.size() ||
        encodings_.size() == 0 );

    for ( size_t i = 0; i < encodings_.size(); ++i )
        {
            auto end = begin + encodings_[i].n_unique();

            auto imp = std::accumulate( begin, end, 0.0 );

            ( *_feature_importances )[n_dense + i] = imp;

            begin = end;
        }
}

// -------------------------------------------------------------------------

size_t PredictorImpl::check_plausibility(
    const std::vector<CIntColumn>& _X_categorical,
    const std::vector<CFloatColumn>& _X_numerical ) const
{
    if ( _X_categorical.size() == 0 && _X_numerical.size() == 0 )
        {
            throw std::invalid_argument(
                "You must provide at least one input column!" );
        }

    size_t expected_size = 0;

    if ( _X_categorical.size() > 0 )
        {
            expected_size = _X_categorical[0]->size();
        }
    else
        {
            expected_size = _X_numerical[0]->size();
        }

    for ( const auto& X : _X_categorical )
        {
            if ( X->size() != expected_size )
                {
                    throw std::invalid_argument(
                        "All input columns must have the same "
                        "length!" );
                }
        }

    for ( const auto& X : _X_numerical )
        {
            if ( X->size() != expected_size )
                {
                    throw std::invalid_argument(
                        "All input columns must have the same length!" );
                }
        }

    return expected_size;
}

// -------------------------------------------------------------------------

void PredictorImpl::check_plausibility(
    const std::vector<CIntColumn>& _X_categorical,
    const std::vector<CFloatColumn>& _X_numerical,
    const CFloatColumn& _y ) const
{
    const auto expected_size =
        check_plausibility( _X_categorical, _X_numerical );

    if ( _y->size() != expected_size )
        {
            throw std::invalid_argument(
                "Length of targets must be the same as the length of the input "
                "columns!" );
        }
}

// -----------------------------------------------------------------------------

void PredictorImpl::fit_encodings(
    const std::vector<CIntColumn>& _X_categorical )
{
    encodings_.clear();

    for ( auto& col : _X_categorical )
        {
            encodings_.push_back( Encoding() );

            encodings_.back().fit( col );
        }
}

// -------------------------------------------------------------------------

void PredictorImpl::save( const std::string& _fname ) const
{
    std::ofstream output( _fname );

    output << to_json();

    output.close();
}

// ----------------------------------------------------------------------------

void PredictorImpl::select_features(
    const size_t _n_selected, const std::vector<size_t>& _index )
{
    encodings_.clear();

    auto num_preceding = num_autofeatures() + numerical_colnames_.size();

    categorical_colnames_ = select_cols(
        _n_selected, _index, num_preceding, categorical_colnames_ );

    num_preceding -= numerical_colnames_.size();

    numerical_colnames_ =
        select_cols( _n_selected, _index, num_preceding, numerical_colnames_ );

    const auto size = autofeatures_.size();

    if ( size == 0 )
        {
            return;
        }

    for ( size_t i = 0; i < size; ++i )
        {
            auto& vec = autofeatures_[size - 1 - i];

            assert_true( vec.size() >= num_preceding );

            num_preceding -= vec.size();

            vec = select_cols( _n_selected, _index, num_preceding, vec );
        }
}

// ----------------------------------------------------------------------------

Poco::JSON::Object PredictorImpl::to_json_obj() const
{
    // -------------------------------------------------------------------------

    Poco::JSON::Object obj;

    // -------------------------------------------------------------------------

    obj.set(
        "categorical_colnames_",
        JSON::vector_to_array( categorical_colnames_ ) );

    obj.set(
        "numerical_colnames_", JSON::vector_to_array( numerical_colnames_ ) );

    // -------------------------------------------------------------------------

    auto arr = Poco::JSON::Array();

    for ( auto& enc : encodings_ )
        {
            arr.add( enc.to_json_obj() );
        }

    obj.set( "encodings_", arr );

    // -------------------------------------------------------------------------

    // TODO
    // obj.set( "num_autofeatures_", num_autofeatures_ );

    // -------------------------------------------------------------------------

    return obj;

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

std::vector<CIntColumn> PredictorImpl::transform_encodings(
    const std::vector<CIntColumn>& _X_categorical ) const
{
    if ( _X_categorical.size() != encodings_.size() )
        {
            throw std::runtime_error(
                "Expected " + std::to_string( encodings_.size() ) +
                " categorical columns, got " +
                std::to_string( _X_categorical.size() ) + "." );
        }

    std::vector<CIntColumn> transformed( encodings_.size() );

    for ( size_t i = 0; i < encodings_.size(); ++i )
        {
            transformed[i] = encodings_[i].transform( _X_categorical[i] );
        }

    return transformed;
}

// -----------------------------------------------------------------------------
}  // namespace predictors
