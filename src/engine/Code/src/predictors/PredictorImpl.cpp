#include "predictors/predictors.hpp"

namespace predictors
{
// -----------------------------------------------------------------------------

PredictorImpl::PredictorImpl(
    const std::vector<size_t>& _num_autofeatures,
    const std::vector<std::string>& _categorical_colnames,
    const std::vector<std::string>& _numerical_colnames )
    : categorical_colnames_( _categorical_colnames ),
      numerical_colnames_( _numerical_colnames )
{
    for ( const auto n : _num_autofeatures )
        {
            autofeatures_.emplace_back( std::vector<size_t>( n ) );

            for ( size_t i = 0; i < n; ++i )
                {
                    autofeatures_.back().at( i ) = i;
                }
        }
};

// -----------------------------------------------------------------------------

PredictorImpl::PredictorImpl( const Poco::JSON::Object& _obj )
    : categorical_colnames_( JSON::array_to_vector<std::string>(
          JSON::get_array( _obj, "categorical_colnames_" ) ) ),
      numerical_colnames_( JSON::array_to_vector<std::string>(
          JSON::get_array( _obj, "numerical_colnames_" ) ) )
{
    // -----------------------------------------------

    auto arr = JSON::get_array( _obj, "encodings_" );

    for ( size_t i = 0; i < arr->size(); ++i )
        {
            auto ptr = arr->getObject( static_cast<unsigned int>( i ) );

            if ( !ptr )
                {
                    throw std::runtime_error(
                        "Element " + std::to_string( i + 1 ) +
                        " of 'encodings_' is not a proper JSON object." );
                }

            encodings_.push_back( Encoding( *ptr ) );
        }

    // -----------------------------------------------

    arr = JSON::get_array( _obj, "autofeatures_" );

    for ( size_t i = 0; i < arr->size(); ++i )
        {
            auto ptr = arr->getArray( static_cast<unsigned int>( i ) );

            if ( !ptr )
                {
                    throw std::runtime_error(
                        "Element " + std::to_string( i + 1 ) +
                        " of 'autofeatures_' is not a proper JSON "
                        "array." );
                }

            autofeatures_.push_back( JSON::array_to_vector<size_t>( ptr ) );
        }

    // -----------------------------------------------
};

// -----------------------------------------------------------------------------

PredictorImpl::~PredictorImpl() = default;

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
            auto& vec = autofeatures_.at( size - 1 - i );

            assert_true( vec.size() <= num_preceding );

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

    auto enc_arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    for ( auto& enc : encodings_ )
        {
            enc_arr->add( enc.to_json_obj() );
        }

    obj.set( "encodings_", enc_arr );

    // -------------------------------------------------------------------------

    auto auto_arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    for ( auto& auto_f : autofeatures() )
        {
            auto_arr->add( JSON::vector_to_array( auto_f ) );
        }

    obj.set( "autofeatures_", auto_arr );

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
