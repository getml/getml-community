#include "predictors/predictors.hpp"

namespace predictors
{
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

void PredictorImpl::select_cols(
    const size_t _n_selected,
    const size_t _n_autofeatures,
    const std::vector<size_t>& _index )
{
    select_cols(
        _n_selected,
        _index,
        _n_autofeatures + discrete_colnames_.size(),
        &numerical_colnames_ );

    select_cols( _n_selected, _index, _n_autofeatures, &discrete_colnames_ );
}

// ----------------------------------------------------------------------------

void PredictorImpl::select_cols(
    const size_t _n_selected,
    const std::vector<size_t>& _index,
    const size_t _ix_begin,
    std::vector<std::string>* _colnames ) const
{
    std::vector<std::string> selected;

    const auto begin = _index.begin();

    const auto end = _index.begin() + _n_selected;

    for ( size_t i = 0; i < _colnames->size(); ++i )
        {
            const auto is_included = [_ix_begin, i]( size_t ix ) {
                return ix == _ix_begin + i;
            };

            const auto it = std::find_if( begin, end, is_included );

            if ( it != end )
                {
                    selected.push_back( ( *_colnames )[i] );
                }
        }

    *_colnames = selected;
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
        "discrete_colnames_", JSON::vector_to_array( discrete_colnames_ ) );

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
