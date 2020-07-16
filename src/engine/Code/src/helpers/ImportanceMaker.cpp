#include "helpers/helpers.hpp"

namespace helpers
{
// ----------------------------------------------------------------------------

void ImportanceMaker::add_to_importances(
    const std::string& _name, const Float _value )
{
    const auto it = importances_.find( _name );

    if ( it != importances_.end() )
        {
            it->second += _value;
            return;
        }

    importances_[_name] = _value;
}

// ----------------------------------------------------------------------------

void ImportanceMaker::fill_zeros(
    const Placeholder& _pl,
    const std::string& _tname,
    const bool _is_population )
{
    const auto marker = _is_population ? population() : peripheral();

    fill_zeros_from_columns( marker, _tname, _pl.categoricals_ );

    fill_zeros_from_columns( marker, _tname, _pl.discretes_ );

    fill_zeros_from_columns( marker, _tname, _pl.numericals_ );

    fill_zeros_from_columns( marker, _tname, _pl.time_stamps_ );
}

// ----------------------------------------------------------------------------

void ImportanceMaker::fill_zeros_from_columns(
    const std::string& _marker,
    const std::string& _pname,
    const std::vector<std::string>& _colnames )
{
    for ( const auto colname : _colnames )
        {
            const std::string name = _marker + _pname + "." + colname;

            add_to_importances( name, 0.0 );
        }
}

// ----------------------------------------------------------------------------

void ImportanceMaker::merge( const std::map<std::string, Float>& _importances )
{
    for ( const auto& [key, value] : _importances )
        {
            add_to_importances( key, value );
        }
}

// ----------------------------------------------------------------------------

void ImportanceMaker::multiply( const Float _importance_factor )
{
    for ( auto& [_, value] : importances_ )
        {
            value *= _importance_factor;
        }
}

// ----------------------------------------------------------------------------

void ImportanceMaker::normalize()
{
    Float sum = 0.0;

    for ( const auto& [_, value] : importances_ )
        {
            sum += std::abs( value );
        }

    if ( sum > 0.0 )
        {
            for ( auto& [_, value] : importances_ )
                {
                    value /= sum;
                }
        }
}

// ----------------------------------------------------------------------------

void ImportanceMaker::transfer(
    const std::string& _from, const std::string& _to )
{
    auto it = importances_.find( _from );

    assert_true( it != importances_.end() );

    add_to_importances( _to, it->second );

    importances_.erase( it );
}

// ----------------------------------------------------------------------------
}  // namespace helpers
