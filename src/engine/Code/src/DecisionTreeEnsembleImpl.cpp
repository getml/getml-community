#include "ensemble/ensemble.hpp"

namespace relboost
{
namespace ensemble
{
// ----------------------------------------------------------------------------

void DecisionTreeEnsembleImpl::check_placeholder(
    const Placeholder& _placeholder ) const
{
    for ( auto& joined : _placeholder.joined_tables_ )
        {
            const bool not_found =
                ( std::find(
                      peripheral_names_->begin(),
                      peripheral_names_->end(),
                      joined.name_ ) == peripheral_names_->end() );

            if ( not_found )
                {
                    throw std::runtime_error(
                        "Table  named '" + joined.name_ +
                        "' not among peripheral tables!" );
                }

            check_placeholder( joined );
        }
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relboost
