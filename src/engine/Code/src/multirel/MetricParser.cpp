#include "metrics/metrics.hpp"

namespace multirel
{
namespace metrics
{
// ----------------------------------------------------------------------------

std::shared_ptr<Metric> MetricParser::parse( const std::string& _type )
{
    if ( _type == "accuracy_" )
        {
            return std::make_shared<Accuracy>();
        }
    else if ( _type == "auc_" )
        {
            return std::make_shared<AUC>();
        }
    else if ( _type == "cross_entropy_" )
        {
            return std::make_shared<CrossEntropy>();
        }
    else if ( _type == "mae_" )
        {
            return std::make_shared<MAE>();
        }
    else if ( _type == "rmse_" )
        {
            return std::make_shared<RMSE>();
        }
    else if ( _type == "rsquared_" )
        {
            return std::make_shared<RSquared>();
        }
    else
        {
            std::string msg = "Metric of type '";
            msg.append( _type );
            msg.append( "' not known!" );

            throw std::invalid_argument( msg );
        }
}

// ----------------------------------------------------------------------------
}  // namespace metrics
}  // namespace multirel
