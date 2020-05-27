#ifndef ENGINE_TS_MULTIRELTIMESERIES_HPP_
#define ENGINE_TS_MULTIRELTIMESERIES_HPP_

namespace engine
{
namespace ts
{
// ----------------------------------------------------------------------------

typedef TimeSeriesModel<multirel::ensemble::DecisionTreeEnsemble>
    MultirelTimeSeries;

// ----------------------------------------------------------------------------
}  // namespace ts
}  // namespace engine

#endif  // ENGINE_TS_MULTIRELTIMESERIES_HPP_
