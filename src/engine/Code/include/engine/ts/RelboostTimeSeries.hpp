#ifndef ENGINE_TS_RELBOOSTTIMESERIES_HPP_
#define ENGINE_TS_RELBOOSTTIMESERIES_HPP_

namespace engine
{
namespace ts
{
// ----------------------------------------------------------------------------

typedef TimeSeriesModel<relboost::ensemble::DecisionTreeEnsemble>
    RelboostTimeSeries;

// ----------------------------------------------------------------------------
}  // namespace ts
}  // namespace engine

#endif  // ENGINE_TS_RELBOOSTTIMESERIES_HPP_
