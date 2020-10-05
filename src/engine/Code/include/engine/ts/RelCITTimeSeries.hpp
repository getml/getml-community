#ifndef ENGINE_TS_RELCITTIMESERIES_HPP_
#define ENGINE_TS_RELCITTIMESERIES_HPP_

namespace engine
{
namespace ts
{
// ----------------------------------------------------------------------------

typedef TimeSeriesModel<relcit::ensemble::DecisionTreeEnsemble>
    RelCITTimeSeries;

// ----------------------------------------------------------------------------
}  // namespace ts
}  // namespace engine

#endif  // ENGINE_TS_RELCITTIMESERIES_HPP_
