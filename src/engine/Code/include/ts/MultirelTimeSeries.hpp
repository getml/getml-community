#ifndef TS_MULTIRELTIMESERIES_HPP_
#define TS_MULTIRELTIMESERIES_HPP_

namespace ts
{
// ----------------------------------------------------------------------------

typedef TimeSeriesModel<multirel::ensemble::DecisionTreeEnsemble>
    MultirelTimeSeries;

// ----------------------------------------------------------------------------
}  // namespace ts

#endif  // TS_MULTIRELTIMESERIES_HPP_
