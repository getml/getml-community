#ifndef TS_RELBOOSTTIMESERIES_HPP_
#define TS_RELBOOSTTIMESERIES_HPP_

namespace ts
{
// ----------------------------------------------------------------------------

typedef TimeSeriesModel<relboost::ensemble::DecisionTreeEnsemble>
    RelboostTimeSeries;

// ----------------------------------------------------------------------------
}  // namespace ts

#endif  // TS_RELBOOSTTIMESERIES_HPP_
