#ifndef ENGINE_TS_DFSTIMESERIES_HPP_
#define ENGINE_TS_DFSTIMESERIES_HPP_

namespace engine
{
namespace ts
{
// ----------------------------------------------------------------------------

typedef TimeSeriesModel<dfs::algorithm::DeepFeatureSynthesis> DFSTimeSeries;

// ----------------------------------------------------------------------------
}  // namespace ts
}  // namespace engine

#endif  // ENGINE_TS_DFSTIMESERIES_HPP_
