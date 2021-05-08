#ifndef ENGINE_CONTAINER_DATAFRAMEEXTRACTOR_HPP_
#define ENGINE_CONTAINER_DATAFRAMERXTRACTOR_HPP_

namespace engine
{
namespace containers
{
// ----------------------------------------------------------------------------

struct DataFrameExtractor
{
    /// Extracts the data frames referenced in the command _cmd.
    static std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
    extract_data_frames(
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames );

    /// Extracts the fingerprints of all data frames referenced in the command
    /// _cmd.
    static std::vector<Poco::JSON::Object::Ptr> extract_df_fingerprints(
        const Poco::JSON::Object& _obj,
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames );
};

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINER_DATAFRAMEPRINTER_HPP_
