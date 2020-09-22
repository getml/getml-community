#include "engine/containers/containers.hpp"

namespace engine
{
namespace containers
{
// ----------------------------------------------------------------------------

std::vector<Poco::JSON::Object::Ptr>
DataFrameExtractor::extract_df_fingerprints(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames )
{
    const auto [population, peripheral] =
        extract_data_frames( _cmd, _data_frames );

    std::vector<Poco::JSON::Object::Ptr> df_fingerprints = {
        population.fingerprint()};

    for ( const auto& df : peripheral )
        {
            df_fingerprints.push_back( df.fingerprint() );
        }

    return df_fingerprints;
}

// ----------------------------------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
DataFrameExtractor::extract_data_frames(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames )
{
    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto peripheral_names = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "peripheral_names_" ) );

    const auto population = utils::Getter::get( population_name, _data_frames );

    auto peripheral = std::vector<containers::DataFrame>();

    for ( const auto& df_name : peripheral_names )
        {
            const auto df = utils::Getter::get( df_name, _data_frames );

            peripheral.push_back( df );
        }

    return std::make_pair( population, peripheral );
}
// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine
