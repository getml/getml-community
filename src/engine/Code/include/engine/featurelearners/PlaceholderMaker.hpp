#ifndef ENGINE_FEATURELEARNERS_PLACEHOLDERMAKER_HPP_
#define ENGINE_FEATURELEARNERS_PLACEHOLDERMAKER_HPP_

namespace engine
{
namespace featurelearners
{
// ----------------------------------------------------------------------------

class PlaceholderMaker
{
   public:
    /// Creates the placeholder, including transforming memory into upper time
    /// stamps.
    static helpers::Placeholder make_placeholder(
        const Poco::JSON::Object& _obj );

    /// Returns a list of all peripheral tables used in the placeholder.
    static std::vector<std::string> make_peripheral(
        const helpers::Placeholder& _placeholder );

    /// Generates the name for the upper time stamp that is produced using
    /// memory.
    static std::string make_ts_name(
        const std::string& _ts_used, const Float _diff )
    {
        return "$GETML_GENERATED_TS" + _ts_used + "\"" +
               utils::TSDiffMaker::make_time_stamp_diff( _diff ) +
               "$GETML_REMOVE_CHAR";
    }

   private:
    template <typename T>
    static void append( const std::vector<T>& _vec2, std::vector<T>* _vec1 );

    static void extract_joined_tables(
        const helpers::Placeholder& _placeholder,
        std::set<std::string>* _names );

    template <typename T>
    static std::vector<T> extract_vector(
        const Poco::JSON::Object& _population_placeholder,
        const std::string& _name,
        const size_t _expected_size );

    static std::vector<std::string> handle_horizon(
        const helpers::Placeholder& _placeholder,
        const std::vector<Float>& _horizon );

    static helpers::Placeholder handle_joined_tables(
        const helpers::Placeholder& _placeholder,
        const Poco::JSON::Array& _joined_tables_arr,
        const std::vector<bool>& _many_to_one,
        const std::vector<std::string>& _other_time_stamps_used,
        const std::vector<std::string>& _upper_time_stamps_used );

    static std::vector<std::string> handle_memory(
        const helpers::Placeholder& _placeholder,
        const std::vector<Float>& _horizon,
        const std::vector<Float>& _memory );

   private:
    static std::string make_name(
        const std::string& _join_key,
        const std::string& _other_join_key,
        const std::string& _time_stamp,
        const std::string& _other_time_stamp,
        const std::string& _upper_time_stamp,
        const std::string& _name )
    {
        return "$GETML_JOIN_PARAM_NAME=" + _name +
               "$GETML_JOIN_PARAM_JOIN_KEY=" + _join_key +
               "$GETML_JOIN_PARAM_OTHER_JOIN_KEY=" + _other_join_key +
               "$GETML_JOIN_PARAM_TIME_STAMP=" + _time_stamp +
               "$GETML_JOIN_PARAM_OTHER_TIME_STAMP=" + _other_time_stamp +
               "$GETML_JOIN_PARAM_UPPER_TIME_STAMP=" + _upper_time_stamp +
               "$GETML_JOIN_PARAM_END";
    }
};

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------

template <typename T>
void PlaceholderMaker::append(
    const std::vector<T>& _vec2, std::vector<T>* _vec1 )
{
    for ( const auto& elem : _vec2 )
        {
            _vec1->push_back( elem );
        }
}

// ------------------------------------------------------------------------

template <typename T>
std::vector<T> PlaceholderMaker::extract_vector(
    const Poco::JSON::Object& _population_placeholder,
    const std::string& _name,
    const size_t _expected_size )
{
    // ------------------------------------------------------------------------

    if ( !_population_placeholder.getArray( _name ) )
        {
            throw std::invalid_argument(
                "The placeholder has no array named '" + _name + "'!" );
        }

    const auto vec =
        JSON::array_to_vector<T>( _population_placeholder.getArray( _name ) );

    // ------------------------------------------------------------------------

    if ( vec.size() != _expected_size )
        {
            throw std::invalid_argument(
                "Size of '" + _name + "' unexpected. Expected " +
                std::to_string( _expected_size ) + ", got " +
                std::to_string( vec.size() ) + "." );
        }

    // ------------------------------------------------------------------------

    return vec;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace featurelearners
}  // namespace engine

#endif  // ENGINE_FEATURELEARNERS_PLACEHOLDERMAKER_HPP_
