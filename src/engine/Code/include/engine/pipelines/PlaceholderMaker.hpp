#ifndef ENGINE_PIPELINES_PLACEHOLDERMAKER_HPP_
#define ENGINE_PIPELINES_PLACEHOLDERMAKER_HPP_

namespace engine
{
namespace pipelines
{
// ----------------------------------------------------------------------------

class PlaceholderMaker
{
   private:
    static constexpr const char* RELATIONSHIP_MANY_TO_MANY = "many-to-many";
    static constexpr const char* RELATIONSHIP_MANY_TO_ONE = "many-to-one";
    static constexpr const char* RELATIONSHIP_PROPOSITIONALIZATION =
        helpers::Placeholder::RELATIONSHIP_PROPOSITIONALIZATION;
    static constexpr const char* RELATIONSHIP_ONE_TO_MANY = "one-to-many";
    static constexpr const char* RELATIONSHIP_ONE_TO_ONE = "one-to-one";

   public:
    /// Creates the placeholder, including transforming memory into upper time
    /// stamps.
    static helpers::Placeholder make_placeholder(
        const Poco::JSON::Object& _obj,
        const std::string& _alias,
        const std::shared_ptr<size_t> _num_alias = nullptr );

    /// Returns a list of all peripheral tables used in the placeholder.
    static std::vector<std::string> make_peripheral(
        const helpers::Placeholder& _placeholder );

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
        const std::string& _alias,
        const std::shared_ptr<size_t> _num_alias,
        const Poco::JSON::Array& _joined_tables_arr,
        const std::vector<std::string>& _relationship,
        const std::vector<std::string>& _other_time_stamps_used,
        const std::vector<std::string>& _upper_time_stamps_used );

    static std::vector<std::string> handle_memory(
        const helpers::Placeholder& _placeholder,
        const std::vector<Float>& _horizon,
        const std::vector<Float>& _memory );

    static std::vector<std::string> make_colnames(
        const std::string& _tname,
        const std::string& _alias,
        const std::vector<std::string>& _old_colnames );

   private:
    static bool is_to_many( const std::string& _relationship )
    {
        return (
            _relationship == RELATIONSHIP_MANY_TO_MANY ||
            _relationship == RELATIONSHIP_PROPOSITIONALIZATION ||
            _relationship == RELATIONSHIP_ONE_TO_MANY );
    }

    static std::string make_alias( const std::shared_ptr<size_t> _num_alias )
    {
        assert_true( _num_alias );
        auto& num_alias = *_num_alias;
        return "t" + std::to_string( ++num_alias );
    }

    /// Generates the name for the time stamp that is produced using
    /// memory.
    static std::string make_ts_name(
        const std::string& _ts_used, const Float _diff )
    {
        return ts::TimeStampMaker::make_ts_name( _ts_used, _diff );
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
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_PLACEHOLDERMAKER_HPP_
