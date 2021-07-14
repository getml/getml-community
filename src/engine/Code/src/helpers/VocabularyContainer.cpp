#include "helpers/helpers.hpp"

namespace helpers
{
// ----------------------------------------------------------------------------

VocabularyContainer::VocabularyContainer(
    size_t _min_df,
    size_t _max_size,
    const DataFrame& _population,
    const std::vector<DataFrame>& _peripheral )
{
    const auto extract_from_col =
        [_min_df, _max_size]( const Column<strings::String>& col ) {
            return textmining::Vocabulary::generate(
                _min_df, _max_size, stl::Range( col.begin(), col.end() ) );
        };

    const auto extract_from_df =
        [extract_from_col]( const DataFrame& df ) -> VocabForDf {
        auto range = df.text_ | std::views::transform( extract_from_col );
        return VocabForDf( range.begin(), range.end() );
    };

    auto range = _peripheral | std::views::transform( extract_from_df );

    peripheral_ = stl::collect::vector<VocabForDf>( range );

    population_ = extract_from_df( _population );

#ifndef NDEBUG
    assert_true( _population.num_text() == population().size() );

    assert_true( _peripheral.size() == peripheral().size() );

    for ( size_t i = 0; i < _peripheral.size(); ++i )
        {
            assert_true(
                _peripheral.at( i ).num_text() == peripheral().at( i ).size() );
        }
#endif
}

// ----------------------------------------------------------------------------

VocabularyContainer::VocabularyContainer(
    const VocabForDf& _population, const std::vector<VocabForDf>& _peripheral )
    : peripheral_( _peripheral ), population_( _population )
{
}

// ----------------------------------------------------------------------------

VocabularyContainer::VocabularyContainer( const Poco::JSON::Object& _obj )
{
    // ------------------------------------------------------------------

    const auto to_str = []( const std::string& _str ) -> strings::String {
        return strings::String( _str );
    };

    // ------------------------------------------------------------------

    const auto make_vocab = [to_str]( Poco::JSON::Array::Ptr arr )
        -> std::shared_ptr<const std::vector<strings::String>> {
        throw_unless( arr, "Vocabulary: Expected a proper JSON array." );

        const auto vec = jsonutils::JSON::array_to_vector<std::string>( arr );

        auto range = vec | std::views::transform( to_str );

        return std::make_shared<const std::vector<strings::String>>(
            stl::collect::vector<strings::String>( range ) );
    };

    // ------------------------------------------------------------------

    const auto make_vocab_for_df =
        [make_vocab]( Poco::JSON::Array::Ptr arr ) -> VocabForDf {
        throw_unless( arr, "Vocabulary for DF: Expected a proper JSON array." );

        VocabForDf vocab_for_df;

        for ( size_t i = 0; i < arr->size(); ++i )
            {
                vocab_for_df.push_back( make_vocab( arr->getArray( i ) ) );
            }

        return vocab_for_df;
    };

    // ------------------------------------------------------------------

    auto peripheral_arr = jsonutils::JSON::get_array( _obj, "peripheral_" );

    for ( size_t i = 0; i < peripheral_arr->size(); ++i )
        {
            peripheral_.push_back(
                make_vocab_for_df( peripheral_arr->getArray( i ) ) );
        }

    // ------------------------------------------------------------------

    population_ =
        make_vocab_for_df( jsonutils::JSON::get_array( _obj, "population_" ) );

    // ------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

VocabularyContainer::~VocabularyContainer() = default;

// ----------------------------------------------------------------------------

Poco::JSON::Object::Ptr VocabularyContainer::to_json_obj() const
{
    // ------------------------------------------------------------------

    const auto to_str = []( const strings::String& _str ) -> std::string {
        return _str.str();
    };

    // ------------------------------------------------------------------

    const auto handle_vocab =
        [to_str]( const std::vector<strings::String>& _vocab )
        -> Poco::JSON::Array::Ptr {
        auto range = _vocab | std::views::transform( to_str );
        const auto vec = stl::collect::vector<std::string>( range );
        return jsonutils::JSON::vector_to_array_ptr( vec );
    };

    // ------------------------------------------------------------------

    const auto handle_vocab_for_df =
        [handle_vocab]( const VocabForDf& _vocab ) -> Poco::JSON::Array::Ptr {
        auto arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

        for ( const auto& v : _vocab )
            {
                assert_true( v );
                arr->add( handle_vocab( *v ) );
            }

        return arr;
    };

    // ------------------------------------------------------------------

    auto peripheral_arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    for ( const auto& p : peripheral_ )
        {
            peripheral_arr->add( handle_vocab_for_df( p ) );
        }

    // ------------------------------------------------------------------

    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    obj->set( "peripheral_", peripheral_arr );

    obj->set( "population_", handle_vocab_for_df( population_ ) );

    return obj;

    // ------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace helpers
