#include "helpers/helpers.hpp"

namespace helpers
{
// ----------------------------------------------------------------------------

MappingContainer::MappingContainer(
    const std::vector<MappingForDf>& _categorical,
    const std::vector<std::shared_ptr<const MappingContainer>>& _subcontainers,
    const std::vector<MappingForDf>& _text )
    : categorical_( _categorical ),
      subcontainers_( _subcontainers ),
      text_( _text )
{
    assert_msg(
        categorical_.size() == subcontainers_.size(),
        "categorical_.size(): " + std::to_string( categorical_.size() ) +
            ", subcontainers_.size(): " +
            std::to_string( subcontainers_.size() ) );

    assert_msg(
        categorical_.size() == text_.size(),
        "categorical_.size(): " + std::to_string( categorical_.size() ) +
            ", text_.size(): " + std::to_string( text_.size() ) );
}

// ----------------------------------------------------------------------------

MappingContainer::MappingContainer( const Poco::JSON::Object& _obj )
    : categorical_( extract_mapping_vector( _obj, "categorical_" ) ),
      subcontainers_( extract_subcontainers( _obj ) ),
      text_( extract_mapping_vector( _obj, "text_" ) )
{
    assert_msg(
        categorical_.size() == subcontainers_.size(),
        "categorical_.size(): " + std::to_string( categorical_.size() ) +
            ", subcontainers_.size(): " +
            std::to_string( subcontainers_.size() ) );

    assert_msg(
        categorical_.size() == text_.size(),
        "categorical_.size(): " + std::to_string( categorical_.size() ) +
            ", text_.size(): " + std::to_string( text_.size() ) );
}

// ----------------------------------------------------------------------------

MappingContainer::~MappingContainer() = default;

// ----------------------------------------------------------------------------

std::vector<typename MappingContainer::MappingForDf>
MappingContainer::extract_mapping_vector(
    const Poco::JSON::Object& _obj, const std::string& _key )
{
    // --------------------------------------------------------------

    const auto obj_to_map = []( const Poco::JSON::Object& _obj ) {
        auto m = std::make_shared<std::map<Int, std::vector<Float>>>();

        for ( const auto& [key, _] : _obj )
            {
                const auto arr = _obj.getArray( key );

                assert_msg(
                    arr,
                    "key: " + key +
                        ", _obj: " + jsonutils::JSON::stringify( _obj ) );

                ( *m )[static_cast<Int>( std::atoi( key.c_str() ) )] =
                    jsonutils::JSON::array_to_vector<Float>( arr );
            }

        return m;
    };

    // --------------------------------------------------------------

    const auto extract_mapping = [obj_to_map]( Poco::JSON::Array _arr ) {
        MappingForDf mapping;

        for ( size_t i = 0; i < _arr.size(); ++i )
            {
                auto ptr = _arr.getObject( i );
                throw_unless( ptr, "Expected an object inside the mapping." );
                mapping.push_back( obj_to_map( *ptr ) );
            }

        return mapping;
    };

    // --------------------------------------------------------------

    const auto arr_to_vec = [extract_mapping]( Poco::JSON::Array::Ptr _arr ) {
        throw_unless( _arr, "Expected an array inside the mapping." );

        std::vector<MappingForDf> vec;

        for ( size_t i = 0; i < _arr->size(); ++i )
            {
                auto ptr = _arr->getArray( i );
                throw_unless( ptr, "Expected an array inside the mapping." );
                vec.push_back( extract_mapping( *ptr ) );
            }

        return vec;
    };

    // --------------------------------------------------------------

    return arr_to_vec( _obj.getArray( _key ) );
}

// ----------------------------------------------------------------------------

std::vector<std::shared_ptr<const MappingContainer>>
MappingContainer::extract_subcontainers( const Poco::JSON::Object& _obj )
{
    auto arr = _obj.getArray( "subcontainers_" );

    throw_unless( arr, "Expected array called 'subcontainers_'" );

    std::vector<std::shared_ptr<const MappingContainer>> subcontainers;

    for ( size_t i = 0; i < arr->size(); ++i )
        {
            auto ptr = arr->getObject( i );

            if ( ptr )
                {
                    subcontainers.push_back(
                        std::make_shared<const MappingContainer>( *ptr ) );
                }
            else
                {
                    subcontainers.push_back( nullptr );
                }
        }

    return subcontainers;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object::Ptr MappingContainer::to_json_obj() const
{
    // --------------------------------------------------------------

    const auto map_to_object =
        []( const std::map<Int, std::vector<Float>>& _map ) {
            Poco::JSON::Object::Ptr obj( new Poco::JSON::Object() );

            for ( const auto& [key, value] : _map )
                {
                    obj->set(
                        std::to_string( key ),
                        jsonutils::JSON::vector_to_array_ptr( value ) );
                }

            return obj;
        };

    // --------------------------------------------------------------

    const auto transform_mapping =
        [map_to_object]( const MappingForDf& _mapping ) {
            auto arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

            for ( const auto& ptr : _mapping )
                {
                    assert_true( ptr );
                    arr->add( map_to_object( *ptr ) );
                }

            return arr;
        };

    // --------------------------------------------------------------

    const auto transform_mapping_vec =
        [transform_mapping]( const std::vector<MappingForDf>& _mapping_vec ) {
            auto arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

            for ( const auto& vec : _mapping_vec )
                {
                    arr->add( transform_mapping( vec ) );
                }

            return arr;
        };

    // --------------------------------------------------------------

    const auto transform_subcontainers =
        []( const std::vector<std::shared_ptr<const MappingContainer>>&
                _subcontainers ) {
            auto arr = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

            for ( const auto& s : _subcontainers )
                {
                    if ( s )
                        {
                            arr->add( s->to_json_obj() );
                        }
                    else
                        {
                            arr->add( "" );
                        }
                }

            return arr;
        };

    // --------------------------------------------------------------

    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    obj->set( "categorical_", transform_mapping_vec( categorical_ ) );

    obj->set( "subcontainers_", transform_subcontainers( subcontainers_ ) );

    obj->set( "text_", transform_mapping_vec( text_ ) );

    return obj;

    // --------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace helpers
