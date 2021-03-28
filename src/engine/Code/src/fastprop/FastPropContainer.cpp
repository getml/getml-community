#include "fastprop/subfeatures/subfeatures.hpp"

namespace fastprop
{
namespace subfeatures
{
// ----------------------------------------------------------------------------

FastPropContainer::FastPropContainer(
    const std::shared_ptr<const algorithm::FastProp>& _fast_prop,
    const std::shared_ptr<const Subcontainers>& _subcontainers )
    : fast_prop_( _fast_prop ), subcontainers_( _subcontainers )
{
    assert_true( subcontainers_ );
}

// ----------------------------------------------------------------------------

FastPropContainer::FastPropContainer( const Poco::JSON::Object& _obj )
    : fast_prop_(
          _obj.has( "fast_prop_" )
              ? std::make_shared<algorithm::FastProp>(
                    *jsonutils::JSON::get_object( _obj, "fast_prop_" ) )
              : std::shared_ptr<algorithm::FastProp>() ),
      subcontainers_( FastPropContainer::parse_subcontainers( _obj ) )
{
    assert_true( subcontainers_ );
}

// ----------------------------------------------------------------------------

FastPropContainer::~FastPropContainer() = default;

// ----------------------------------------------------------------------------

std::shared_ptr<typename FastPropContainer::Subcontainers>
FastPropContainer::parse_subcontainers( const Poco::JSON::Object& _obj )
{
    auto arr = _obj.getArray( "subcontainers_" );

    throw_unless( arr, "Expected array called 'subcontainers_'" );

    const auto subcontainers = std::make_shared<Subcontainers>();

    for ( size_t i = 0; i < arr->size(); ++i )
        {
            auto ptr = arr->getObject( i );

            if ( ptr )
                {
                    subcontainers->push_back(
                        std::make_shared<const FastPropContainer>( *ptr ) );
                }
            else
                {
                    subcontainers->push_back( nullptr );
                }
        }

    return subcontainers;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object::Ptr FastPropContainer::to_json_obj() const
{
    const auto transform_subcontainers =
        []( const Subcontainers& _subcontainers ) {
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

    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    if ( fast_prop_ )
        {
            auto fp = Poco::JSON::Object::Ptr(
                new Poco::JSON::Object( fast_prop_->to_json_obj() ) );
            obj->set( "fast_prop_", fp );
        }

    obj->set( "subcontainers_", transform_subcontainers( *subcontainers_ ) );

    return obj;
}

// ----------------------------------------------------------------------------

}  // namespace subfeatures
}  // namespace fastprop
