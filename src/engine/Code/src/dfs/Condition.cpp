#include "dfs/containers/containers.hpp"

namespace dfs
{
namespace containers
{
// ----------------------------------------------------------------------------

Condition::Condition(
    const enums::DataUsed _data_used,
    const size_t _input_col,
    const size_t _output_col,
    const size_t _peripheral )
    : category_used_( -1 ),
      data_used_( _data_used ),
      input_col_( _input_col ),
      output_col_( _output_col ),
      peripheral_( _peripheral )
{
    assert_true( _data_used == enums::DataUsed::same_units_categorical );
}

// ----------------------------------------------------------------------------

Condition::Condition(
    const Int _category_used,
    const enums::DataUsed _data_used,
    const size_t _input_col,
    const size_t _peripheral )
    : category_used_( _category_used ),
      data_used_( _data_used ),
      input_col_( _input_col ),
      output_col_( 0 ),
      peripheral_( _peripheral )
{
    assert_true( _data_used == enums::DataUsed::categorical );
}

// ----------------------------------------------------------------------------

Condition::Condition( const Poco::JSON::Object &_obj )
    : category_used_(
          jsonutils::JSON::get_value<Int>( _obj, "category_used_" ) ),
      data_used_( enums::Parser<enums::DataUsed>::parse(
          jsonutils::JSON::get_value<std::string>( _obj, "data_used_" ) ) ),
      input_col_( jsonutils::JSON::get_value<size_t>( _obj, "input_col_" ) ),
      output_col_( jsonutils::JSON::get_value<size_t>( _obj, "output_col_" ) ),
      peripheral_( jsonutils::JSON::get_value<size_t>( _obj, "peripheral_" ) )
{
}

// ----------------------------------------------------------------------------

Condition::~Condition() = default;

// ----------------------------------------------------------------------------

Poco::JSON::Object::Ptr Condition::to_json_obj() const
{
    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    obj->set( "category_used_", category_used_ );

    obj->set(
        "data_used_", enums::Parser<enums::DataUsed>::to_str( data_used_ ) );

    obj->set( "input_col_", input_col_ );

    obj->set( "output_col_", output_col_ );

    obj->set( "peripheral_", peripheral_ );

    return obj;
}

// ----------------------------------------------------------------------------

std::string Condition::to_sql(
    const std::vector<strings::String> &_categories,
    const std::string &_feature_prefix,
    const Placeholder &_input,
    const Placeholder &_output ) const
{
    return SQLMaker::condition(
        _categories, _feature_prefix, *this, _input, _output );
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace dfs
