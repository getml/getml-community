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
    : data_used_( _data_used ),
      input_col_( _input_col ),
      output_col_( _output_col ),
      peripheral_( _peripheral )
{
}

// ----------------------------------------------------------------------------

Condition::Condition( const Poco::JSON::Object &_obj )
    : Condition(
          enums::Parser<enums::DataUsed>::parse(
              jsonutils::JSON::get_value<std::string>( _obj, "data_used_" ) ),
          jsonutils::JSON::get_value<size_t>( _obj, "input_col_" ),
          jsonutils::JSON::get_value<size_t>( _obj, "output_col_" ),
          jsonutils::JSON::get_value<size_t>( _obj, "peripheral_" ) )
{
}

// ----------------------------------------------------------------------------

Condition::~Condition() = default;

// ----------------------------------------------------------------------------

std::string Condition::to_sql(
    const std::string &_feature_prefix,
    const Placeholder &_input,
    const Placeholder &_output ) const
{
    return SQLMaker::condition( _feature_prefix, *this, _input, _output );
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace dfs
