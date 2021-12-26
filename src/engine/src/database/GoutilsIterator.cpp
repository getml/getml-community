#include "database/GoutilsIterator.hpp"

namespace database {
// ----------------------------------------------------------------------------

Float GoutilsIterator::get_double() {
  const auto [str, is_null] = get_value();

  if (is_null) {
    return static_cast<Float>(NAN);
  }

  return Getter::get_double(str);
}

// ----------------------------------------------------------------------------

Int GoutilsIterator::get_int() {
  const auto [str, is_null] = get_value();

  if (is_null) {
    return 0;
  }

  return Getter::get_int(str);
}

// ----------------------------------------------------------------------------

std::string GoutilsIterator::get_string() {
  const auto [val, is_null] = get_value();

  if (is_null) {
    return "NULL";
  }

  return val;
}

// ----------------------------------------------------------------------------

Float GoutilsIterator::get_time_stamp() {
  const auto [str, is_null] = get_value();

  if (is_null) {
    return static_cast<Float>(NAN);
  }

  return Getter::get_time_stamp(str, time_formats_);
}

// ----------------------------------------------------------------------------

}  // namespace database

