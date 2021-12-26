#include "database/CSVBuffer.hpp"

namespace database {
// ----------------------------------------------------------------------------

std::string CSVBuffer::make_buffer(const std::vector<std::string>& _line,
                                   const std::vector<io::Datatype>& _coltypes,
                                   const char _sep, const char _quotechar,
                                   const bool _always_enclose_str,
                                   const bool _explicit_null) {
  std::string buffer;

  assert_true(_line.size() == _coltypes.size());

  for (size_t i = 0; i < _line.size(); ++i) {
    buffer += parse_field(_line[i], _coltypes[i], _sep, _quotechar,
                          _always_enclose_str, _explicit_null);

    if (i < _line.size() - 1) {
      buffer += _sep;
    } else {
      buffer += '\n';
    }
  }

  return buffer;
}

// ----------------------------------------------------------------------------

std::string CSVBuffer::parse_field(const std::string& _raw_field,
                                   const io::Datatype _datatype,
                                   const char _sep, const char _quotechar,
                                   const bool _always_enclose_str,
                                   const bool _explicit_null) {
  switch (_datatype) {
    case io::Datatype::double_precision: {
      const auto [val, success] = io::Parser::to_double(_raw_field);

      if (success) {
        return std::to_string(val);
      } else {
        return _explicit_null ? "NULL" : "";
      }
    }

      // ------------------------------------------------------------

    case io::Datatype::integer: {
      const auto [val, success] = io::Parser::to_int(_raw_field);

      if (success) {
        return std::to_string(val);
      } else {
        return _explicit_null ? "NULL" : "";
      }
    }

      // ------------------------------------------------------------

    default:
      auto field = io::Parser::remove_quotechars(_raw_field, _quotechar);

      if (_always_enclose_str || field.find(_sep) != std::string::npos) {
        field = _quotechar + field + _quotechar;
      }

      return field;

      // ------------------------------------------------------------
  }
}

// ----------------------------------------------------------------------------
}  // namespace database
