#ifndef HELPERS_STRINGREPLACER_HPP_
#define HELPERS_STRINGREPLACER_HPP_

#include <string>

namespace helpers {
// ----------------------------------------------------------------------------

struct StringReplacer {
  /// Replaces all instances of _from in _str with _to.
  static std::string replace_all(const std::string &_str,
                                 const std::string &_from,
                                 const std::string &_to);
};

// ----------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_STRINGREPLACER_HPP_

