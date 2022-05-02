#ifndef STRINGS_STRING_HPP_
#define STRINGS_STRING_HPP_

// ----------------------------------------------------------------------------

#include <cstring>

// ----------------------------------------------------------------------------

#include <algorithm>
#include <memory>
#include <string>
#include <utility>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"

// ----------------------------------------------------------------------------

namespace strings {

// String is an implementation of a string class that has
// no memory overhead over a standard C-string.
class String {
  static constexpr char nullstr = '\0';

 public:
  String() : chars_(std::make_unique<char[]>(1)) { chars_.get()[0] = '\0'; }

  String(const std::string& _str)
      : chars_(std::make_unique<char[]>(_str.size() + 1)) {
    std::copy(_str.begin(), _str.end(), chars_.get());
    chars_.get()[_str.size()] = '\0';
  }

  explicit String(const uint64_t _size)
      : chars_(std::make_unique<char[]>(_size + 1)) {
    chars_.get()[_size] = '\0';
  }

  String(const char* _str) : String(_str, _str ? strlen(_str) : 0) {}

  String(const char* _str, const size_t _size)
      : chars_(_str ? std::make_unique<char[]>(_size + 1) : nullptr) {
    if (chars_) {
      std::copy(_str, _str + _size, chars_.get());
      chars_.get()[_size] = '\0';
    }
  }

  String(const String& _other)
      : chars_(_other ? std::make_unique<char[]>(_other.size() + 1) : nullptr) {
    if (chars_) {
      strcpy(chars_.get(), _other.c_str());
    }
  }

  String(String&& _other) noexcept : chars_(std::move(_other.chars_)) {}

  ~String() = default;

 public:
  /// Checks whether the string contains another string.
  bool contains(const strings::String& _other) const {
    return (strstr(c_str(), _other.c_str()) != NULL);
  }

  /// Returns a pointer to the underlying C-String.
  const char* c_str() const {
    if (!chars_) {
      return &nullstr;
    }
    return chars_.get();
  }

  /// Calculates the hash function of this string.
  /// This is useful for std::unordered_map.
  size_t hash() const {
    return std::hash<std::string_view>()(std::string_view(c_str(), size()));
  }

  /// Whether the string is set.
  operator bool() const { return (chars_ && true); }

  /// Copy assignment operator.
  String& operator=(const String& _other) {
    String temp(_other);
    *this = std::move(temp);
    return *this;
  }

  /// Move assignment operator
  String& operator=(String&& _other) noexcept {
    if (this == &_other) return *this;
    chars_ = std::move(_other.chars_);
    return *this;
  }

  /// Equal to operator
  bool operator==(const String& _other) const {
    return (strcmp(c_str(), _other.c_str()) == 0);
  }

  /// Equal to operator
  bool operator==(const char* _other) const {
    return (strcmp(c_str(), _other) == 0);
  }

  /// Less than operator
  bool operator<(const String& _other) const {
    return (strcmp(c_str(), _other.c_str()) < 0);
  }

  /// Returns the size of the underlying string.
  size_t size() const {
    if (!chars_) {
      return 0;
    }
    return static_cast<size_t>(strlen(chars_.get()));
  }

  /// Returns a std::string created from the underlying data.
  std::string str() const {
    if (!chars_) {
      return "NULL";
    }
    return std::string(chars_.get());
  }

  /// Returns a lower case version of this string.
  String to_lower() const {
    const auto tolower = [](const char c) { return std::tolower(c); };
    auto lower = String(*this);
    std::transform(lower.c_str(), lower.c_str() + lower.size(),
                   lower.chars_.get(), tolower);
    return lower;
  }

  /// Returns a upper case version of this string.
  String to_upper() const {
    const auto toupper = [](const char c) { return std::toupper(c); };
    auto upper = String(*this);
    std::transform(upper.c_str(), upper.c_str() + upper.size(),
                   upper.chars_.get(), toupper);
    return upper;
  }

 private:
  /// The underlying data.
  std::unique_ptr<char[]> chars_;
};

}  // namespace strings

#endif  // STRINGS_STRING_HPP_
