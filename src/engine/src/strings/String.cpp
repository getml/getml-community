// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "strings/String.hpp"

namespace strings {

String::String() : chars_(std::make_unique<char[]>(1)) {
  chars_.get()[0] = '\0';
}

String::String(const std::string& _str)
    : chars_(std::make_unique<char[]>(_str.size() + 1)) {
  std::copy(_str.begin(), _str.end(), chars_.get());
  chars_.get()[_str.size()] = '\0';
}

String::String(const uint64_t _size)
    : chars_(std::make_unique<char[]>(_size + 1)) {
  chars_.get()[_size] = '\0';
}

String::String(const char* _str) : String(_str, _str ? strlen(_str) : 0) {}

String::String(const char* _str, const size_t _size)
    : chars_(_str ? std::make_unique<char[]>(_size + 1) : nullptr) {
  if (chars_) {
    std::copy(_str, _str + _size, chars_.get());
    chars_.get()[_size] = '\0';
  }
}

String::String(const String& _other)
    : chars_(_other ? std::make_unique<char[]>(_other.size() + 1) : nullptr) {
  if (chars_) {
    strcpy(chars_.get(), _other.c_str());
  }
}

String::String(String&& _other) noexcept : chars_(std::move(_other.chars_)) {}

}  // namespace strings
