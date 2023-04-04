// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef JSON_ARRAYGETTER_HPP_
#define JSON_ARRAYGETTER_HPP_

#include <Poco/JSON/Array.h>

namespace json {

struct ArrayGetter {
  /// Retrieves the array from the dynamic var.
  static Poco::JSON::Array get_array(const Poco::Dynamic::Var& _var) {
    try {
      return _var.extract<Poco::JSON::Array>();

    } catch (std::exception& _exp) {
    }

    try {
      const auto ptr = _var.extract<Poco::JSON::Array::Ptr>();

      if (!ptr) {
        throw std::runtime_error("Poco::JSON::Array::Ptr is NULL.");
      }

      return *ptr;
    } catch (std::exception& _exp) {
      throw std::runtime_error(
          "Expected a Poco::JSON::Array or a Poco::JSON::Array::Ptr.");
    }
  }
};

}  // namespace json

#endif  // JSON_ARRAYGETTER_HPP_
