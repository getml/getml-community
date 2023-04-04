// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef JSON_OBJECTGETTER_HPP_
#define JSON_OBJECTGETTER_HPP_

#include <Poco/JSON/Object.h>

namespace json {

struct ObjectGetter {
  /// Retrieves the object from the dynamic var.
  static Poco::JSON::Object get_object(const Poco::Dynamic::Var& _var) {
    try {
      return _var.extract<Poco::JSON::Object>();

    } catch (std::exception& _exp) {
    }

    try {
      const auto ptr = _var.extract<Poco::JSON::Object::Ptr>();

      if (!ptr) {
        throw std::runtime_error("Poco::JSON::Object::Ptr is NULL.");
      }

      return *ptr;
    } catch (std::exception& _exp) {
      throw std::runtime_error(
          "Expected a Poco::JSON::Object or a Poco::JSON::Object::Ptr.");
    }
  }
};

}  // namespace json

#endif  // JSON_OBJECTGETTER_HPP_
