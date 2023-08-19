// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CAPNPROTO_OBJECTGETTER_HPP_
#define CAPNPROTO_OBJECTGETTER_HPP_

#include <Poco/CAPNPROTO/Object.h>

namespace capnproto {

struct ObjectGetter {
  /// Retrieves the object from the dynamic var.
  static Poco::CAPNPROTO::Object get_object(const Poco::Dynamic::Var& _var) {
    try {
      return _var.extract<Poco::CAPNPROTO::Object>();

    } catch (std::exception& _exp) {
    }

    try {
      const auto ptr = _var.extract<Poco::CAPNPROTO::Object::Ptr>();

      if (!ptr) {
        throw std::runtime_error("Poco::CAPNPROTO::Object::Ptr is NULL.");
      }

      return *ptr;
    } catch (std::exception& _exp) {
      throw std::runtime_error(
          "Expected a Poco::CAPNPROTO::Object or a "
          "Poco::CAPNPROTO::Object::Ptr.");
    }
  }
};

}  // namespace capnproto

#endif  // CAPNPROTO_OBJECTGETTER_HPP_
