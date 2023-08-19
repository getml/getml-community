// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CAPTNPROTO_READER_HPP_
#define CAPTNPROTO_READER_HPP_

#include <capnp/dynamic.h>

#include <exception>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "fct/Result.hpp"
#include "fct/always_false.hpp"

namespace capnproto {

struct Reader {
  using InputArrayType = capnp::DynamicList::Reader;
  using InputObjectType = capnp::DynamicStruct::Reader;
  using InputVarType = capnp::DynamicValue::Reader;

  using TypeEnum = capnp::DynamicValue::Type;

  fct::Result<InputVarType> get_field(const std::string& _name,
                                      InputObjectType* _obj) const noexcept {
    if (!_obj->has(_name)) {
      return fct::Error("Object contains no field named '" + _name + "'.");
    }
    return _obj->get(_name);
  }

  bool is_empty(InputVarType* _var) const noexcept {
    // NULL values are not possible in capnproto.
    return false;
  }

  template <class T>
  fct::Result<T> to_basic_type(InputVarType* _var) const noexcept {
    const auto var_type = _var->getType();
    if constexpr (std::is_same<std::decay_t<T>, std::string>()) {
      if (var_type != TypeEnum::TEXT) {
        return fct::Error("Could not cast to string.");
      }
      return std::string(_var->as<capnp::Text>().cStr());
    } else if constexpr (std::is_same<std::decay_t<T>, bool>()) {
      if (var_type != TypeEnum::BOOL) {
        return fct::Error("Could not cast to boolean.");
      }
      return _var->as<bool>();
    } else if constexpr (std::is_floating_point<std::decay_t<T>>()) {
      if (var_type != TypeEnum::FLOAT) {
        return fct::Error("Could not cast to double.");
      }
      return static_cast<T>(_var->as<double>());
    } else if constexpr (std::is_unsigned<std::decay_t<T>>() &&
                         std::is_integral<std::decay_t<T>>()) {
      if (var_type != TypeEnum::UINT) {
        return fct::Error("Could not cast to unsigned int.");
      }
      return static_cast<T>(_var->as<std::uint64_t>());
    } else if constexpr (std::is_integral<std::decay_t<T>>()) {
      if (var_type != TypeEnum::INT) {
        return fct::Error("Could not cast to signed int.");
      }
      return static_cast<T>(_var->as<std::int64_t>());
    } else {
      static_assert(fct::always_false_v<T>, "Unsupported type.");
    }
  }

  fct::Result<InputArrayType> to_array(InputVarType* _var) const noexcept {
    const auto var_type = _var->getType();
    if (var_type != TypeEnum::LIST) {
      return fct::Error("Could not cast to list!");
    }
    return _var->as<InputArrayType>();
  }

  std::map<std::string, InputVarType> to_map(
      InputObjectType* _obj) const noexcept {
    std::map<std::string, InputVarType> m;

    for (const auto& field : _obj->getSchema().getFields()) {
      if (!_obj->has(field)) {
        continue;
      }
      m[field.getProto().getName().cStr()] = _obj->get(field);
    }

    return m;
  }

  fct::Result<InputObjectType> to_object(InputVarType* _var) const noexcept {
    const auto var_type = _var->getType();
    if (var_type != TypeEnum::STRUCT) {
      return fct::Error("Could not cast to struct!");
    }
    return _var->as<InputObjectType>();
  }

  std::vector<InputVarType> to_vec(InputArrayType* _arr) const noexcept {
    std::vector<InputVarType> vec;
    for (const auto& elem : *_arr) {
      vec.push_back(elem);
    }
    return vec;
  }
};

};  // namespace capnproto

#endif  // CAPTNPROTO_PARSER_HPP_
