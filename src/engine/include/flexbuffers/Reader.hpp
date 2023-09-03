// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FLEXBUFFERS_READER_HPP_
#define FLEXBUFFERS_READER_HPP_

#include <flatbuffers/flexbuffers.h>

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

namespace flexbuffers {

struct Reader {
  using InputArrayType = flexbuffers::Vector;
  using InputObjectType = flexbuffers::Map;
  using InputVarType = flexbuffers::Reference;

  template <class T, class = void>
  struct has_from_flexbuffers : std::false_type {};

  template <class T>
  struct has_from_flexbuffers<
      T, std::enable_if_t<std::is_invocable_r<T, decltype(T::from_flexbuffers),
                                              InputVarType>::value>>
      : std::true_type {};

  template <class T>
  static constexpr bool has_custom_constructor = has_from_flexbuffers<T>::value;

  fct::Result<InputVarType> get_field(const std::string& _name,
                                      InputObjectType* _obj) const noexcept {
    const auto keys = _obj->Keys();
    for (size_t i = 0; i < keys.size(); ++i) {
      if (_name == keys[i].AsString().c_str()) {
        return _obj->Values()[i];
      }
    }
    return fct::Error("Map does not contain any element called '" + _name +
                      "'.");
  }

  bool is_empty(InputVarType* _var) const noexcept { return _var->IsNull(); }

  template <class T>
  fct::Result<T> to_basic_type(InputVarType* _var) const noexcept {
    if constexpr (std::is_same<std::decay_t<T>, std::string>()) {
      if (!_var->IsString()) {
        return fct::Error("Could not cast to string.");
      }
      return std::string(_var->AsString().c_str());
    } else if constexpr (std::is_same<std::decay_t<T>, bool>()) {
      if (!_var->IsBool()) {
        return fct::Error("Could not cast to boolean.");
      }
      return _var->AsBool();
    } else if constexpr (std::is_floating_point<std::decay_t<T>>()) {
      if (!_var->IsNumeric()) {
        return fct::Error("Could not cast to double.");
      }
      return static_cast<T>(_var->AsDouble());
    } else if constexpr (std::is_integral<std::decay_t<T>>()) {
      if (!_var->IsNumeric()) {
        return fct::Error("Could not cast to int.");
      }
      return static_cast<T>(_var->AsInt64());
    } else {
      static_assert(fct::always_false_v<T>, "Unsupported type.");
    }
  }

  fct::Result<InputArrayType> to_array(InputVarType* _var) const noexcept {
    // Necessary, because we write empty vectors as null.
    if (_var->IsNull()) {
      return flexbuffers::Vector::EmptyVector();
    }
    if (!_var->IsVector()) {
      return fct::Error("Could not cast to Vector.");
    }
    return _var->AsVector();
  }

  std::map<std::string, InputVarType> to_map(
      InputObjectType* _obj) const noexcept {
    std::map<std::string, InputVarType> m;

    const auto keys = _obj->Keys();
    const auto values = _obj->Values();
    const auto size = std::min(keys.size(), values.size());

    for (size_t i = 0; i < size; ++i) {
      m[keys[i].AsString().c_str()] = values[i];
    }

    return m;
  }

  fct::Result<InputObjectType> to_object(InputVarType* _var) const noexcept {
    // Necessary, because we write empty maps as null.
    if (_var->IsNull()) {
      return flexbuffers::Map::EmptyMap();
    }
    if (!_var->IsMap()) {
      return fct::Error("Could not cast to Map!");
    }
    return _var->AsMap();
  }

  std::vector<InputVarType> to_vec(InputArrayType* _arr) const noexcept {
    const auto size = _arr->size();
    std::vector<InputVarType> vec;
    for (size_t i = 0; i < size; ++i) {
      vec.push_back((*_arr)[i]);
    }
    return vec;
  }
};

};  // namespace flexbuffers

#endif  // CAPTNPROTO_PARSER_HPP_
