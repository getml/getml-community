// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CAPNPROTO_WRITER_HPP_
#define CAPNPROTO_WRITER_HPP_

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
#include "parsing/Parser.hpp"

namespace capnproto {

struct Writer {
  using OutputArrayType = capnp::DynamicList;
  using OutputObjectType = capnp::DynamicStruct;
  using OutputVarType = capnp::DynamicValue;

  Writer(yycapnproto_mut_doc* _doc) : doc_(_doc) {}

  ~Writer() = default;

  void add(const OutputVarType _var, OutputArrayType* _arr) const noexcept {
    yycapnproto_mut_arr_add_val(_arr->val_, _var.val_);
  }

  OutputVarType empty_var() const noexcept {
    return OutputVarType(yycapnproto_mut_null(doc_));
  }

  template <class T>
  OutputVarType from_basic_type(const T _var) const noexcept {
    if constexpr (std::is_same<std::decay_t<T>, std::string>()) {
      return OutputVarType(yycapnproto_mut_strcpy(doc_, _var.c_str()));
    } else if constexpr (std::is_same<std::decay_t<T>, bool>()) {
      return OutputVarType(yycapnproto_mut_bool(doc_, _var));
    } else if constexpr (std::is_floating_point<std::decay_t<T>>()) {
      return OutputVarType(
          yycapnproto_mut_real(doc_, static_cast<double>(_var)));
    } else {
      return OutputVarType(yycapnproto_mut_int(doc_, static_cast<int>(_var)));
    }
  }

  OutputArrayType new_array() const noexcept {
    return OutputArrayType(yycapnproto_mut_arr(doc_));
  }

  OutputObjectType new_object() const noexcept {
    return OutputObjectType(yycapnproto_mut_obj(doc_));
  }

  bool is_empty(OutputVarType* _var) const noexcept {
    return yycapnproto_mut_is_null(_var->val_);
  }

  void set_field(const std::string& _name, const OutputVarType& _var,
                 OutputObjectType* _obj) const noexcept {
    yycapnproto_mut_obj_add(
        _obj->val_, yycapnproto_mut_strcpy(doc_, _name.c_str()), _var.val_);
  }

  yycapnproto_mut_doc* doc_;
};

};  // namespace capnproto

#endif  // CAPNPROTO_PARSER_HPP_
