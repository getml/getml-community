// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FLEXBUFFERS_WRITER_HPP_
#define FLEXBUFFERS_WRITER_HPP_

#include <flatbuffers/flexbuffers.h>

#include <exception>
#include <functional>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "flexbuffers/OutputArray.hpp"
#include "flexbuffers/OutputNull.hpp"
#include "flexbuffers/OutputObject.hpp"
#include "flexbuffers/OutputValue.hpp"
#include "flexbuffers/OutputVar.hpp"
#include "rfl/Ref.hpp"
#include "rfl/Result.hpp"

namespace flexbuffers {

struct Writer {
  using OutputArrayType = rfl::Ref<OutputArray>;
  using OutputObjectType = rfl::Ref<OutputObject>;
  using OutputVarType = rfl::Ref<OutputVar>;

  Writer() {}

  ~Writer() = default;

  void add(const OutputVarType _var, OutputArrayType* _arr) const noexcept {
    (*_arr)->push_back(_var);
  }

  OutputVarType empty_var() const noexcept {
    return rfl::Ref<OutputNull>::make();
  }

  template <class T>
  OutputVarType from_basic_type(const T _var) const noexcept {
    return rfl::Ref<OutputValue<T>>::make(_var);
  }

  OutputArrayType new_array() const noexcept {
    return rfl::Ref<OutputArray>::make();
  }

  OutputObjectType new_object() const noexcept {
    return rfl::Ref<OutputObject>::make();
  }

  bool is_empty(OutputVarType* _var) const noexcept {
    return (*_var)->is_null();
  }

  void set_field(const std::string& _name, const OutputVarType& _var,
                 OutputObjectType* _obj) const noexcept {
    (*_obj)->push_back(_name, _var);
  }
};

};  // namespace flexbuffers

#endif  // FLEXBUFFERS_PARSER_HPP_
