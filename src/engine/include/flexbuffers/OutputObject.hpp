// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FLEXBUFFERS_OUTPUTOBJECT_HPP_
#define FLEXBUFFERS_OUTPUTOBJECT_HPP_

#include <flatbuffers/flexbuffers.h>

#include <optional>
#include <string>
#include <utility>

#include "fct/Ref.hpp"
#include "flexbuffers/OutputVar.hpp"

namespace flexbuffers {

class OutputObject : public OutputVar {
 public:
  OutputObject() {}

  ~OutputObject() = default;

  /// Inserts all elements into the builder.
  void insert(const std::optional<std::string>& _key,
              flexbuffers::Builder* _fbb) final {
    const auto start = _key ? _fbb->StartMap(_key->c_str()) : _fbb->StartMap();
    for (const auto& [name, elem] : vars_) {
      elem->insert(name, _fbb);
    }
    _fbb->EndMap(start);
  };

  /// Whether this is null.
  bool is_null() const final { return false; }

  /// Adds a new element to the vector.
  void push_back(const std::string& _name, const fct::Ref<OutputVar>& _var) {
    vars_.push_back(std::make_pair(_name, _var));
  }

 private:
  /// The underlying variables.
  std::vector<std::pair<std::string, fct::Ref<OutputVar>>> vars_;
};

}  // namespace flexbuffers

#endif
