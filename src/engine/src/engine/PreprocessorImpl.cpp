// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "engine/preprocessors/PreprocessorImpl.hpp"

namespace engine {
namespace preprocessors {
// ----------------------------------------------------

std::vector<std::shared_ptr<helpers::ColumnDescription>>
PreprocessorImpl::from_array(const Poco::JSON::Array::Ptr& _arr) {
  assert_true(_arr);

  auto desc = std::vector<std::shared_ptr<helpers::ColumnDescription>>();

  for (size_t i = 0; i < _arr->size(); ++i) {
    const auto ptr = _arr->getObject(i);

    assert_true(ptr);

    desc.push_back(std::make_shared<helpers::ColumnDescription>(*ptr));
  }

  return desc;
}

// ----------------------------------------------------

std::vector<std::string> PreprocessorImpl::retrieve_names(
    const std::string& _marker, const size_t _table,
    const std::vector<std::shared_ptr<helpers::ColumnDescription>>& _desc) {
  const auto table = std::to_string(_table);

  auto names = std::vector<std::string>();

  for (const auto& ptr : _desc) {
    assert_true(ptr);

    if (ptr->marker_ == _marker && ptr->table_ == table) {
      names.push_back(ptr->name_);
    }
  }

  return names;
}

// ----------------------------------------------------

Poco::JSON::Array::Ptr PreprocessorImpl::to_array(
    const std::vector<std::shared_ptr<helpers::ColumnDescription>>& _desc) {
  auto arr = Poco::JSON::Array::Ptr(new Poco::JSON::Array());

  for (const auto& ptr : _desc) {
    assert_true(ptr);
    arr->add(ptr->to_json_obj());
  }

  return arr;
}

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
