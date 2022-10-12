// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include <string>
#include <vector>

#include "engine/JSON.hpp"
#include "fct/join.hpp"
#include "helpers/Schema.hpp"

#ifndef ENGINE_CONTAINERS_ROLES_HPP_
#define ENGINE_CONTAINERS_ROLES_HPP_

namespace engine {
namespace containers {

struct Roles {
  /// Retrieves the roles from the schema.
  static Roles from_schema(const helpers::Schema& _schema) {
    return Roles{.categorical_ = _schema.categoricals_,
                 .join_key_ = _schema.join_keys_,
                 .numerical_ = fct::join::vector<std::string>(
                     {_schema.discretes_, _schema.numericals_}),
                 .target_ = _schema.targets_,
                 .text_ = _schema.text_,
                 .time_stamp_ = _schema.time_stamps_,
                 .unused_float_ = _schema.unused_floats_,
                 .unused_string_ = _schema.unused_strings_};
  }

  /// Transpiles the roles to a JSON object.
  Poco::JSON::Object::Ptr to_json_obj() const {
    auto obj = Poco::JSON::Object::Ptr(new Poco::JSON::Object());

    obj->set("categorical", JSON::vector_to_array_ptr(categorical_));
    obj->set("join_key", JSON::vector_to_array_ptr(join_key_));
    obj->set("numerical", JSON::vector_to_array_ptr(numerical_));
    obj->set("target", JSON::vector_to_array_ptr(target_));
    obj->set("text", JSON::vector_to_array_ptr(text_));
    obj->set("time_stamp", JSON::vector_to_array_ptr(time_stamp_));
    obj->set("unused_float", JSON::vector_to_array_ptr(unused_float_));
    obj->set("unused_string", JSON::vector_to_array_ptr(unused_string_));

    return obj;
  }

  /// The names of the categorical columns.
  const std::vector<std::string> categorical_;

  /// The names of the join keys.
  const std::vector<std::string> join_key_;

  /// The names of the numerical columns.
  const std::vector<std::string> numerical_;

  /// The names of the target columns.
  const std::vector<std::string> target_;

  /// The names of the text columns.
  const std::vector<std::string> text_;

  /// The names of the time stamp columns.
  const std::vector<std::string> time_stamp_;

  /// The names of the unused float columns.
  const std::vector<std::string> unused_float_;

  /// The names of the unused string columns.
  const std::vector<std::string> unused_string_;
};

}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_ROLES_HPP_
