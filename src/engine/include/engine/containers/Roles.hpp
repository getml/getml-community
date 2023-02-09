// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include <string>
#include <vector>

#include "fct/NamedTuple.hpp"
#include "fct/join.hpp"
#include "helpers/Schema.hpp"
#include "json/json.hpp"

#ifndef ENGINE_CONTAINERS_ROLES_HPP_
#define ENGINE_CONTAINERS_ROLES_HPP_

namespace engine {
namespace containers {

struct Roles {
  /// The names of the categorical columns
  using f_categorical = fct::Field<"categorical", std::vector<std::string>>;

  /// The names of the join keys
  using f_join_key = fct::Field<"join_key", std::vector<std::string>>;

  /// The names of the numerical columns
  using f_numerical = fct::Field<"numerical", std::vector<std::string>>;

  /// The names of the target columns
  using f_target = fct::Field<"target", std::vector<std::string>>;

  /// The names of the text columns
  using f_text = fct::Field<"text", std::vector<std::string>>;

  /// The names of the time stamp columns
  using f_time_stamp = fct::Field<"time_stamp", std::vector<std::string>>;

  /// The names of the unused float columns
  using f_unused_float = fct::Field<"unused_float", std::vector<std::string>>;

  /// The names of the unused string columns
  using f_unused_string = fct::Field<"unused_string", std::vector<std::string>>;

  using NamedTupleType =
      fct::NamedTuple<f_categorical, f_join_key, f_numerical, f_target, f_text,
                      f_time_stamp, f_unused_float, f_unused_string>;

  /// Retrieves the roles from the schema.
  static Roles from_schema(const helpers::Schema& _schema) {
    return Roles{.val_ = f_categorical(_schema.categoricals()) *
                         f_join_key(_schema.join_keys()) *
                         f_numerical(fct::join::vector<std::string>(
                             {_schema.discretes(), _schema.numericals()})) *
                         f_target(_schema.targets()) * f_text(_schema.text()) *
                         f_time_stamp(_schema.time_stamps()) *
                         f_unused_float(_schema.unused_floats()) *
                         f_unused_string(_schema.unused_strings())};
  }

  /// Transpiles the roles to a JSON object.
  Poco::JSON::Object::Ptr to_json_obj() const {
    return json::Parser<Roles>::to_json(*this);
  }

  /// Normally used for recursion, but here it is used
  /// to support the static constructors.
  const NamedTupleType val_;
};

}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_ROLES_HPP_
