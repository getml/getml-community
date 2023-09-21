// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/pipelines/Pipeline.hpp"

#include "engine/pipelines/make_placeholder.hpp"
#include "engine/pipelines/staging.hpp"
#include "fct/collect.hpp"
#include "rfl/Field.hpp"
#include "rfl/replace.hpp"
#include "rfl/to_named_tuple.hpp"
#include "transpilation/SQLDialectParser.hpp"

namespace engine {
namespace pipelines {

Pipeline::Pipeline(const rfl::Ref<const commands::Pipeline>& _obj)
    : allow_http_(false),
      creation_time_(make_creation_time()),
      include_categorical_(_obj->include_categorical()),
      obj_(_obj),
      scores_(rfl::Ref<const metrics::Scores>::make()) {}

// ----------------------------------------------------------------------------

Pipeline::Pipeline(const commands::Pipeline& _obj)
    : Pipeline(rfl::Ref<const commands::Pipeline>::make(_obj)) {}

// ----------------------------------------------------------------------------

Pipeline::~Pipeline() = default;

// ------------------------------------------------------------------------

std::pair<rfl::Ref<const helpers::Placeholder>,
          rfl::Ref<const std::vector<std::string>>>
Pipeline::make_placeholder() const {
  const auto& data_model = *obj().data_model();

  const auto placeholder = make_placeholder::make_placeholder(data_model, "t1");

  const auto peripheral_names = rfl::Ref<const std::vector<std::string>>::make(
      make_placeholder::make_peripheral(*placeholder));

  return std::make_pair(placeholder, peripheral_names);
}

// ----------------------------------------------------------------------

rfl::Ref<std::vector<std::string>> Pipeline::parse_peripheral() const {
  const auto get_name = [](const auto& _p) -> std::string {
    return rfl::get<"name_">(_p.val_);
  };
  return rfl::Ref<std::vector<std::string>>::make(
      fct::collect::vector(*obj().peripheral() | VIEWS::transform(get_name)));
}

// ----------------------------------------------------------------------

std::shared_ptr<std::string> Pipeline::parse_population() const {
  return std::make_shared<std::string>(obj().data_model()->val_.get<"name_">());
}

// ----------------------------------------------------------------------------

MonitorSummary Pipeline::to_monitor(const helpers::StringIterator& _categories,
                                    const std::string& _name) const {
  const auto not_fitted = MonitorSummaryNotFitted{
      .pipeline = rfl::replace(obj(), rfl::make_field<"name_">(_name)),
      .allow_http = allow_http(),
      .creation_time = creation_time()};

  if (!fitted()) {
    return not_fitted;
  }

  return MonitorSummaryFitted{.not_fitted = not_fitted,
                              .num_features = fitted()->num_features(),
                              .peripheral_schema = fitted()->peripheral_schema_,
                              .population_schema = fitted()->population_schema_,
                              .targets = fitted()->targets()};
}

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine
