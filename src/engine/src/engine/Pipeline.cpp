#include "engine/pipelines/Pipeline.hpp"

// ----------------------------------------------------------------------------

#include "engine/pipelines/DataFrameModifier.hpp"
#include "engine/pipelines/PlaceholderMaker.hpp"
#include "engine/pipelines/Staging.hpp"
#include "transpilation/SQLDialectParser.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace pipelines {

Pipeline::Pipeline(const Poco::JSON::Object& _obj)
    : allow_http_(false),
      creation_time_(make_creation_time()),
      include_categorical_(JSON::get_value<bool>(_obj, "include_categorical_")),
      obj_(_obj),
      scores_(fct::Ref<const metrics::Scores>::make()) {}

// ----------------------------------------------------------------------------

Pipeline::~Pipeline() = default;

// ------------------------------------------------------------------------

std::pair<fct::Ref<const helpers::Placeholder>,
          fct::Ref<const std::vector<std::string>>>
Pipeline::make_placeholder() const {
  const auto data_model = *JSON::get_object(obj(), "data_model_");

  const auto placeholder = fct::Ref<const helpers::Placeholder>::make(
      PlaceholderMaker::make_placeholder(data_model, "t1"));

  const auto peripheral_names = fct::Ref<const std::vector<std::string>>::make(
      PlaceholderMaker::make_peripheral(*placeholder));

  return std::make_pair(placeholder, peripheral_names);
}

// ----------------------------------------------------------------------

fct::Ref<std::vector<std::string>> Pipeline::parse_peripheral() const {
  auto peripheral = fct::Ref<std::vector<std::string>>::make();

  const auto arr = JSON::get_array(obj(), "peripheral_");

  assert_true(arr);

  for (size_t i = 0; i < arr->size(); ++i) {
    const auto ptr = arr->getObject(i);

    if (!ptr) {
      throw std::runtime_error("Element " + std::to_string(i) +
                               " in peripheral_ is not a proper JSON "
                               "object.");
    }

    const auto name = JSON::get_value<std::string>(*ptr, "name_");

    peripheral->push_back(name);
  }

  return peripheral;
}

// ----------------------------------------------------------------------

std::shared_ptr<std::string> Pipeline::parse_population() const {
  const auto ptr = JSON::get_object(obj(), "data_model_");

  if (!ptr) {
    throw std::runtime_error("'population_' is not a proper JSON object!");
  }

  const auto name = JSON::get_value<std::string>(*ptr, "name_");

  return std::make_shared<std::string>(name);
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Pipeline::to_monitor(
    const helpers::StringIterator& _categories,
    const std::string& _name) const {
  const auto to_json_obj = [](const containers::Schema& _schema) {
    return _schema.to_json_obj();
  };

  auto feature_learners = Poco::JSON::Array::Ptr(new Poco::JSON::Array());

  Poco::JSON::Object json_obj;

  json_obj.set("name_", _name);

  json_obj.set("allow_http_", allow_http());

  json_obj.set("creation_time_", creation_time());

  json_obj.set("feature_learners_",
               JSON::get_array(obj(), "feature_learners_"));

  json_obj.set("feature_selectors_",
               JSON::get_array(obj(), "feature_selectors_"));

  json_obj.set("data_model_", JSON::get_object(obj(), "data_model_"));

  json_obj.set("predictors_", JSON::get_array(obj(), "predictors_"));

  json_obj.set("preprocessors_", JSON::get_array(obj(), "preprocessors_"));

  json_obj.set("tags_", JSON::get_array(obj(), "tags_"));

  if (fitted()) {
    json_obj.set("num_features_", fitted()->num_features());

    json_obj.set("peripheral_schema_",
                 fct::collect::array(*fitted()->peripheral_schema_ |
                                     VIEWS::transform(to_json_obj)));

    json_obj.set("population_schema_",
                 fitted()->population_schema_->to_json_obj());

    json_obj.set("targets_", JSON::vector_to_array(fitted()->targets()));
  }

  auto scores_obj = scores().to_json_obj();

  const auto make_staging_table_colname =
      [](const std::string& _colname) -> std::string {
    return transpilation::SQLite3Generator().make_staging_table_colname(
        _colname);
  };

  const auto modified_names = helpers::Macros::modify_colnames(
      scores().feature_names(), make_staging_table_colname);

  const auto feature_names = JSON::vector_to_array(modified_names);

  scores_obj.set("feature_names_", feature_names);

  json_obj.set("scores_", scores_obj);

  return json_obj;
}

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine
