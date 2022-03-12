#include "fastprop/subfeatures/FastPropContainer.hpp"

namespace fastprop {
namespace subfeatures {
// ----------------------------------------------------------------------------

FastPropContainer::FastPropContainer(
    const std::shared_ptr<const algorithm::FastProp>& _fast_prop,
    const std::shared_ptr<const Subcontainers>& _subcontainers)
    : fast_prop_(_fast_prop), subcontainers_(_subcontainers) {
  assert_true(subcontainers_);
}

// ----------------------------------------------------------------------------

FastPropContainer::FastPropContainer(const Poco::JSON::Object& _obj)
    : fast_prop_(_obj.has("fast_prop_")
                     ? std::make_shared<algorithm::FastProp>(
                           *jsonutils::JSON::get_object(_obj, "fast_prop_"))
                     : std::shared_ptr<algorithm::FastProp>()),
      subcontainers_(FastPropContainer::parse_subcontainers(_obj)) {
  assert_true(subcontainers_);
}

// ----------------------------------------------------------------------------

FastPropContainer::~FastPropContainer() = default;

// ----------------------------------------------------------------------------

std::shared_ptr<typename FastPropContainer::Subcontainers>
FastPropContainer::parse_subcontainers(const Poco::JSON::Object& _obj) {
  auto arr = _obj.getArray("subcontainers_");

  throw_unless(arr, "Expected array called 'subcontainers_'");

  const auto subcontainers = std::make_shared<Subcontainers>();

  for (size_t i = 0; i < arr->size(); ++i) {
    auto ptr = arr->getObject(i);

    if (ptr) {
      subcontainers->push_back(std::make_shared<const FastPropContainer>(*ptr));
    } else {
      subcontainers->push_back(nullptr);
    }
  }

  return subcontainers;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object::Ptr FastPropContainer::to_json_obj() const {
  const auto transform_subcontainers = [](const Subcontainers& _subcontainers) {
    auto arr = Poco::JSON::Array::Ptr(new Poco::JSON::Array());

    for (const auto& s : _subcontainers) {
      if (s) {
        arr->add(s->to_json_obj());
      } else {
        arr->add("");
      }
    }

    return arr;
  };

  auto obj = Poco::JSON::Object::Ptr(new Poco::JSON::Object());

  if (fast_prop_) {
    auto fp = Poco::JSON::Object::Ptr(
        new Poco::JSON::Object(fast_prop_->to_json_obj()));
    obj->set("fast_prop_", fp);
  }

  obj->set("subcontainers_", transform_subcontainers(*subcontainers_));

  return obj;
}

// ----------------------------------------------------------------------------

void FastPropContainer::to_sql(
    const helpers::StringIterator& _categories,
    const helpers::VocabularyTree& _vocabulary,
    const std::shared_ptr<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator,
    const std::string& _prefix, const bool _subfeatures,
    std::vector<std::string>* _sql) const {
  if (has_fast_prop()) {
    const auto features =
        fast_prop().to_sql(_categories, _vocabulary, _sql_dialect_generator,
                           _prefix, 0, _subfeatures);

    _sql->insert(_sql->end(), features.begin(), features.end());

    if (_subfeatures) {
      const auto to_feature_name =
          [&_prefix](const size_t _feature_num) -> std::string {
        return "feature_" + _prefix + std::to_string(_feature_num + 1);
      };

      const auto iota = fct::iota<size_t>(0, fast_prop().num_features());

      const auto autofeatures = fct::collect::vector<std::string>(
          iota | VIEWS::transform(to_feature_name));

      const auto main_table =
          transpilation::SQLGenerator::make_staging_table_name(
              fast_prop().placeholder().name());

      assert_true(_sql_dialect_generator);

      _sql->push_back(_sql_dialect_generator->make_feature_table(
                          main_table, autofeatures, {}, {}, {},
                          "_" + _prefix + "PROPOSITIONALIZATION") +
                      "\n");
    }
  }

  if (_subfeatures) {
    for (size_t i = 0; i < size(); ++i) {
      if (subcontainers(i)) {
        subcontainers(i)->to_sql(
            _categories, _vocabulary, _sql_dialect_generator,
            _prefix + std::to_string(i + 1) + "_", _subfeatures, _sql);
      }
    }
  }
}

// ----------------------------------------------------------------------------
}  // namespace subfeatures
}  // namespace fastprop
