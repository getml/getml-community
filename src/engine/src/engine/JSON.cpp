#include "engine/JSON.hpp"

namespace engine {
// ------------------------------------------------------------------------

std::vector<Poco::JSON::Object::Ptr> JSON::array_to_obj_vector(
    const Poco::JSON::Array::Ptr _arr) {
  if (!_arr) {
    throw std::runtime_error("_arr is a nullptr!");
  }

  std::vector<Poco::JSON::Object::Ptr> vec;

  for (size_t i = 0; i < _arr->size(); ++i) {
    const auto ptr = _arr->getObject(i);

    if (!ptr) {
      throw std::runtime_error("Element " + std::to_string(i) +
                               " in array is not a proper JSON "
                               "object.");
    }

    vec.push_back(ptr);
  }

  return vec;
}

// ------------------------------------------------------------------------

/// Gets an array from a JSON object or throws.
Poco::JSON::Array::Ptr JSON::get_array(const Poco::JSON::Object& _obj,
                                       const std::string& _key) {
  auto arr = _obj.getArray(_key);

  if (!arr) {
    throw std::runtime_error("Array named '" + _key + "' not found!");
  }

  return arr;
}

// ------------------------------------------------------------------------

Poco::JSON::Object::Ptr JSON::get_object(const Poco::JSON::Object& _obj,
                                         const std::string& _key) {
  auto ptr = _obj.getObject(_key);

  if (!ptr) {
    throw std::runtime_error("Object named '" + _key + "' not found!");
  }

  return ptr;
}

// ------------------------------------------------------------------------

std::string JSON::stringify(const Poco::JSON::Object& _obj) {
  std::stringstream json;

  _obj.stringify(json);

  return json.str();
}

// ------------------------------------------------------------------------
}  // namespace engine
