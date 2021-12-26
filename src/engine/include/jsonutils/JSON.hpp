#ifndef JSONUTILS_JSON_HPP_
#define JSONUTILS_JSON_HPP_

// ------------------------------------------------------------------------

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

// ------------------------------------------------------------------------

#include <cmath>
#include <fstream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

// ------------------------------------------------------------------------

#include "debug/debug.hpp"

// ------------------------------------------------------------------------
namespace jsonutils {
// ------------------------------------------------------------------------

struct JSON {
  /// Transforms a Poco array to a vector
  template <typename T>
  static std::vector<T> array_to_vector(const Poco::JSON::Array::Ptr _array) {
    if (_array.isNull()) {
      throw std::runtime_error(
          "Error in JSON: Array does not exist or is not an array!");
    }

    std::vector<T> vec;

    for (auto& val : *_array) {
      vec.push_back(val);
    }

    return vec;
  }

  /// Gets an array from a JSON object or throws.
  static Poco::JSON::Array::Ptr get_array(const Poco::JSON::Object& _obj,
                                          const std::string& _key) {
    auto arr = _obj.getArray(_key);

    if (!arr) {
      throw std::runtime_error("Array named '" + _key + "' not found!");
    }

    return arr;
  }

  /// Gets an array from a JSON object or throws.
  static Poco::JSON::Object::Ptr get_object(const Poco::JSON::Object& _obj,
                                            const std::string& _key) {
    auto ptr = _obj.getObject(_key);

    if (!ptr) {
      throw std::runtime_error("Object named '" + _key + "' not found!");
    }

    return ptr;
  }

  /// Gets an array of JSON objects or throws.
  static Poco::JSON::Array::Ptr get_object_array(const Poco::JSON::Object& _obj,
                                                 const std::string& _key) {
    const auto arr = get_array(_obj, _key);

    for (size_t i = 0; i < arr->size(); ++i) {
      const auto ptr = arr->getObject(i);

      if (!ptr) {
        throw std::invalid_argument("Element " + std::to_string(i) +
                                    " in array '" + _key +
                                    "' is not a proper JSON object.");
      }
    }

    return arr;
  }

  /// Gets a vector containing a complex type.
  template <typename T>
  static std::vector<T> get_type_vector(const Poco::JSON::Object& _obj,
                                        const std::string& _key) {
    auto arr = get_object_array(_obj, _key);

    auto vec = std::vector<T>();

    for (size_t i = 0; i < arr->size(); ++i) {
      const auto ptr = arr->getObject(i);
      vec.push_back(T(*ptr));
    }

    return vec;
  }

  /// Gets a value from a JSON object or throws.
  template <typename T>
  static T get_value(const Poco::JSON::Object& _obj, const std::string& _key) {
    if (!_obj.has(_key)) {
      throw std::runtime_error("Value named '" + _key + "' not found!");
    }

    return _obj.get(_key).convert<T>();
  }

  /// Loads a JSON object from disc.
  static Poco::JSON::Object load(const std::string& _fname) {
    std::ifstream input(_fname);

    std::stringstream json;

    std::string line;

    if (input.is_open()) {
      while (std::getline(input, line)) {
        json << line;
      }

      input.close();
    } else {
      throw std::invalid_argument("File '" + _fname + "' not found!");
    }

    const auto ptr = Poco::JSON::Parser()
                         .parse(json.str())
                         .extract<Poco::JSON::Object::Ptr>();

    if (!ptr) {
      throw std::runtime_error("JSON file did not contain an object!");
    }

    return *ptr;
  }

  /// Transforms a map into an object.
  template <typename KeyType, typename ValueType>
  static Poco::JSON::Object::Ptr map_to_object(
      const std::map<KeyType, ValueType>& _map) {
    Poco::JSON::Object::Ptr obj(new Poco::JSON::Object());

    for (const auto& [key, value] : _map) {
      obj->set(std::to_string(key), value);
    }

    return obj;
  }

  /// Expresses JSON object as JSON string
  static std::string stringify(const Poco::JSON::Object& _obj) {
    std::stringstream json;

    _obj.stringify(json);

    return json.str();
  }

  /// Transforms a vector to a Poco array
  template <typename T>
  static Poco::JSON::Array vector_to_array(const std::vector<T>& _vector) {
    Poco::JSON::Array arr;

    for (T elem : _vector) {
      if constexpr (std::is_floating_point<T>()) {
        if (std::isnan(elem) || std::isinf(elem)) {
          arr.add(0.0);
          continue;
        }
      }

      arr.add(Poco::Dynamic::Var(elem));
    }

    return arr;
  }

  /// Transforms a vector to a Poco array containing objects
  template <typename T>
  static Poco::JSON::Array::Ptr vector_to_object_array_ptr(
      const std::vector<T>& _vector) {
    auto arr = Poco::JSON::Array::Ptr(new Poco::JSON::Array());

    for (const auto& val : _vector) {
      arr->add(val.to_json_obj());
    }

    return arr;
  }

  /// Transforms a vector to a Poco array
  template <typename T>
  static Poco::JSON::Array::Ptr vector_to_array_ptr(
      const std::vector<T>& _vector) {
    return Poco::JSON::Array::Ptr(
        new Poco::JSON::Array(JSON::vector_to_array(_vector)));
  }

  // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace jsonutils

#endif  // JSONUTILS_JSON_HPP_
