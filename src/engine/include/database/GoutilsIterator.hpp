#ifndef DATABASE_GOUTILSITERATOR_HPP_
#define DATABASE_GOUTILSITERATOR_HPP_

// -----------------------------------------------------------------------------

#include <cmath>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

// -----------------------------------------------------------------------------

#include <goutils.hpp>

// -----------------------------------------------------------------------------

#include "debug/debug.hpp"

// -----------------------------------------------------------------------------

#include "database/Float.hpp"
#include "database/Getter.hpp"
#include "database/Int.hpp"
#include "database/Iterator.hpp"

// -----------------------------------------------------------------------------

namespace database {
// -----------------------------------------------------------------------------

class GoutilsIterator : public Iterator {
  // -------------------------------------------------------------------------

 private:
  /// The type used to store the data.
  typedef goutils::Helpers::DataTypeNoSkipped DataType;

  /// The datatype of the underlying fields.
  typedef std::tuple_element<0, DataType>::type::element_type FieldsType;

  // -------------------------------------------------------------------------

 public:
  GoutilsIterator(const std::vector<std::string>& _colnames,
                  const DataType& _data,
                  const std::vector<std::string>& _time_formats)
      : colnames_(_colnames),
        data_(_data),
        field_num_(0),
        time_formats_(_time_formats) {
    assert_true(std::get<0>(data_));
  }

  ~GoutilsIterator() = default;

  // -------------------------------------------------------------------------

 public:
  /// Returns a double.
  Float get_double() final;

  /// Returns an int.
  Int get_int() final;

  /// Returns a time stamp.
  Float get_time_stamp() final;

  /// Returns a string .
  std::string get_string() final;

  // -------------------------------------------------------------------------

 public:
  /// Returns the column names of the query.
  std::vector<std::string> colnames() const final { return colnames_; }

  /// Whether the end is reached.
  bool end() const final { return field_num_ >= nfields(); }

  // -------------------------------------------------------------------------

 private:
  /// Trivial accessor.
  FieldsType& fields() {
    assert_true(std::get<0>(data_));
    return *std::get<0>(data_);
  }

  /// Returns the raw value.
  std::pair<std::string, bool> get_value() {
    if (end()) {
      throw std::runtime_error("End of data reached.");
    }

    const auto& ptr = fields()[field_num_];

    if (!ptr) {
      increment();
      return std::pair<std::string, bool>("", true);
    }

    const auto str = std::string(ptr.get());

    increment();

    return std::pair<std::string, bool>(str, false);
  }

  /// Increments the iterator.
  void increment() { fields()[field_num_++].reset(); }

  /// The total number of fields.
  size_t nfields() const { return std::get<1>(data_) * std::get<2>(data_); }

  // -------------------------------------------------------------------------

 private:
  /// The colnames for this query.
  const std::vector<std::string> colnames_;

  /// The actual data to be retrieved.
  const DataType data_;

  /// The current field we are accessing.
  size_t field_num_;

  /// Vector containing the time formats.
  const std::vector<std::string> time_formats_;

  // -------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace database

#endif  // DATABASE_GOUTILSITERATOR_HPP_
