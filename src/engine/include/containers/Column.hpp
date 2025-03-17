// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CONTAINERS_COLUMN_HPP_
#define CONTAINERS_COLUMN_HPP_

#include <cstddef>
#include <fstream>
#include <ranges>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

#include "containers/ColumnViewIterator.hpp"
#include "containers/ULong.hpp"
#include "helpers/Column.hpp"
#include "helpers/Endianness.hpp"
#include "helpers/NullChecker.hpp"
#include "helpers/SubroleParser.hpp"

namespace containers {

template <class T>
class Column {
 public:
  typedef typename helpers::Column<T>::InMemoryVector InMemoryVector;
  typedef typename helpers::Column<T>::MemmapVector MemmapVector;
  typedef typename helpers::Column<T>::InMemoryPtr InMemoryPtr;
  typedef typename helpers::Column<T>::MemmapPtr MemmapPtr;

  typedef typename helpers::Column<T>::ConstInMemoryPtr ConstInMemoryPtr;
  typedef typename helpers::Column<T>::ConstMemmapPtr ConstMemmapPtr;

  typedef typename helpers::Column<T>::Variant Variant;
  typedef typename helpers::Column<T>::ConstVariant ConstVariant;

  // Strings need special treatment because of the memory mapping
  typedef typename std::conditional<std::is_same<T, strings::String>::value,
                                    ColumnViewIterator<T>, T *>::type iterator;

  // Strings need special treatment because of the memory mapping
  typedef typename std::conditional<std::is_same<T, strings::String>::value,
                                    ColumnViewIterator<T>, const T *>::type
      const_iterator;

  typedef T value_type;

  static constexpr bool IN_MEMORY = true;
  static constexpr bool MEMORY_MAPPING = false;

  static constexpr const char *FLOAT_COLUMN = "FloatColumn";
  static constexpr const char *STRING_COLUMN = "StringColumn";

  static constexpr const char *FLOAT_COLUMN_VIEW = "FloatColumnView";
  static constexpr const char *STRING_COLUMN_VIEW = "StringColumnView";
  static constexpr const char *BOOLEAN_COLUMN_VIEW = "BooleanColumnView";

 public:
  Column(const Variant _data_ptr) : data_ptr_(_data_ptr), name_(""), unit_("") {
    static_assert(std::is_arithmetic<T>::value ||
                      std::is_same<T, std::string>::value ||
                      std::is_same<T, strings::String>::value,
                  "Only arithmetic types or strings::String allowed for "
                  "Column<T>(...)!");
  }

  Column(const Variant _data_ptr, const std::string &_name)
      : Column(_data_ptr) {
    set_name(_name);
  }

  template <typename IteratorType = iterator,
            typename std::enable_if<std::is_same<IteratorType, T *>::value,
                                    int>::type = 0>
  Column(const std::shared_ptr<memmap::Pool> &_pool, const size_t _nrows)
      : data_ptr_(make_data_ptr(_pool, _nrows)),
        name_(""),
        pool_(_pool),
        unit_("") {}

  Column(const std::shared_ptr<memmap::Pool> &_pool)
      : data_ptr_(make_data_ptr(_pool)), name_(""), pool_(_pool), unit_("") {}

  ~Column() = default;

  /// Appends another Column through rowbinding
  void append(const Column<T> &_other);

  /// Intialises data_ptr_ with an empty vector
  void clear();

  /// Generates a deep copy of the column itself.
  Column<T> clone(const std::shared_ptr<memmap::Pool> &_pool) const;

  /// Loads the Column from binary format
  void load(const std::string &_fname);

  /// Saves the Column in binary format
  void save(const std::string &_fname) const;

  /// Returns a copy of the column that has been sorted by the
  /// key provided.
  /// The resulting column does not have to be the same length
  /// as the original one, but will be of the same length as the
  /// _key.
  Column<T> sort_by_key(const std::vector<size_t> &_key) const;

  /// Returns a Column containing all rows for which _key is true.
  Column<T> where(const std::vector<bool> &_condition) const;

  /// Boundary-checked accessor to data
  template <class T2, typename IteratorType = iterator,
            typename std::enable_if<std::is_same<IteratorType, T *>::value,
                                    int>::type = 0>
  T &at(const T2 _i) {
    if (_i < 0 || static_cast<size_t>(_i) >= nrows()) {
      throw std::runtime_error("Out-of-bounds access to column '" + name_ +
                               "'");
    }
    const auto get = [_i](auto &&_ptr) -> T & { return (*_ptr)[_i]; };
    return std::visit(get, data_ptr_);
  }

  /// Boundary-checker accessor to data
  template <class T2>
  const T at(const T2 _i) const {
    if (_i < 0 || static_cast<size_t>(_i) >= nrows()) {
      throw std::runtime_error("Out-of-bounds access to column '" + name_ +
                               "'");
    }
    const auto get = [_i](auto &&_ptr) -> T { return (*_ptr)[_i]; };
    return std::visit(get, data_ptr_);
  }

  /// Iterator to beginning of data
  template <typename IteratorType = iterator,
            typename std::enable_if<std::is_same<IteratorType, T *>::value,
                                    int>::type = 0>
  iterator begin() {
    return data();
  }

  /// Const iterator to beginning of data
  const_iterator begin() const {
    if constexpr (std::is_same<iterator, T *>()) {
      return data();
    } else {
      const auto value_func = [this](const size_t _i) -> std::optional<T> {
        if (_i >= nrows()) {
          return std::nullopt;
        }
        return (*this)[_i];
      };
      return ColumnViewIterator<T>(value_func);
    }
  }

  /// Returns the data ptr as a constant.
  ConstVariant const_data_ptr() const {
    const auto to_const = [](auto &&_ptr) -> ConstVariant {
      using PtrType = std::decay_t<decltype(_ptr)>;

      if constexpr (std::is_same_v<PtrType, InMemoryPtr>)
        return ConstInMemoryPtr(_ptr);

      if constexpr (std::is_same_v<PtrType, MemmapPtr>)
        return ConstMemmapPtr(_ptr);
    };

    return std::visit(to_const, data_ptr_);
  }

  /// Trivial getter
  template <typename IteratorType = iterator,
            typename std::enable_if<std::is_same<IteratorType, T *>::value,
                                    int>::type = 0>
  T *data() {
    const auto get_data = [](auto &&_ptr) -> T * { return _ptr->data(); };
    return std::visit(get_data, data_ptr_);
  }

  /// Trivial getter
  template <typename IteratorType = iterator,
            typename std::enable_if<std::is_same<IteratorType, T *>::value,
                                    int>::type = 0>
  const T *data() const {
    const auto get_data = [](auto &&_ptr) -> const T * { return _ptr->data(); };
    return std::visit(get_data, data_ptr_);
  }

  /// Trivial getter
  Variant data_ptr() const { return data_ptr_; }

  /// Iterator to end of data
  template <typename IteratorType = iterator,
            typename std::enable_if<std::is_same<IteratorType, T *>::value,
                                    int>::type = 0>
  iterator end() {
    return data() + nrows();
  }

  /// Iterator to end of data
  const_iterator end() const {
    if constexpr (std::is_same<iterator, T *>()) {
      return data() + nrows();
    } else {
      return ColumnViewIterator<T>();
    }
  }

  /// Trivial getter
  const std::string &name() const { return name_; }

  /// Returns number of bytes occupied by the data
  ULong nbytes() const {
    if constexpr (std::is_same<T, strings::String>()) {
      ULong nbytes = nrows() * (sizeof(T) + 1);

      for (size_t i = 0; i < nrows(); ++i) {
        nbytes += static_cast<ULong>((*this)[i].size());
      }

      return nbytes;
    } else {
      return static_cast<ULong>(nrows()) * sizeof(T);
    }
  }

  /// Accessor to data
  template <class T2, typename IteratorType = iterator,
            typename std::enable_if<std::is_same<IteratorType, T *>::value,
                                    int>::type = 0>
  T &operator[](const T2 _i) {
    assert_true(_i >= 0);
    assert_true(static_cast<size_t>(_i) < nrows());
    const auto get = [_i](auto &&_ptr) -> T & { return (*_ptr)[_i]; };
    return std::visit(get, data_ptr_);
  }

  /// Accessor to data (for Float and Int)
  template <class T2, typename IteratorType = iterator,
            typename std::enable_if<std::is_same<IteratorType, T *>::value,
                                    int>::type = 0>
  const T operator[](const T2 _i) const {
    assert_true(_i >= 0);
    assert_true(static_cast<size_t>(_i) < nrows());
    const auto get = [_i](auto &&_ptr) -> T { return (*_ptr)[_i]; };
    return std::visit(get, data_ptr_);
  }

  /// Accessor to data (for strings::String)
  template <class T2, typename IteratorType = iterator,
            typename std::enable_if<!std::is_same<IteratorType, T *>::value,
                                    int>::type = 0>
  const T operator[](const T2 _i) const {
    assert_true(_i >= 0);
    assert_true(static_cast<size_t>(_i) < nrows());
    const auto get = [_i](auto &&_ptr) -> T {
      const auto str = (*_ptr)[_i];
      return str.c_str()[0] != '\0' ? str : strings::String(nullptr);
    };
    return std::visit(get, data_ptr_);
  }

  /// Trivial getter
  size_t nrows() const {
    const auto get_size = [](auto &&_ptr) -> size_t { return _ptr->size(); };
    return std::visit(get_size, data_ptr_);
  }

  /// Trivial getter
  std::shared_ptr<memmap::Pool> pool() const { return pool_; }

  /// Appends data to the end
  void push_back(const T &_val) {
    const auto push_back = [&_val](auto &&_ptr) { _ptr->push_back(_val); };
    std::visit(push_back, data_ptr_);
  }

  /// Trivial setter
  void set_name(const std::string &_name) { name_ = _name; }

  /// Trivial setter
  void set_subroles(const std::vector<std::string> &_subroles) {
    // For checking only
    helpers::SubroleParser::parse(_subroles);
    subroles_ = _subroles;
  }

  /// Trivial setter
  void set_unit(const std::string &_unit) { unit_ = _unit; }

  /// Trivial getter.
  size_t size() const { return nrows(); }

  /// Trivial getter
  const std::vector<std::string> &subroles() const { return subroles_; }

  /// Trivial getter
  const std::string &unit() const { return unit_; }

  /// Transforms Column to a std::vector
  std::vector<T> to_vector() const { return std::vector<T>(begin(), end()); }

  /// Transforms Column to a std::vector
  // TODO: remove this
  std::shared_ptr<std::vector<T>> to_vector_ptr() const {
    return std::make_shared<std::vector<T>>(begin(), end());
  }

  // -------------------------------

 private:
  /// Called by load(...) when the system's byte order is big endian or the
  /// underlying type is char.
  Column<T> load_big_endian(const std::string &_fname) const;

  /// Called by load(...) when the system's byte order is little endian and
  /// the underlying type is not char.
  Column<T> load_little_endian(const std::string &_fname) const;

  /// Called by save(...) when the system's byte order is big endian or the
  /// underlying type is char.
  void save_big_endian(const std::string &_fname) const;

  /// Called by save(...) when the system's byte order is little endian and
  /// the underlying type is not char.
  void save_little_endian(const std::string &_fname) const;

  // -------------------------------

 private:
  /// Initializes the data_ptr
  static Variant make_data_ptr(const std::shared_ptr<memmap::Pool> &_pool) {
    if (_pool) {
      return std::make_shared<MemmapVector>(_pool);
    }
    return std::make_shared<InMemoryVector>(0);
  }

  /// Initializes the data_ptr
  template <typename IteratorType = iterator,
            typename std::enable_if<std::is_same<IteratorType, T *>::value,
                                    int>::type = 0>
  static Variant make_data_ptr(const std::shared_ptr<memmap::Pool> &_pool,
                               const size_t _nrows) {
    if (_pool) {
      return std::make_shared<MemmapVector>(_pool, _nrows);
    }
    return std::make_shared<InMemoryVector>(_nrows);
  }

  /// Called by load_big_endian(...).
  template <class StringType>
  void read_string_big_endian(StringType *_str, std::ifstream *_input) const {
    size_t str_size = 0;

    _input->read(reinterpret_cast<char *>(&str_size), sizeof(size_t));

    std::string str;

    // Using resize is important, otherwise
    // the result is weird behaviour!
    str.resize(str_size);

    _input->read(&(str[0]), str_size);

    *_str = std::move(str);
  }

  /// Called by load_little_endian(...).
  template <class StringType>
  void read_string_little_endian(StringType *_str,
                                 std::ifstream *_input) const {
    size_t str_size = 0;

    _input->read(reinterpret_cast<char *>(&str_size), sizeof(size_t));

    helpers::Endianness::reverse_byte_order(&str_size);

    std::string str;

    // Using resize is important, otherwise
    // the result is weird behaviour!
    str.resize(str_size);

    _input->read(&(str[0]), str_size);

    *_str = std::move(str);
  }

  /// Called by save_big_endian(...).
  template <class StringType>
  void write_string_big_endian(const StringType &_str,
                               std::ofstream *_output) const {
    size_t str_size = _str.size();

    _output->write(reinterpret_cast<const char *>(&str_size), sizeof(size_t));

    _output->write(_str.c_str(), _str.size());
  }

  /// Called by save_little_endian(...).
  template <class StringType>
  void write_string_little_endian(const StringType &_str,
                                  std::ofstream *_output) const {
    size_t str_size = _str.size();

    helpers::Endianness::reverse_byte_order(&str_size);

    _output->write(reinterpret_cast<const char *>(&str_size), sizeof(size_t));

    _output->write(_str.c_str(), _str.size());
  }

  // -------------------------------

 private:
  /// The actual data.
  Variant data_ptr_;

  /// Name of the column.
  std::string name_;

  /// The memory pool, for memory mapping
  std::shared_ptr<memmap::Pool> pool_;

  /// Subroles applied to this column.
  std::vector<std::string> subroles_;

  /// Unit of the column.
  std::string unit_;

  // -------------------------------
};

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

template <class T>
void Column<T>::append(const Column<T> &_other) {
  for (const auto &val : _other) {
    push_back(val);
  }
}

// -------------------------------------------------------------------------

template <class T>
void Column<T>::clear() {
  *this = Column<T>(pool());
}

// -------------------------------------------------------------------------

template <class T>
Column<T> Column<T>::clone(const std::shared_ptr<memmap::Pool> &_pool) const {
  const auto new_pool =
      _pool ? std::make_shared<memmap::Pool>(_pool->temp_dir()) : _pool;

  const auto data_ptr =
      new_pool
          ? Variant(std::make_shared<MemmapVector>(new_pool, begin(), end()))
          : Variant(std::make_shared<std::vector<T>>(begin(), end()));

  auto col = Column<T>(data_ptr);

  col.set_name(name_);

  col.set_unit(unit_);

  return col;
}

// -----------------------------------------------------------------------------

template <class T>
void Column<T>::load(const std::string &_fname) {
  if (!std::is_same<T, char>() && helpers::Endianness::is_little_endian()) {
    *this = load_little_endian(_fname);
  } else {
    *this = load_big_endian(_fname);
  }
}

// -----------------------------------------------------------------------------

template <class T>
Column<T> Column<T>::load_big_endian(const std::string &_fname) const {
  std::ifstream input(_fname, std::ios::binary);

  size_t nrows = 0;

  input.read(reinterpret_cast<char *>(&nrows), sizeof(size_t));

  auto col = Column<T>(pool());

  if constexpr (std::is_same<T, strings::String>()) {
    for (size_t i = 0; i < nrows; ++i) {
      strings::String str;
      read_string_big_endian(&str, &input);
      col.push_back(str.c_str()[0] != '\0' ? str : strings::String(nullptr));
    }
  } else {
    col = Column<T>(pool(), nrows);
    input.read(reinterpret_cast<char *>(col.data()), col.nrows() * sizeof(T));
  }

  read_string_big_endian(&col.name_, &input);

  read_string_big_endian(&col.unit_, &input);

  return col;
}

// -----------------------------------------------------------------------------

template <class T>
Column<T> Column<T>::load_little_endian(const std::string &_fname) const {
  std::ifstream input(_fname, std::ios::binary);

  size_t nrows = 0;

  input.read(reinterpret_cast<char *>(&nrows), sizeof(size_t));

  helpers::Endianness::reverse_byte_order(&nrows);

  auto col = Column<T>(pool());

  if constexpr (std::is_same<T, strings::String>()) {
    for (size_t i = 0; i < nrows; ++i) {
      strings::String str;
      read_string_little_endian(&str, &input);
      col.push_back(str.c_str()[0] != '\0' ? str : strings::String(nullptr));
    }
  } else {
    col = Column<T>(pool(), nrows);
    input.read(reinterpret_cast<char *>(col.data()), col.nrows() * sizeof(T));
  }

  if constexpr (!std::is_same<T, strings::String>()) {
    for (size_t i = 0; i < nrows; ++i) {
      helpers::Endianness::reverse_byte_order(&col[i]);
    }
  }

  read_string_little_endian(&col.name_, &input);

  read_string_little_endian(&col.unit_, &input);

  return col;
}

// -----------------------------------------------------------------------------

template <class T>
void Column<T>::save(const std::string &_fname) const {
  if (std::is_same<T, char>::value == false &&
      helpers::Endianness::is_little_endian()) {
    save_little_endian(_fname);
  } else {
    save_big_endian(_fname);
  }
}

// -----------------------------------------------------------------------------

template <class T>
void Column<T>::save_big_endian(const std::string &_fname) const {
  std::ofstream output(_fname, std::ios::binary);

  const auto num_rows = nrows();

  output.write(reinterpret_cast<const char *>(&num_rows), sizeof(size_t));

  if constexpr (std::is_same<T, strings::String>()) {
    for (size_t i = 0; i < nrows(); ++i) {
      write_string_big_endian((*this)[i], &output);
    }
  } else {
    output.write(reinterpret_cast<const char *>(data()), nrows() * sizeof(T));
  }

  write_string_big_endian(name_, &output);

  write_string_big_endian(unit_, &output);
}

// -----------------------------------------------------------------------------

template <class T>
void Column<T>::save_little_endian(const std::string &_fname) const {
  std::ofstream output(_fname, std::ios::binary);

  auto num_rows = nrows();

  helpers::Endianness::reverse_byte_order(&num_rows);

  output.write(reinterpret_cast<const char *>(&num_rows), sizeof(size_t));

  if constexpr (std::is_same<T, strings::String>()) {
    for (size_t i = 0; i < nrows(); ++i) {
      write_string_little_endian((*this)[i], &output);
    }
  } else {
    auto write_reversed_data = [&output](const T _val) {
      T val_reversed = _val;

      helpers::Endianness::reverse_byte_order(&val_reversed);

      output.write(reinterpret_cast<const char *>(&val_reversed), sizeof(T));
    };

    for (size_t i = 0; i < nrows(); ++i) {
      write_reversed_data((*this)[i]);
    }
  }

  write_string_little_endian(name_, &output);

  write_string_little_endian(unit_, &output);
}

// -------------------------------------------------------------------------

template <class T>
Column<T> Column<T>::sort_by_key(const std::vector<size_t> &_key) const {
  auto sorted = Column<T>(pool());

  if constexpr (std::is_same<iterator, T *>()) {
    sorted = Column<T>(pool(), _key.size());

    for (size_t i = 0; i < _key.size(); ++i) {
      if (_key[i] < nrows()) {
        sorted[i] = (*this)[_key[i]];
      } else {
        sorted[i] = helpers::NullChecker::make_null<T>();
      }
    }
  } else {
    for (size_t i = 0; i < _key.size(); ++i) {
      if (_key[i] < nrows()) {
        sorted.push_back((*this)[_key[i]]);
      } else {
        sorted.push_back(helpers::NullChecker::make_null<T>());
      }
    }
  }

  sorted.set_name(name());

  sorted.set_subroles(subroles());

  sorted.set_unit(unit());

  return sorted;
}

// -----------------------------------------------------------------------------

template <class T>
Column<T> Column<T>::where(const std::vector<bool> &_condition) const {
  if (_condition.size() != nrows()) {
    throw std::runtime_error(
        "Size of keys must be identical to number of rows!");
  }

  const auto include = [&_condition](size_t _i) -> bool {
    return _condition[_i];
  };

  const auto get_val = [this](size_t _i) -> T { return (*this)[_i]; };

  auto range = std::views::iota(0uz, nrows()) | std::views::filter(include) |
               std::views::transform(get_val);

  const auto data_ptr = pool() ? Variant(std::make_shared<MemmapVector>(
                                     pool(), range.begin(), range.end()))
                               : Variant(std::make_shared<std::vector<T>>(
                                     range | std::ranges::to<std::vector>()));

  Column<T> trimmed(data_ptr);

  trimmed.set_name(name_);

  trimmed.set_subroles(subroles_);

  trimmed.set_unit(unit_);

  return trimmed;
}

}  // namespace containers

#endif  // CONTAINERS_COLUMN_HPP_
