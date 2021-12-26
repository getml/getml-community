#ifndef ENGINE_PREPROCESSORS_SUBSTRING_HPP_
#define ENGINE_PREPROCESSORS_SUBSTRING_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include <memory>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

#include "helpers/helpers.hpp"
#include "strings/strings.hpp"

// ----------------------------------------------------------------------------

#include "engine/containers/containers.hpp"

// ----------------------------------------------------------------------------

#include "engine/preprocessors/FitParams.hpp"
#include "engine/preprocessors/Preprocessor.hpp"
#include "engine/preprocessors/PreprocessorImpl.hpp"
#include "engine/preprocessors/TransformParams.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace preprocessors {
// ----------------------------------------------------

class Substring : public Preprocessor {
 public:
  Substring() : begin_(0), length_(0) {}

  Substring(const Poco::JSON::Object& _obj,
            const std::vector<Poco::JSON::Object::Ptr>& _dependencies) {
    *this = from_json_obj(_obj);
    dependencies_ = _dependencies;
  }

  ~Substring() = default;

 public:
  /// Returns the fingerprint of the preprocessor (necessary to build
  /// the dependency graphs).
  Poco::JSON::Object::Ptr fingerprint() const final;

  /// Identifies which features should be extracted from which time stamps.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  fit_transform(const FitParams& _params) final;

  /// Expresses the Seasonal preprocessor as a JSON object.
  Poco::JSON::Object::Ptr to_json_obj() const final;

  /// Transforms the data frames by adding the desired time series
  /// transformations.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  transform(const TransformParams& _params) const final;

 public:
  /// Creates a deep copy.
  std::shared_ptr<Preprocessor> clone(
      const std::optional<std::vector<Poco::JSON::Object::Ptr>>& _dependencies =
          std::nullopt) const final {
    const auto c = std::make_shared<Substring>(*this);
    if (_dependencies) {
      c->dependencies_ = *_dependencies;
    }
    return c;
  }

  /// The preprocessor does not generate any SQL scripts.
  std::vector<std::string> to_sql(
      const helpers::StringIterator& _categories,
      const std::shared_ptr<const helpers::SQLDialectGenerator>&
          _sql_dialect_generator) const final {
    return {};
  }

  /// Returns the type of the preprocessor.
  std::string type() const final { return Preprocessor::SUBSTRING; }

 private:
  /// Extracts the substring during fitting.
  std::optional<containers::Column<Int>> extract_substring(
      const containers::Column<strings::String>& _col,
      containers::Encoding* _categories) const;

  /// Extracts the substring during transformation.
  containers::Column<Int> extract_substring(
      const containers::Encoding& _categories,
      const containers::Column<strings::String>& _col) const;

  /// Extracts the substring as a string.
  containers::Column<strings::String> extract_substring_string(
      const containers::Column<strings::String>& _col) const;

  /// Fits and transforms an individual data frame.
  containers::DataFrame fit_transform_df(const containers::DataFrame& _df,
                                         const std::string& _marker,
                                         const size_t _table,
                                         containers::Encoding* _categories);

  /// Parses a JSON object.
  Substring from_json_obj(const Poco::JSON::Object& _obj) const;

  /// Generates a string column from the _categories and the int column.
  containers::Column<strings::String> make_str_col(
      const containers::Encoding& _categories,
      const containers::Column<Int>& _col) const;

  /// Transforms a single data frame.
  containers::DataFrame transform_df(const containers::Encoding& _categories,
                                     const containers::DataFrame& _df,
                                     const std::string& _marker,
                                     const size_t _table) const;

 private:
  /// Extracts the columns and adds it to the data frame
  template <class T>
  void extract_and_add(const std::string& _marker, const size_t _table,
                       const containers::Column<T>& _original_col,
                       containers::Encoding* _categories,
                       containers::DataFrame* _df) {
    const auto whitelist = std::vector<helpers::Subrole>(
        {helpers::Subrole::substring, helpers::Subrole::substring_only});

    const auto blacklist =
        std::vector<helpers::Subrole>({helpers::Subrole::exclude_preprocessors,
                                       helpers::Subrole::email_only});

    if (!helpers::SubroleParser::contains_any(_original_col.subroles(),
                                              whitelist)) {
      return;
    }

    if (helpers::SubroleParser::contains_any(_original_col.subroles(),
                                             blacklist)) {
      return;
    }

    if (unit_ != "" && _original_col.unit() != unit_) {
      return;
    }

    const auto col = extract_substring(_original_col, _categories);

    if (col) {
      PreprocessorImpl::add(_marker, _table, _original_col.name(), &cols_);
      _df->add_int_column(*col, containers::DataFrame::ROLE_CATEGORICAL);
    }
  }

  /// Extracts the substring during fitting.
  std::optional<containers::Column<Int>> extract_substring(
      const containers::Column<Int>& _col,
      containers::Encoding* _categories) const {
    const auto str_col = make_str_col(*_categories, _col);
    return extract_substring(str_col, _categories);
  }

  /// Extracts the substring during transformation.
  containers::Column<Int> extract_substring(
      const containers::Encoding& _categories,
      const containers::Column<Int>& _col) const {
    const auto str_col = make_str_col(_categories, _col);
    return extract_substring(_categories, str_col);
  }

  /// Generates the colname for the newly created column.
  std::string make_name(const std::string& _colname) const {
    return helpers::Macros::substring() + _colname + helpers::Macros::begin() +
           std::to_string(begin_ + 1) + helpers::Macros::length() +
           std::to_string(length_) + helpers::Macros::close_bracket();
  }

  /// Generates the unit for the newly created column.
  std::string make_unit(const std::string& _unit) const {
    return _unit + ", " + std::to_string(begin_) + ", " +
           std::to_string(length_);
  }

 private:
  /// The beginning of the substring to extract.
  size_t begin_;

  /// List of all columns to which the email domain transformation applies.
  std::vector<std::shared_ptr<helpers::ColumnDescription>> cols_;

  /// The dependencies inserted into the the preprocessor.
  std::vector<Poco::JSON::Object::Ptr> dependencies_;

  /// The length of the substring to extract.
  size_t length_;

  /// The unit to which we want to apply the substring preprocessor
  std::string unit_;
};

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_SUBSTRING_HPP_

