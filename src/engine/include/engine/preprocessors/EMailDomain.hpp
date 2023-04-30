// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PREPROCESSORS_EMAILDOMAIN_HPP_
#define ENGINE_PREPROCESSORS_EMAILDOMAIN_HPP_

#include <memory>
#include <utility>
#include <vector>

#include "commands/Fingerprint.hpp"
#include "commands/Preprocessor.hpp"
#include "containers/containers.hpp"
#include "engine/Int.hpp"
#include "engine/preprocessors/Params.hpp"
#include "engine/preprocessors/Preprocessor.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "helpers/ColumnDescription.hpp"
#include "helpers/Macros.hpp"
#include "helpers/StringIterator.hpp"
#include "strings/strings.hpp"

namespace engine {
namespace preprocessors {

class EMailDomain : public Preprocessor {
 public:
  using EMailDomainOp = typename commands::Preprocessor::EMailDomainOp;

  using f_cols =
      fct::Field<"cols_",
                 std::vector<std::shared_ptr<helpers::ColumnDescription>>>;

  using NamedTupleType = fct::NamedTuple<f_cols>;

 public:
  EMailDomain(const EMailDomainOp& _op,
              const std::vector<commands::Fingerprint>& _dependencies)
      : dependencies_(_dependencies) {}

  ~EMailDomain() = default;

 public:
  /// Identifies which features should be extracted from which time stamps.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  fit_transform(const Params& _params) final;

  /// Loads the predictor
  void load(const std::string& _fname) final;

  /// Stores the preprocessor.
  void save(const std::string& _fname) const final;

  /// Transforms the data frames by adding the desired time series
  /// transformations.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  transform(const Params& _params) const final;

 public:
  /// Creates a deep copy.
  fct::Ref<Preprocessor> clone(
      const std::optional<std::vector<commands::Fingerprint>>& _dependencies =
          std::nullopt) const final {
    const auto c = fct::Ref<EMailDomain>::make(*this);
    if (_dependencies) {
      c->dependencies_ = *_dependencies;
    }
    return c;
  }

  /// Returns the fingerprint of the preprocessor (necessary to build
  /// the dependency graphs).
  commands::Fingerprint fingerprint() const final {
    using FingerprintType =
        typename commands::Fingerprint::EMailDomainFingerprint;
    return commands::Fingerprint(FingerprintType(
        fct::make_field<"dependencies_">(dependencies_),
        fct::make_field<"type_">(fct::Literal<"EMailDomain">())));
  }

  /// Necessary for the automated parsing to work.
  NamedTupleType named_tuple() const { return NamedTupleType(f_cols(cols_)); }

  /// The preprocessor does not generate any SQL scripts.
  std::vector<std::string> to_sql(
      const helpers::StringIterator& _categories,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator) const final {
    return {};
  }

  /// Returns the type of the preprocessor.
  std::string type() const final { return Preprocessor::EMAILDOMAIN; }

 private:
  /// Extracts the domain during fitting.
  std::optional<containers::Column<Int>> extract_domain(
      const containers::Column<strings::String>& _col,
      containers::Encoding* _categories) const;

  /// Extracts the domain during transformation.
  containers::Column<Int> extract_domain(
      const containers::Encoding& _categories,
      const containers::Column<strings::String>& _col) const;

  /// Extracts the domain as a string.
  containers::Column<strings::String> extract_domain_string(
      const containers::Column<strings::String>& _col) const;

  /// Fits and transforms an individual data frame.
  containers::DataFrame fit_transform_df(const containers::DataFrame& _df,
                                         const std::string& _marker,
                                         const size_t _table,
                                         containers::Encoding* _categories);

  /// Transforms a single data frame.
  containers::DataFrame transform_df(const containers::Encoding& _categories,
                                     const containers::DataFrame& _df,
                                     const std::string& _marker,
                                     const size_t _table) const;

 private:
  /// Generates the name for the newly created column.
  std::string make_name(const std::string& _colname) const {
    return helpers::Macros::email_domain_begin() + _colname +
           helpers::Macros::email_domain_end();
  }

 private:
  /// List of all columns to which the email domain transformation applies.
  std::vector<std::shared_ptr<helpers::ColumnDescription>> cols_;

  /// The dependencies inserted into the the preprocessor.
  std::vector<commands::Fingerprint> dependencies_;
};

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_EMAILDOMAIN_HPP_

