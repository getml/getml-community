// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PREPROCESSORS_EMAILDOMAIN_HPP_
#define ENGINE_PREPROCESSORS_EMAILDOMAIN_HPP_

#include "commands/Fingerprint.hpp"
#include "commands/Preprocessor.hpp"
#include "engine/Int.hpp"
#include "engine/preprocessors/Params.hpp"
#include "engine/preprocessors/Preprocessor.hpp"
#include "helpers/ColumnDescription.hpp"
#include "helpers/Macros.hpp"
#include "helpers/StringIterator.hpp"

#include <rfl/Field.hpp>
#include <rfl/Literal.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>

#include <memory>
#include <utility>
#include <vector>

namespace engine {
namespace preprocessors {

class EMailDomain final : public Preprocessor {
  using MarkerType = typename helpers::ColumnDescription::MarkerType;

 public:
  using EMailDomainOp = typename commands::Preprocessor::EMailDomainOp;

  struct SaveLoad {
    rfl::Field<"cols_", std::vector<rfl::Ref<helpers::ColumnDescription>>> cols;
  };

  using ReflectionType = SaveLoad;

 public:
  EMailDomain(const EMailDomainOp&,
              const std::vector<commands::Fingerprint>& _dependencies);

  ~EMailDomain() final = default;

 public:
  /// Identifies which features should be extracted from which time stamps.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  fit_transform(const Params& _params) final;

  /// Loads the predictor
  void load(const std::string& _fname) final;

  /// Stores the preprocessor.
  void save(const std::string& _fname,
            const typename helpers::Saver::Format& _format) const final;

  /// Transforms the data frames by adding the desired time series
  /// transformations.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  transform(const Params& _params) const final;

 public:
  /// Creates a deep copy.
  rfl::Ref<Preprocessor> clone(
      const std::optional<std::vector<commands::Fingerprint>>& _dependencies =
          std::nullopt) const final {
    const auto c = rfl::Ref<EMailDomain>::make(*this);
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
    return commands::Fingerprint(FingerprintType{
        .dependencies = dependencies_,
        .op = commands::Preprocessor::EMailDomainOp{std::nullopt}});
  }

  /// Necessary for the automated parsing to work.
  ReflectionType reflection() const { return ReflectionType{.cols = cols_}; }

  /// The preprocessor does not generate any SQL scripts.
  std::vector<std::string> to_sql(
      const helpers::StringIterator&,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&)
      const final {
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
                                         const MarkerType _marker,
                                         const size_t _table,
                                         containers::Encoding* _categories);

  /// Transforms a single data frame.
  containers::DataFrame transform_df(const containers::Encoding& _categories,
                                     const containers::DataFrame& _df,
                                     const MarkerType _marker,
                                     const size_t _table) const;

 private:
  /// Generates the name for the newly created column.
  std::string make_name(const std::string& _colname) const {
    return helpers::Macros::email_domain_begin() + _colname +
           helpers::Macros::email_domain_end();
  }

 private:
  /// List of all columns to which the email domain transformation applies.
  std::vector<rfl::Ref<helpers::ColumnDescription>> cols_;

  /// The dependencies inserted into the the preprocessor.
  std::vector<commands::Fingerprint> dependencies_;
};

}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_EMAILDOMAIN_HPP_
