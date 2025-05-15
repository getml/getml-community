// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/preprocessors/EMailDomain.hpp"

#include "engine/preprocessors/PreprocessorImpl.hpp"
#include "helpers/Loader.hpp"
#include "helpers/Saver.hpp"

namespace engine {
namespace preprocessors {

EMailDomain::EMailDomain(
    const EMailDomainOp& /*unused*/,
    const std::vector<commands::Fingerprint>& _dependencies)
    : dependencies_(_dependencies) {}

std::optional<containers::Column<Int>> EMailDomain::extract_domain(
    const containers::Column<strings::String>& _col,
    containers::Encoding* _categories) const {
  auto str_col = extract_domain_string(_col);

  auto int_col = containers::Column<Int>(_col.pool(), str_col.nrows());

  for (size_t i = 0; i < str_col.nrows(); ++i) {
    int_col[i] = (*_categories)[str_col[i]];
  }

  int_col.set_name(make_name(_col.name()));

  int_col.set_unit("email domain");

  if (PreprocessorImpl::has_warnings(int_col)) {
    return std::nullopt;
  }

  return int_col;
}

// ----------------------------------------------------

containers::Column<Int> EMailDomain::extract_domain(
    const containers::Encoding& _categories,
    const containers::Column<strings::String>& _col) const {
  auto str_col = extract_domain_string(_col);

  auto int_col = containers::Column<Int>(_col.pool(), str_col.nrows());

  for (size_t i = 0; i < str_col.nrows(); ++i) {
    int_col[i] = _categories[str_col[i]];
  }

  int_col.set_name(make_name(_col.name()));

  int_col.set_unit("email domain");

  return int_col;
}

// ----------------------------------------------------

containers::Column<strings::String> EMailDomain::extract_domain_string(
    const containers::Column<strings::String>& _col) const {
  auto result = containers::Column<strings::String>(_col.pool());

  for (size_t i = 0; i < _col.nrows(); ++i) {
    const auto str = _col[i].str();

    const auto at_pos = str.find("@");

    if (at_pos == std::string::npos) {
      continue;
    }

    const auto domain = str.substr(at_pos);

    if (domain.find(".") == std::string::npos) {
      continue;
    }

    result.push_back(strings::String(domain));
  }

  return result;
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
EMailDomain::fit_transform(const Params& _params) {
  const auto population_df = fit_transform_df(
      _params.population_df(), MarkerType::make<"[POPULATION]">(), 0,
      _params.categories().get());

  auto peripheral_dfs = std::vector<containers::DataFrame>();

  for (size_t i = 0; i < _params.peripheral_dfs().size(); ++i) {
    const auto& df = _params.peripheral_dfs().at(i);

    const auto new_df = fit_transform_df(df, MarkerType::make<"[PERIPHERAL]">(),
                                         i, _params.categories().get());

    peripheral_dfs.push_back(new_df);
  }

  return std::make_pair(population_df, peripheral_dfs);
}

// ----------------------------------------------------

containers::DataFrame EMailDomain::fit_transform_df(
    const containers::DataFrame& _df, const MarkerType _marker,
    const size_t _table, containers::Encoding* _categories) {
  const auto whitelist = std::vector<helpers::Subrole>(
      {helpers::Subrole::email, helpers::Subrole::email_only});

  const auto blacklist =
      std::vector<helpers::Subrole>({helpers::Subrole::exclude_preprocessors,
                                     helpers::Subrole::substring_only});

  auto df = _df;

  for (size_t i = 0; i < _df.num_text(); ++i) {
    const auto& email_col = _df.text(i);

    if (!helpers::SubroleParser::contains_any(email_col.subroles(),
                                              whitelist)) {
      continue;
    }

    if (helpers::SubroleParser::contains_any(email_col.subroles(), blacklist)) {
      continue;
    }

    const auto col = extract_domain(email_col, _categories);

    if (col) {
      PreprocessorImpl::add(_marker, _table, email_col.name(), &cols_);
      df.add_int_column(*col, containers::DataFrame::ROLE_CATEGORICAL);
    }
  }

  return df;
}

// -----------------------------------------------------------------------------

void EMailDomain::load(const std::string& _fname) {
  const auto named_tuple = helpers::Loader::load<ReflectionType>(_fname);
  cols_ = named_tuple.cols();
}

// ----------------------------------------------------

void EMailDomain::save(const std::string& _fname,
                       const typename helpers::Saver::Format& _format) const {
  helpers::Saver::save(_fname, *this, _format);
}

// ----------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
EMailDomain::transform(const Params& _params) const {
  const auto population_df =
      transform_df(*_params.categories(), _params.population_df(),
                   MarkerType::make<"[POPULATION]">(), 0);

  auto peripheral_dfs = std::vector<containers::DataFrame>();

  for (size_t i = 0; i < _params.peripheral_dfs().size(); ++i) {
    const auto& df = _params.peripheral_dfs().at(i);

    const auto new_df = transform_df(*_params.categories(), df,
                                     MarkerType::make<"[PERIPHERAL]">(), i);

    peripheral_dfs.push_back(new_df);
  }

  return std::make_pair(population_df, peripheral_dfs);
}

// ----------------------------------------------------

containers::DataFrame EMailDomain::transform_df(
    const containers::Encoding& _categories, const containers::DataFrame& _df,
    const MarkerType _marker, const size_t _table) const {
  auto df = _df;

  auto names = PreprocessorImpl::retrieve_names(_marker, _table, cols_);

  for (const auto& name : names) {
    const auto col = extract_domain(_categories, df.unused_string(name));

    df.add_int_column(col, containers::DataFrame::ROLE_CATEGORICAL);
  }

  return df;
}

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine
