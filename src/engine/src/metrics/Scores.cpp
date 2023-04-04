// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "metrics/Scores.hpp"

#include "helpers/Saver.hpp"

namespace metrics {

// ----------------------------------------------------------------------------

Scores::Scores()
    : val_(f_accuracy({}) * f_auc({}) * f_cross_entropy({}) * f_mae({}) *
           f_rmse({}) * f_rsquared({}) * f_accuracy_curves({}) *
           f_average_targets({}) * f_column_descriptions({}) *
           f_column_importances({}) * f_feature_correlations({}) *
           f_feature_densities({}) * f_feature_importances({}) *
           f_feature_names({}) * f_fpr({}) * f_history({}) * f_labels({}) *
           f_lift({}) * f_precision({}) * f_prediction_min({}) *
           f_prediction_step_size({}) * f_proportion({}) * f_set_used("") *
           f_tpr({})) {}

// ----------------------------------------------------------------------------

Scores::Scores(const NamedTupleType& _val) : val_(_val) {}

// ----------------------------------------------------------------------------

Scores::~Scores() = default;

// ----------------------------------------------------------------------------

void Scores::save(const std::string& _fname) const {
  helpers::Saver::save_as_json(_fname, *this);
}

// ----------------------------------------------------------------------------

void Scores::to_history() {
  const auto now = Poco::DateTimeFormatter::format(
      Poco::LocalDateTime(), Poco::DateTimeFormat::SORTABLE_FORMAT);

  if (val_.get<"auc_">().size() > 0) {
    val_.get<f_history>().push_back(ClassificationMetricsType(val_) *
                                    fct::make_field<"date_time_">(now) *
                                    f_set_used(val_.get<f_set_used>()));
  } else {
    val_.get<f_history>().push_back(RegressionMetricsType(val_) *
                                    fct::make_field<"date_time_">(now) *
                                    f_set_used(val_.get<f_set_used>()));
  }
}

// ----------------------------------------------------------------------------
}  // namespace metrics
