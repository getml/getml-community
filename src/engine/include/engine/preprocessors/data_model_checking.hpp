// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PREPROCESSORS_DATAMODELCHECKING_HPP_
#define ENGINE_PREPROCESSORS_DATAMODELCHECKING_HPP_

#include "communication/SocketLogger.hpp"
#include "communication/Warner.hpp"
#include "engine/Float.hpp"
#include "engine/Int.hpp"
#include "featurelearners/AbstractFeatureLearner.hpp"
#include "helpers/Placeholder.hpp"

#include <memory>
#include <string>
#include <vector>

namespace engine {
namespace preprocessors {
namespace data_model_checking {

/// Generates warnings, if there are obvious issues in the data model.
communication::Warner check(
    const std::shared_ptr<const helpers::Placeholder> _placeholder,
    const std::shared_ptr<const std::vector<std::string>> _peripheral_names,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>
        _feature_learners,
    const std::shared_ptr<const communication::SocketLogger>& _logger);

/// Checks the plausibility of a categorical column.
void check_categorical_column(const containers::Column<Int>& _col,
                              const std::string& _df_name,
                              communication::Warner* _warner);

/// Checks the plausibility of a float column.
void check_float_column(const containers::Column<Float>& _col,
                        const std::string& _df_name,
                        communication::Warner* _warner);

}  // namespace data_model_checking
}  // namespace preprocessors
}  // namespace engine

#endif
