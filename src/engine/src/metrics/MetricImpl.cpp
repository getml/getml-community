// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "metrics/MetricImpl.hpp"

namespace metrics {

MetricImpl::MetricImpl() : MetricImpl(nullptr) {}

MetricImpl::MetricImpl(multithreading::Communicator* _comm) : comm_(_comm) {}

}  // namespace metrics
