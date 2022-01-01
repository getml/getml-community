#include "relmt/utils/CriticalValues.hpp"

// ----------------------------------------------------------------------------

#include "relmt/utils/Reducer.hpp"

// ----------------------------------------------------------------------------

namespace relmt {
namespace utils {
// ----------------------------------------------------------------------------

const std::shared_ptr<const std::vector<Int>> CriticalValues::calc_categorical(
    const enums::DataUsed _data_used, const size_t _num_column,
    const containers::DataFrame& _input,
    const containers::DataFrameView& _output,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    multithreading::Communicator* _comm) {
  // ---------------------------------------------------------------------------
  // In parallel versions, it is possible that there are no sample sizes
  // left in this process rank. In that case we effectively pass plus infinity
  // to min and minus infinity to max, ensuring that they not will be the
  // chosen minimum or maximum.

  Int min = std::numeric_limits<Int>::max();

  Int max = 0;

  find_min_max(_data_used, _num_column, _input, _output, _begin, _end, &min,
               &max, _comm);

  // ---------------------------------------------------------------------------
  // There is a possibility that all critical values are NAN in all processes.
  // This accounts for this edge case.

  if (min >= max) {
    return std::make_shared<std::vector<Int>>(0);
  }

  // ------------------------------------------------------------------------
  // Find unique categories (signified by a boolean vector). We cannot use the
  // actual boolean type, because bool is smaller than char and therefore the
  // all_reduce operator won't work. So we use std::int8_t instead.

  auto included = std::vector<std::int8_t>(max - min, 0);

  for (auto it = _begin; it != _end; ++it) {
    Int cat = 0;

    switch (_data_used) {
      case enums::DataUsed::categorical_input:
        cat = _input.categorical(it->ix_input, _num_column);
        break;

      case enums::DataUsed::categorical_output:
        cat = _output.categorical(it->ix_output, _num_column);
        break;

      default:
        assert_true(false && "Unknown _data_used!");
    }

    if (cat < 0) {
      continue;
    }

    assert_true(cat >= min);
    assert_true(cat < max);

    included[cat - min] = 1;
  }

  // ------------------------------------------------------------------------
  // Reduce included.

  utils::Reducer::reduce(multithreading::maximum<std::int8_t>(), &included,
                         _comm);

  // ------------------------------------------------------------------------
  // Build vector.

  auto categories = std::make_shared<std::vector<Int>>(0);

  for (Int i = 0; i < max - min; ++i) {
    if (included[i] == 1) {
      categories->push_back(min + i);
    }
  }

  // ------------------------------------------------------------------------

  return categories;

  // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<Float> CriticalValues::calc_discrete(
    const enums::DataUsed _data_used, const size_t _input_col,
    const size_t _output_col, const containers::DataFrame& _input,
    const containers::DataFrameView& _output,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    multithreading::Communicator* _comm) {
  // ---------------------------------------------------------------------------
  // In parallel versions, it is possible that there are no sample sizes
  // left in this process rank. In that case we effectively pass plus infinity
  // to min and minus infinity to max, ensuring that they not will be the
  // chosen minimum or maximum.

  Float min = std::numeric_limits<Float>::max();

  Float max = std::numeric_limits<Float>::lowest();

  if (is_same_units(_data_used)) {
    find_min_max(_data_used, _input_col, _output_col, _input, _output, _begin,
                 _end, &min, &max, _comm);
  } else {
    assert_true(_input_col == _output_col);

    find_min_max(_data_used, _output_col, _input, _output, _begin, _end, &min,
                 &max, _comm);
  }

  // ---------------------------------------------------------------------------

  min = std::ceil(min);

  max = std::ceil(max);

  // ---------------------------------------------------------------------------
  // There is a possibility that all critical values are NAN in all processes.
  // This accounts for this edge case.

  if (min > max) {
    return std::vector<Float>(0);
  }

  // ---------------------------------------------------------------------------
  // We want to prevent there being to many critical values.

  auto dist = static_cast<Int>(std::distance(_begin, _end));

  utils::Reducer::reduce(std::plus<Int>(), &dist, _comm);

  const size_t num_critical_values_numerical = calc_num_critical_values(dist);

  const auto num_critical_values = static_cast<size_t>(max - min);

  if (num_critical_values_numerical < num_critical_values) {
    auto critical_values =
        calc_numerical(num_critical_values_numerical, min, max);

    for (auto& c : critical_values) {
      c = std::floor(c);
    }

    return critical_values;
  }

  // ---------------------------------------------------------------------------

  std::vector<Float> critical_values(num_critical_values);

  for (size_t i = 0; i < critical_values.size(); ++i) {
    critical_values[i] = max - static_cast<Float>(i + 1);
  }

  return critical_values;

  // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<Float> CriticalValues::calc_numerical(
    const enums::DataUsed _data_used, const size_t _input_col,
    const size_t _output_col, const containers::DataFrame& _input,
    const containers::DataFrameView& _output,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    multithreading::Communicator* _comm) {
  // ---------------------------------------------------------------------------
  // In parallel versions, it is possible that there are no sample sizes
  // left in this process rank. In that case we effectively pass plus infinity
  // to min and minus infinity to max, ensuring that they not will be the
  // chosen minimum or maximum.

  Float min = std::numeric_limits<Float>::max();

  Float max = std::numeric_limits<Float>::lowest();

  if (is_same_units(_data_used)) {
    find_min_max(_data_used, _input_col, _output_col, _input, _output, _begin,
                 _end, &min, &max, _comm);
  } else {
    assert_true(_input_col == _output_col);

    find_min_max(_data_used, _output_col, _input, _output, _begin, _end, &min,
                 &max, _comm);
  }

  // ---------------------------------------------------------------------------
  // There is a possibility that all critical values are NAN in all processes.
  // This accounts for this edge case.

  if (min > max) {
    return std::vector<Float>(0);
  }

  // ---------------------------------------------------------------------------

  auto dist = static_cast<Int>(std::distance(_begin, _end));

  utils::Reducer::reduce(std::plus<Int>(), &dist, _comm);

  const size_t num_critical_values = calc_num_critical_values(dist);

  return calc_numerical(num_critical_values, min, max);

  // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<Float> CriticalValues::calc_numerical(
    const size_t _num_critical_values, const Float _min, const Float _max) {
  Float step_size =
      (_max - _min) / static_cast<Float>(_num_critical_values + 1);

  std::vector<Float> critical_values(_num_critical_values);

  for (size_t i = 0; i < _num_critical_values; ++i) {
    critical_values[i] = _max - static_cast<Float>(i + 1) * step_size;
  }

  return critical_values;
}

// ----------------------------------------------------------------------------

std::vector<Float> CriticalValues::calc_subfeatures(
    const size_t _col, const containers::Subfeatures& _subfeatures,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    multithreading::Communicator* _comm) {
  // ---------------------------------------------------------------------------

  assert_true(_col < _subfeatures.size());

  const auto& subfeature = _subfeatures[_col];

  // ---------------------------------------------------------------------------
  // In parallel versions, it is possible that there are no sample sizes
  // left in this process rank. In that case we effectively pass plus infinity
  // to min and minus infinity to max, ensuring that they not will be the
  // chosen minimum or maximum.

  Float min = std::numeric_limits<Float>::max();

  Float max = std::numeric_limits<Float>::lowest();

  auto dist = static_cast<Int>(std::distance(_begin, _end));

  if (dist > 0) {
    max = subfeature[(_begin)->ix_input];

    min = subfeature[(_end - 1)->ix_input];
  }

  utils::Reducer::reduce(multithreading::minimum<Float>(), &min, _comm);

  utils::Reducer::reduce(multithreading::maximum<Float>(), &max, _comm);

  // ---------------------------------------------------------------------------
  // There is a possibility that all critical values are NAN in all processes.
  // This accounts for this edge case.

  if (min > max) {
    return std::vector<Float>(0);
  }

  // ---------------------------------------------------------------------------

  utils::Reducer::reduce(std::plus<Int>(), &dist, _comm);

  size_t num_critical_values = calc_num_critical_values(dist);

  return calc_numerical(num_critical_values, min, max);

  // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<Float> CriticalValues::calc_time_window(
    const Float _delta_t, const containers::DataFrame& _input,
    const containers::DataFrameView& _output,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end,
    multithreading::Communicator* _comm) {
  // ---------------------------------------------------------------------------
  // In parallel versions, it is possible that there are no sample sizes
  // left in this process rank. In that case we effectively pass plus infinity
  // to min and minus infinity to max, ensuring that they not will be the
  // chosen minimum or maximum.

  Float min = std::numeric_limits<Float>::max();

  Float max = std::numeric_limits<Float>::lowest();

  find_min_max(enums::DataUsed::time_stamps_window, 0, _input, _output, _begin,
               _end, &min, &max, _comm);

  // ---------------------------------------------------------------------------
  // There is a possibility that all critical values are NAN in all processes.
  // This accounts for this edge case.

  if (min > max) {
    return std::vector<Float>(0);
  }

  // ---------------------------------------------------------------------------
  // The input value for delta_t could be stupid...we want to avoid memory
  // overflow.

  assert_true(_delta_t > 0.0);

  const auto num_critical_values =
      static_cast<size_t>((max - min) / _delta_t) + 1;

  if (num_critical_values > 100000) {
    return std::vector<Float>(0);
  }

  // ---------------------------------------------------------------------------

  std::vector<Float> critical_values(num_critical_values);

  for (size_t i = 0; i < num_critical_values; ++i) {
    critical_values[i] = max - static_cast<Float>(i + 1) * _delta_t;
  }

  return critical_values;

  // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void CriticalValues::find_min_max(
    const enums::DataUsed _data_used, const size_t _num_column,
    const containers::DataFrame& _input,
    const containers::DataFrameView& _output,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end, Int* _min, Int* _max,
    multithreading::Communicator* _comm) {
  if (std::distance(_begin, _end) > 0) {
    switch (_data_used) {
      case enums::DataUsed::categorical_input:
        *_min = _input.categorical(_begin->ix_input, _num_column);

        *_max = _input.categorical((_end - 1)->ix_input, _num_column) + 1;
        break;

      case enums::DataUsed::categorical_output:
        *_min = _output.categorical(_begin->ix_output, _num_column);

        *_max = _output.categorical((_end - 1)->ix_output, _num_column) + 1;
        break;

      default:
        assert_true(false && "Unknown _data_used!");
    }

    if (*_min < 0) {
      *_min = 0;
    }

    if (*_max < 0) {
      *_max = 0;
    }
  }

  utils::Reducer::reduce(multithreading::minimum<Int>(), _min, _comm);

  utils::Reducer::reduce(multithreading::maximum<Int>(), _max, _comm);
}

// ----------------------------------------------------------------------------

void CriticalValues::find_min_max(
    const enums::DataUsed _data_used, const size_t _input_col,
    const size_t _output_col, const containers::DataFrame& _input,
    const containers::DataFrameView& _output,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end, Float* _min,
    Float* _max, multithreading::Communicator* _comm) {
  if (std::distance(_begin, _end) > 0) {
    switch (_data_used) {
      case enums::DataUsed::same_units_discrete_ts:
      case enums::DataUsed::same_units_discrete:

        *_max = _output.discrete(_begin->ix_output, _output_col) -
                _input.discrete(_begin->ix_input, _input_col);

        *_min = _output.discrete((_end - 1)->ix_output, _output_col) -
                _input.discrete((_end - 1)->ix_input, _input_col);

        break;

      case enums::DataUsed::same_units_numerical_ts:
      case enums::DataUsed::same_units_numerical:

        *_max = _output.numerical(_begin->ix_output, _output_col) -
                _input.numerical(_begin->ix_input, _input_col);

        *_min = _output.numerical((_end - 1)->ix_output, _output_col) -
                _input.numerical((_end - 1)->ix_input, _input_col);

        break;

      default:
        assert_true(false && "Unknown _data_used!");
    }
  }

  utils::Reducer::reduce(multithreading::minimum<Float>(), _min, _comm);

  utils::Reducer::reduce(multithreading::maximum<Float>(), _max, _comm);
}

// ----------------------------------------------------------------------------

void CriticalValues::find_min_max(
    const enums::DataUsed _data_used, const size_t _num_column,
    const containers::DataFrame& _input,
    const containers::DataFrameView& _output,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _end, Float* _min,
    Float* _max, multithreading::Communicator* _comm) {
  if (std::distance(_begin, _end) > 0) {
    switch (_data_used) {
      case enums::DataUsed::discrete_output:

        *_max = _output.discrete(_begin->ix_output, _num_column);

        *_min = _output.discrete((_end - 1)->ix_output, _num_column);

        break;

      case enums::DataUsed::discrete_input:

        *_max = _input.discrete(_begin->ix_input, _num_column);

        *_min = _input.discrete((_end - 1)->ix_input, _num_column);

        break;

      case enums::DataUsed::numerical_output:

        *_max = _output.numerical(_begin->ix_output, _num_column);

        *_min = _output.numerical((_end - 1)->ix_output, _num_column);

        break;

      case enums::DataUsed::numerical_input:

        *_max = _input.numerical(_begin->ix_input, _num_column);

        *_min = _input.numerical((_end - 1)->ix_input, _num_column);

        break;

      case enums::DataUsed::time_stamps_window:

        *_max = _output.time_stamp(_begin->ix_output) -
                _input.time_stamp(_begin->ix_input);

        *_min = _output.time_stamp((_end - 1)->ix_output) -
                _input.time_stamp((_end - 1)->ix_input);

        break;

      default:
        assert_true(false && "Unknown _data_used!");
    }
  }

  utils::Reducer::reduce(multithreading::minimum<Float>(), _min, _comm);

  utils::Reducer::reduce(multithreading::maximum<Float>(), _max, _comm);
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relmt
