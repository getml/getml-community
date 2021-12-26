#ifndef RELMT_UTILS_CRITICALVALUESORTER_HPP_
#define RELMT_UTILS_CRITICALVALUESORTER_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <vector>

// ----------------------------------------------------------------------------

#include "helpers/helpers.hpp"
#include "multithreading/multithreading.hpp"
#include "strings/strings.hpp"

// ----------------------------------------------------------------------------

#include "relmt/Int.hpp"
#include "relmt/containers/containers.hpp"

// ----------------------------------------------------------------------------

namespace relmt {
namespace utils {
// ------------------------------------------------------------------------

class CriticalValueSorter {
 public:
  /// Sort critical values in DESCENDING order of the associated averages.
  static std::shared_ptr<const std::vector<Int>> sort(
      const Int _min, const std::optional<textmining::RowIndex>& _row_index,
      const std::vector<size_t>& _indptr,
      const containers::Rescaled& _output_rescaled,
      const containers::Rescaled& _input_rescaled,
      const std::vector<containers::CandidateSplit>::iterator _candidates_begin,
      const std::vector<containers::CandidateSplit>::iterator _candidates_end,
      std::vector<containers::Match>* _bins,
      multithreading::Communicator* _comm);

 private:
  /// Calculates the sums and count of the current candidate split.
  static std::pair<Float, Float> calc_average(
      const containers::CandidateSplit& _split,
      const std::vector<Float>& _total_sums, const Float _total_count,
      const containers::Rescaled& _output_rescaled,
      const containers::Rescaled& _input_rescaled,
      const std::vector<containers::Match>::iterator _split_begin,
      const std::vector<containers::Match>::iterator _split_end);

  /// Calculates the sums of all aggregated columns
  static std::pair<std::vector<Float>, Float> calc_sums(
      const containers::Rescaled& _output_rescaled,
      const containers::Rescaled& _input_rescaled,
      const std::vector<containers::Match>::iterator _begin,
      const std::vector<containers::Match>::iterator _end);

  /// Every candidate split has a average value it is associated with for the
  /// sorting. This calculates said averages.
  static std::vector<Float> make_averages(
      const Int _min, const std::optional<textmining::RowIndex>& _row_index,
      const std::vector<size_t>& _indptr,
      const containers::Rescaled& _output_rescaled,
      const containers::Rescaled& _input_rescaled,
      const std::vector<containers::CandidateSplit>::iterator _candidates_begin,
      const std::vector<containers::CandidateSplit>::iterator _candidates_end,
      std::vector<containers::Match>* _bins,
      multithreading::Communicator* _comm);

  /// Makes the averages for categorical data.
  static void make_averages_category(
      const size_t _size, const std::vector<Float>& _total_sums,
      const Float _total_count, const Int _min,
      const std::vector<size_t>& _indptr,
      const containers::Rescaled& _output_rescaled,
      const containers::Rescaled& _input_rescaled,
      const std::vector<containers::CandidateSplit>::iterator _candidates_begin,
      std::vector<containers::Match>* _bins, Float* _sums, Float* _counts);

  /// Makes the averages for words.
  static void make_averages_words(
      const size_t _size, const std::vector<Float>& _total_sums,
      const Float _total_count, const textmining::RowIndex& _row_index,
      const std::vector<size_t>& _indptr,
      const containers::Rescaled& _output_rescaled,
      const containers::Rescaled& _input_rescaled,
      const std::vector<containers::CandidateSplit>::iterator _candidates_begin,
      std::vector<containers::Match>* _bins, Float* _sums, Float* _counts);

  /// Generates the tuples for sorting.
  static std::vector<std::tuple<Float, Int>> make_tuples(
      const std::vector<Float>& _averages,
      const std::vector<containers::CandidateSplit>::iterator _begin,
      const std::vector<containers::CandidateSplit>::iterator _end);
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relmt

// ----------------------------------------------------------------------------

#endif  // RELMT_UTILS_CRITICALVALUESORTER_HPP_
