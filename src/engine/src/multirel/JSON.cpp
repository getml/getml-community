#include "multirel/JSON.hpp"

namespace multirel {
// ------------------------------------------------------------------------

size_t JSON::data_used_to_int(const enums::DataUsed& _data_used) {
  switch (_data_used) {
    case enums::DataUsed::not_applicable:
      return 0;

    case enums::DataUsed::same_unit_categorical:
      return 1;

    case enums::DataUsed::same_unit_discrete:
      return 2;

    case enums::DataUsed::same_unit_numerical:
      return 3;

    case enums::DataUsed::x_perip_categorical:
      return 4;

    case enums::DataUsed::x_perip_numerical:
      return 5;

    case enums::DataUsed::x_perip_discrete:
      return 6;

    case enums::DataUsed::x_popul_categorical:
      return 7;

    case enums::DataUsed::x_popul_numerical:
      return 8;

    case enums::DataUsed::x_popul_discrete:
      return 9;

    case enums::DataUsed::x_subfeature:
      return 10;

    case enums::DataUsed::time_stamps_diff:
      return 11;

    case enums::DataUsed::time_stamps_window:
      return 12;

    case enums::DataUsed::same_unit_discrete_ts:
      return 13;

    case enums::DataUsed::same_unit_numerical_ts:
      return 14;

    case enums::DataUsed::x_popul_text:
      return 15;

    case enums::DataUsed::x_perip_text:
      return 16;

    default:
      assert_true(!"Unknown enums::DataUsed!");
      return 0;
  }
}

// ------------------------------------------------------------------------

enums::DataUsed JSON::int_to_data_used(const size_t& _val) {
  switch (_val) {
    case 0:
      return enums::DataUsed::not_applicable;

    case 1:
      return enums::DataUsed::same_unit_categorical;

    case 2:
      return enums::DataUsed::same_unit_discrete;

    case 3:
      return enums::DataUsed::same_unit_numerical;

    case 4:
      return enums::DataUsed::x_perip_categorical;

    case 5:
      return enums::DataUsed::x_perip_numerical;

    case 6:
      return enums::DataUsed::x_perip_discrete;

    case 7:
      return enums::DataUsed::x_popul_categorical;

    case 8:
      return enums::DataUsed::x_popul_numerical;

    case 9:
      return enums::DataUsed::x_popul_discrete;

    case 10:
      return enums::DataUsed::x_subfeature;

    case 11:
      return enums::DataUsed::time_stamps_diff;

    case 12:
      return enums::DataUsed::time_stamps_window;

    case 13:
      return enums::DataUsed::same_unit_discrete_ts;

    case 14:
      return enums::DataUsed::same_unit_numerical_ts;

    case 15:
      return enums::DataUsed::x_popul_text;

    case 16:
      return enums::DataUsed::x_perip_text;

    default:
      assert_true(!"Unknown enums::DataUsed!");
      return enums::DataUsed::not_applicable;
  }
}

// ------------------------------------------------------------------------
}  // namespace multirel
