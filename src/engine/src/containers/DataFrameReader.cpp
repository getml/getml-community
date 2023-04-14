// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "containers/DataFrameReader.hpp"

namespace containers {

std::vector<std::string> DataFrameReader::make_colnames(const DataFrame& _df,
                                                        char _quotechar) {
  std::vector<std::string> colnames;

  for (size_t i = 0; i < _df.num_categoricals(); ++i) {
    const auto& colname = _df.categorical(i).name();
    colnames.push_back(colname);
  }

  for (size_t i = 0; i < _df.num_join_keys(); ++i) {
    const auto& colname = _df.join_key(i).name();
    colnames.push_back(colname);
  }

  for (size_t i = 0; i < _df.num_numericals(); ++i) {
    const auto& colname = _df.numerical(i).name();
    colnames.push_back(colname);
  }

  for (size_t i = 0; i < _df.num_targets(); ++i) {
    const auto& colname = _df.target(i).name();
    colnames.push_back(colname);
  }

  for (size_t i = 0; i < _df.num_text(); ++i) {
    const auto& colname = _df.text(i).name();
    colnames.push_back(colname);
  }

  for (size_t i = 0; i < _df.num_time_stamps(); ++i) {
    const auto& colname = _df.time_stamp(i).name();
    colnames.push_back(colname);
  }

  for (size_t i = 0; i < _df.num_unused_floats(); ++i) {
    const auto& colname = _df.unused_float(i).name();
    colnames.push_back(colname);
  }

  for (size_t i = 0; i < _df.num_unused_strings(); ++i) {
    const auto& colname = _df.unused_string(i).name();
    colnames.push_back(colname);
  }

  for (auto& name : colnames) {
    name = io::Parser::remove_quotechars(name, _quotechar);
  }

  return colnames;
}

// ----------------------------------------------------------------------------

std::vector<io::Datatype> DataFrameReader::make_coltypes(const DataFrame& _df) {
  std::vector<io::Datatype> coltypes;

  for (size_t i = 0; i < _df.num_categoricals(); ++i) {
    coltypes.push_back(io::Datatype::string);
  }

  for (size_t i = 0; i < _df.num_join_keys(); ++i) {
    coltypes.push_back(io::Datatype::string);
  }

  for (size_t i = 0; i < _df.num_numericals(); ++i) {
    if (_df.numerical(i).unit().find("time stamp") != std::string::npos) {
      coltypes.push_back(io::Datatype::string);
    } else {
      coltypes.push_back(io::Datatype::double_precision);
    }
  }

  for (size_t i = 0; i < _df.num_targets(); ++i) {
    coltypes.push_back(io::Datatype::double_precision);
  }

  for (size_t i = 0; i < _df.num_text(); ++i) {
    coltypes.push_back(io::Datatype::string);
  }

  for (size_t i = 0; i < _df.num_time_stamps(); ++i) {
    coltypes.push_back(io::Datatype::time_stamp);
  }

  for (size_t i = 0; i < _df.num_unused_floats(); ++i) {
    if (_df.unused_float(i).unit().find("time stamp") != std::string::npos) {
      coltypes.push_back(io::Datatype::string);
    } else {
      coltypes.push_back(io::Datatype::double_precision);
    }
  }

  for (size_t i = 0; i < _df.num_unused_strings(); ++i) {
    coltypes.push_back(io::Datatype::string);
  }

  return coltypes;
}

// ----------------------------------------------------------------------------

std::vector<std::string> DataFrameReader::next_line() {
  // Usually the calling function should make sure that we haven't reached
  // the end of file. But just to be sure, we do it again.
  if (eof()) {
    return std::vector<std::string>();
  }

  assert_true(colnames().size() == coltypes().size());

  std::vector<std::string> result(coltypes().size());

  size_t col = 0;

  for (size_t i = 0; i < df_.num_categoricals(); ++i) {
    const auto& val = df_.categorical(i)[rownum_];
    result[col++] = categories()[val].str();
  }

  for (size_t i = 0; i < df_.num_join_keys(); ++i) {
    const auto& val = df_.join_key(i)[rownum_];
    result[col++] = join_keys_encoding()[val].str();
  }

  for (size_t i = 0; i < df_.num_numericals(); ++i) {
    const auto& val = df_.numerical(i)[rownum_];
    if (coltypes()[col] == io::Datatype::string)
      result[col++] = io::Parser::ts_to_string(val);
    else
      result[col++] = io::Parser::to_precise_string(val);
  }

  for (size_t i = 0; i < df_.num_targets(); ++i) {
    const auto& val = df_.target(i)[rownum_];
    result[col++] = io::Parser::to_precise_string(val);
  }

  for (size_t i = 0; i < df_.num_text(); ++i) {
    const auto& val = df_.text(i)[rownum_];
    result[col++] = val.str();
  }

  for (size_t i = 0; i < df_.num_time_stamps(); ++i) {
    const auto& val = df_.time_stamp(i)[rownum_];
    result[col++] = io::Parser::ts_to_string(val);
  }

  for (size_t i = 0; i < df_.num_unused_floats(); ++i) {
    const auto& val = df_.unused_float(i)[rownum_];
    if (coltypes()[col] == io::Datatype::string)
      result[col++] = io::Parser::ts_to_string(val);
    else
      result[col++] = io::Parser::to_precise_string(val);
  }

  for (size_t i = 0; i < df_.num_unused_strings(); ++i) {
    const auto& val = df_.unused_string(i)[rownum_];
    result[col++] = val.str();
  }

  assert_true(col == result.size());

  ++rownum_;

  return result;
}

// ----------------------------------------------------------------------------

void DataFrameReader::update_counts(const std::string& _colname,
                                    std::map<std::string, Int>* _counts) {
  const auto it = _counts->find(_colname);

  if (it == _counts->end()) {
    (*_counts)[_colname] = 1;
  } else {
    (*_counts)[_colname] += 1;
  }
}

// ----------------------------------------------------------------------------
}  // namespace containers
