//
//  core_workload.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/9/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "uniform_generator.h"
#include "zipfian_generator.h"
#include "scrambled_zipfian_generator.h"
#include "skewed_latest_generator.h"
#include "complex_zipfian_generator.h"
#include "flurry_generator.h"
#include "const_generator.h"
#include "core_workload.h"

#include <string>
#include <cmath>

using ycsbc::CoreWorkload;
using std::string;

const string CoreWorkload::TABLENAME_PROPERTY = "table";
const string CoreWorkload::TABLENAME_DEFAULT = "usertable";

const string CoreWorkload::FIELD_COUNT_PROPERTY = "fieldcount";
const string CoreWorkload::FIELD_COUNT_DEFAULT = "1";

const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_PROPERTY =
    "field_len_dist";
const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_DEFAULT = "constant";

const string CoreWorkload::FIELD_LENGTH_PROPERTY = "fieldlength";
const string CoreWorkload::FIELD_LENGTH_DEFAULT = "100";

const string CoreWorkload::READ_ALL_FIELDS_PROPERTY = "readallfields";
const string CoreWorkload::READ_ALL_FIELDS_DEFAULT = "true";

const string CoreWorkload::WRITE_ALL_FIELDS_PROPERTY = "writeallfields";
const string CoreWorkload::WRITE_ALL_FIELDS_DEFAULT = "false";

const string CoreWorkload::READ_PROPORTION_PROPERTY = "readproportion";
const string CoreWorkload::READ_PROPORTION_DEFAULT = "0.95";

const string CoreWorkload::UPDATE_PROPORTION_PROPERTY = "updateproportion";
const string CoreWorkload::UPDATE_PROPORTION_DEFAULT = "0.05";

const string CoreWorkload::INSERT_PROPORTION_PROPERTY = "insertproportion";
const string CoreWorkload::INSERT_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::SCAN_PROPORTION_PROPERTY = "scanproportion";
const string CoreWorkload::SCAN_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::READMODIFYWRITE_PROPORTION_PROPERTY =
    "readmodifywriteproportion";
const string CoreWorkload::READMODIFYWRITE_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::REQUEST_DISTRIBUTION_PROPERTY =
    "requestdistribution";
const string CoreWorkload::REQUEST_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";
const string CoreWorkload::MAX_SCAN_LENGTH_DEFAULT = "1000";

const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_PROPERTY =
    "scanlengthdistribution";
const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::INSERT_ORDER_PROPERTY = "insertorder";
const string CoreWorkload::INSERT_ORDER_DEFAULT = "hashed";

const string CoreWorkload::INSERT_START_PROPERTY = "insertstart";
const string CoreWorkload::INSERT_START_DEFAULT = "0";

const string CoreWorkload::RECORD_COUNT_PROPERTY = "recordcount";
const string CoreWorkload::OPERATION_COUNT_PROPERTY = "operationcount";
const string CoreWorkload::KEY_RANGE_PROPERTY = "keyrange";
const string CoreWorkload::KEY_RANGE_DEFAULT = "0";

const string CoreWorkload::EXACT_KEY_SIZE = "exact_key_size";
const string CoreWorkload::EXACT_KEY_SIZE_DEFAULT = "24";

const string CoreWorkload::VALIDATE_GETS_PROPERTY = "validateGets";
const string CoreWorkload::VALIDATE_GETS_DEFAULT = "false";

const string CoreWorkload::MIN_VAL_HISTOGRAM_PROPERTY = "histmin";
const string CoreWorkload::MIN_VAL_HISTOGRAM_DEFAULT = "0.0";
const string CoreWorkload::MAX_VAL_HISTOGRAM_PROPERTY = "histmax";
const string CoreWorkload::MAX_VAL_HISTOGRAM_DEFAULT = "2.5";
const string CoreWorkload::BUCKETS_HISTOGRAM_PROPERTY = "histbuckets";
const string CoreWorkload::BUCKETS_HISTOGRAM_DEFAULT = "20000";

void CoreWorkload::Init(const utils::Properties &p) {
  table_name_ = p.GetProperty(TABLENAME_PROPERTY,TABLENAME_DEFAULT);
  
  field_count_ = std::stoi(p.GetProperty(FIELD_COUNT_PROPERTY,
                                         FIELD_COUNT_DEFAULT));
  field_len_generator_ = GetFieldLenGenerator(p);
  
  double read_proportion = std::stod(p.GetProperty(READ_PROPORTION_PROPERTY,
                                                   READ_PROPORTION_DEFAULT));
  double update_proportion = std::stod(p.GetProperty(UPDATE_PROPORTION_PROPERTY,
                                                     UPDATE_PROPORTION_DEFAULT));
  double insert_proportion = std::stod(p.GetProperty(INSERT_PROPORTION_PROPERTY,
                                                     INSERT_PROPORTION_DEFAULT));
  double scan_proportion = std::stod(p.GetProperty(SCAN_PROPORTION_PROPERTY,
                                                   SCAN_PROPORTION_DEFAULT));
  double readmodifywrite_proportion = std::stod(p.GetProperty(
      READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_DEFAULT));
  
  record_count_ = std::stoi(p.GetProperty(RECORD_COUNT_PROPERTY));
  key_range_ = std::stoi(p.GetProperty(KEY_RANGE_PROPERTY,
                                       KEY_RANGE_DEFAULT));
  std::string request_dist = p.GetProperty(REQUEST_DISTRIBUTION_PROPERTY,
                                           REQUEST_DISTRIBUTION_DEFAULT);
  int max_scan_len = std::stoi(p.GetProperty(MAX_SCAN_LENGTH_PROPERTY,
                                             MAX_SCAN_LENGTH_DEFAULT));
  std::string scan_len_dist = p.GetProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY,
                                            SCAN_LENGTH_DISTRIBUTION_DEFAULT);
  int insert_start = std::stoi(p.GetProperty(INSERT_START_PROPERTY,
                                             INSERT_START_DEFAULT));
  
  read_all_fields_ = utils::StrToBool(p.GetProperty(READ_ALL_FIELDS_PROPERTY,
                                                    READ_ALL_FIELDS_DEFAULT));
  write_all_fields_ = utils::StrToBool(p.GetProperty(WRITE_ALL_FIELDS_PROPERTY,
                                                     WRITE_ALL_FIELDS_DEFAULT));
  
  hist_min_ = std::stod(p.GetProperty(MIN_VAL_HISTOGRAM_PROPERTY, MIN_VAL_HISTOGRAM_DEFAULT));
  hist_max_ = std::stod(p.GetProperty(MAX_VAL_HISTOGRAM_PROPERTY, MAX_VAL_HISTOGRAM_DEFAULT));
  hist_buckets_ = std::stoll(p.GetProperty(BUCKETS_HISTOGRAM_PROPERTY, BUCKETS_HISTOGRAM_DEFAULT));

  if (p.GetProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_DEFAULT) == "hashed") {
    ordered_inserts_ = false;
  } else {
    ordered_inserts_ = true;
  }
  
  key_generator_ = new CounterGenerator(insert_start);
  
  if (read_proportion > 0) {
    op_chooser_.AddValue(READ, read_proportion);
  }
  if (update_proportion > 0) {
    op_chooser_.AddValue(UPDATE, update_proportion);
  }
  if (insert_proportion > 0) {
    op_chooser_.AddValue(INSERT, insert_proportion);
  }
  if (scan_proportion > 0) {
    op_chooser_.AddValue(SCAN, scan_proportion);
  }
  if (readmodifywrite_proportion > 0) {
    op_chooser_.AddValue(READMODIFYWRITE, readmodifywrite_proportion);
  }
  
  insert_key_sequence_.Set(record_count_); // is only needed for request_dist="latest"?

  
  //choose the required generator type, set the range and initialize the generator
  if (request_dist == "uniform") {
    if (key_range_ == 0)
        key_range_ = record_count_ - 1;
    key_chooser_ = new UniformGenerator(0, key_range_);
    
  } else if (request_dist == "zipfian") {
    // If the number of keys changes, we don't want to change popular keys.
    // So we construct the scrambled zipfian generator with a keyspace
    // that is larger than what exists at the beginning of the test.
    // If the generator picks a key that is not inserted yet, we just ignore it
    // and pick another key.
    int op_count = std::stoi(p.GetProperty(OPERATION_COUNT_PROPERTY));
    if (key_range_ == 0)
        key_range_ = std::max<size_t>(record_count_, op_count * insert_proportion) * 2; // a fudge factor
    key_chooser_ = new ScrambledZipfianGenerator(key_range_);
    
  } else if (request_dist == "complex") {
    int op_count = std::stoi(p.GetProperty(OPERATION_COUNT_PROPERTY));
    if (key_range_ == 0)
        key_range_ = std::max<size_t>(record_count_, op_count * insert_proportion) * 2; // a fudge factor
    key_chooser_ = new ComplexZipfianGenerator(key_range_);

  } else if (request_dist == "flurry") {
    int op_count = std::stoi(p.GetProperty(OPERATION_COUNT_PROPERTY));
    if (key_range_ == 0)
        key_range_ = std::max<size_t>(record_count_, op_count * insert_proportion) * 2; // a fudge factor
    constexpr size_t PaddingPart = 4;
    const size_t PaddingBits = std::log2(record_count_) / PaddingPart;
    key_chooser_ = new FlurryGenerator(key_range_, PaddingBits);

  } else if (request_dist == "latest") {
    if (key_range_ == 0)
        key_range_ = record_count_;
    insert_key_sequence_.Set(key_range_);
    key_chooser_ = new SkewedLatestGenerator(insert_key_sequence_);
    
  } else {
    throw utils::Exception("Unknown request distribution: " + request_dist);
  }
  
  field_chooser_ = new UniformGenerator(0, field_count_ - 1);
  
  if (scan_len_dist == "uniform") {
    scan_len_chooser_ = new UniformGenerator(1, max_scan_len);
  } else if (scan_len_dist == "zipfian") {
    scan_len_chooser_ = new ZipfianGenerator(1, max_scan_len);
  } else {
    throw utils::Exception("Distribution not allowed for scan length: " +
        scan_len_dist);
  }
  string keySize = p.GetProperty(EXACT_KEY_SIZE, EXACT_KEY_SIZE_DEFAULT);
  if (!keySize.empty())
	  exact_key_size_ = std::stoi(keySize);

  validateGets_ = utils::StrToBool(p.GetProperty(VALIDATE_GETS_PROPERTY,
                                                 VALIDATE_GETS_DEFAULT));
}

ycsbc::Generator<uint64_t> *CoreWorkload::GetFieldLenGenerator(
    const utils::Properties &p) {
  string field_len_dist = p.GetProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY,
                                        FIELD_LENGTH_DISTRIBUTION_DEFAULT);
  int field_len = std::stoi(p.GetProperty(FIELD_LENGTH_PROPERTY,
                                          FIELD_LENGTH_DEFAULT));
  if(field_len_dist == "constant") {
    return new ConstGenerator(field_len);
  } else if(field_len_dist == "uniform") {
    return new UniformGenerator(1, field_len);
  } else if(field_len_dist == "zipfian") {
    return new ZipfianGenerator(1, field_len);
  } else {
    throw utils::Exception("Unknown field length distribution: " +
        field_len_dist);
  }
}

void CoreWorkload::BuildValues(std::vector<ycsbc::DB::KVPair> &values) {
  for (int i = 0; i < field_count_; ++i) {
    ycsbc::DB::KVPair pair;
    pair.first.append("field").append(std::to_string(i));
    pair.second.append(field_len_generator_->Next(), utils::RandomPrintChar());
    values.push_back(pair);
  }
}

void CoreWorkload::BuildUpdate(std::vector<ycsbc::DB::KVPair> &update) {
  ycsbc::DB::KVPair pair;
  pair.first.append(NextFieldName());
  pair.second.append(field_len_generator_->Next(), utils::RandomPrintChar());
  update.push_back(pair);
}

