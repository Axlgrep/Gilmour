//  Copyright (c) 2017-present The gilmour Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
#include <vector>
#include <thread>
#include <functional>

#include "blackwidow/blackwidow.h"

const int TEN_MILLION = 10000000;

using namespace blackwidow;
using namespace std::chrono;

// Nemo performance more stronger
// bw 45471ms  | nm 41529ms
// Nemo的Set接口没有上锁, 虽然速度比较快, 但是不安全
void BenchSet() {
  printf("====== Set ======\n");
  blackwidow::Options options;
  options.create_if_missing = true;
  blackwidow::BlackWidow db;
  blackwidow::Status s = db.Open(options, "./db");

  if (!s.ok()) {
    printf("Open db failed, error: %s\n", s.ToString().c_str());
    return;
  }
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (int i = 0; i < TEN_MILLION; i++) {
    keys.push_back("KEY_" + std::to_string(i));
    values.push_back("VALUE_" + std::to_string(i));
  }

  auto start = std::chrono::system_clock::now();
  for (uint32_t i = 0; i < TEN_MILLION; ++i) {
    db.Set(keys[i], values[i]);
  }

  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test Set " << TEN_MILLION << " Cost: " << cost << "ms" << std::endl;
}

// Blackwidow performance more stronger
// bw 31421ms | nm 192984ms
//
// 因为blackwidow内可以支持多个Field一次删除, 而nemo只能
// 单个单个Field删除，期间每次操作都要上锁解锁, 所以比较慢
void BenchHDel() {
  printf("====== HDel ======\n");
  blackwidow::Options options;
  options.create_if_missing = true;
  blackwidow::BlackWidow db;
  blackwidow::Status s = db.Open(options, "./db");

  if (!s.ok()) {
    printf("Open db failed, error: %s\n", s.ToString().c_str());
    return;
  }

  int32_t ret = 0;
  BlackWidow::FieldValue fv;
  std::vector<std::string> fields;
  std::vector<BlackWidow::FieldValue> fvs;

  fvs.clear();
  for (size_t i = 0; i < TEN_MILLION; ++i) {
    fv.field = "FIELD_" + std::to_string(i);
    fv.value = "VALUE_" + std::to_string(i);
    fvs.push_back(fv);
    fields.push_back("FIELD_" + std::to_string(i));
  }
  db.HMSet("HDEL_KEY", fvs);

  auto start = system_clock::now();
  db.HDel("HDEL_KEY", fields, &ret);
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "HDel " << fvs.size()
    << " Hash Field Cost: "<< cost << "ms" << std::endl;
}

// Case 1
// Performance is equal
// bw 4ms | nm 5ms
//
// Case 2
// Blackwidow performance more stronger
// bw 2ms | nm 3540ms
// blackwidow把Version放到了Hash键当中, 当我们删除一个Hash表然后创建同名Hash表,
// version会变化, 我们可以快速的Seek到rockdb中当前Hash表的数据.
// nemo将Version放到Hash值当中, 上面这种情况会遍历新Hash表旧Hash表所有的Field,
// 然后逐一匹配version, 所以速度比较慢
//
// Case 3
// Nemo performance more stronger
// bw 3000ms | nm 600ms
// 原因待查
void BenchHGetall() {
  printf("====== HGetall ======\n");
  blackwidow::Options options;
  options.create_if_missing = true;
  blackwidow::BlackWidow db;
  blackwidow::Status s = db.Open(options, "./db");

  if (!s.ok()) {
    printf("Open db failed, error: %s\n", s.ToString().c_str());
    return;
  }

  int32_t ret = 0;
  BlackWidow::FieldValue fv;
  std::vector<std::string> fields;
  std::vector<BlackWidow::FieldValue> fvs_in;
  std::vector<BlackWidow::FieldValue> fvs_out;

  // 1. Create the hash table then insert hash table 10000 field
  // 2. HGetall the hash table 10000 field (statistics cost time)
  fvs_in.clear();
  for (size_t i = 0; i < 10000; ++i) {
    fv.field = "FIELD_" + std::to_string(i);
    fv.value = "VALUE_" + std::to_string(i);
    fvs_in.push_back(fv);
  }
  db.HMSet("HGETALL_KEY1", fvs_in);

  fvs_out.clear();
  auto start = system_clock::now();
  db.HGetall("HGETALL_KEY1", &fvs_out);
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test case 1, HGetall " << fvs_out.size()
    << " Field HashTable Cost: "<< cost << "ms" << std::endl;



  // 1. Create the hash table then insert hash table 10000000 field
  // 2. Delete the hash table
  // 3. Create the hash table whos key same as before,
  //    then insert the hash table 10000 field
  // 4. HGetall the hash table 10000 field (statistics cost time)
  fvs_in.clear();
  for (size_t i = 0; i < TEN_MILLION; ++i) {
    fv.field = "FIELD_" + std::to_string(i);
    fv.value = "VALUE_" + std::to_string(i);
    fvs_in.push_back(fv);
  }
  db.HMSet("HGETALL_KEY2", fvs_in);
  std::vector<Slice> del_keys({"HGETALL_KEY2"});
  std::map<BlackWidow::DataType, Status> type_status;
  db.Del(del_keys, &type_status);
  fvs_in.clear();
  for (size_t i = 0; i < 10000; ++i) {
    fv.field = "FIELD_" + std::to_string(i);
    fv.value = "VALUE_" + std::to_string(i);
    fvs_in.push_back(fv);
  }
  db.HMSet("HGETALL_KEY2", fvs_in);

  fvs_out.clear();
  start = system_clock::now();
  db.HGetall("HGETALL_KEY2", &fvs_out);
  end = system_clock::now();
  elapsed_seconds = end - start;
  cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test case 2, HGetall " << fvs_out.size()
    << " Field HashTable Cost: "<< cost << "ms" << std::endl;

  // 1. Create the hash table then insert hash table 10000000 field
  // 2. Delete hash table 9990000 field, the hash table remain 10000 field
  // 3. HGetall the hash table 10000 field (statistics cost time)
  fvs_in.clear();
  for (size_t i = 0; i < TEN_MILLION; ++i) {
    fv.field = "FIELD_" + std::to_string(i);
    fv.value = "VALUE_" + std::to_string(i);
    fvs_in.push_back(fv);
  }
  db.HMSet("HGETALL_KEY3", fvs_in);
  fields.clear();
  for (size_t i = 0; i < TEN_MILLION - 10000; ++i) {
    fields.push_back("FIELD_" + std::to_string(i));
  }
  db.HDel("HGETALL_KEY3", fields, &ret);

  fvs_out.clear();
  start = system_clock::now();
  db.HGetall("HGETALL_KEY3", &fvs_out);
  end = system_clock::now();
  elapsed_seconds = end - start;
  cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test case 3, HGetall " << fvs_out.size()
    << " Field HashTable Cost: "<< cost << "ms" << std::endl;
}

int main() {
  // keys
  BenchSet();

  // hashes
  BenchHDel();
  BenchHGetall();
}
