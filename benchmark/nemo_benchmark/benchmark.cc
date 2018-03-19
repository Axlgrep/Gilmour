//  Copyright (c) 2017-present The gilmour Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
#include <vector>
#include <thread>
#include <functional>

#include "nemo.h"

const int TEN_THOUSAND = 10000;
const int ONE_HUNDRED_THOUSAND = 100000;
const int TEN_MILLION = 10000000;

using namespace std::chrono;

void BenchSet() {
  printf("====== Set ======\n");
  nemo::Options options;
  options.create_if_missing = true;
  nemo::Nemo* db = new nemo::Nemo("./db", options);

  if (!db) {
    printf("Open db failed\n");
    return;
  }
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (int i = 0; i < TEN_MILLION; i++) {
    keys.push_back("KEY_" + std::to_string(i));
    values.push_back("VALUE_" + std::to_string(i));
  }

  auto start = system_clock::now();
  for (uint32_t i = 0; i < TEN_MILLION; ++i) {
    db->Set(keys[i], values[i]);
  }

  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test Set " << TEN_MILLION << " Cost: " << cost << "ms" << std::endl;
  delete db;
}

void BenchHDel() {
  printf("====== HDel ======\n");
  nemo::Options options;
  options.create_if_missing = true;
  nemo::Nemo* db = new nemo::Nemo("./db", options);

  if (!db) {
    printf("Open db failed\n");
    return;
  }

  int32_t ret = 0;
  nemo::FV fv;
  std::vector<std::string> fields;
  std::vector<nemo::FV> fvs;

  fvs.clear();
  for (size_t i = 0; i < TEN_MILLION; ++i) {
    fv.field = "FIELD_" + std::to_string(i);
    fv.val = "VALUE_" + std::to_string(i);
    fvs.push_back(fv);
    fields.push_back("FIELD_" + std::to_string(i));
  }
  db->HMSet("HDEL_KEY", fvs);

  auto start = system_clock::now();
  for (const auto& field : fields) {
    db->HDel("HDEL_KEY", field);
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "HDel " << fvs.size()
    << " Hash Field Cost: "<< cost << "ms" << std::endl;
  delete db;
}

void BenchHGetall() {
  printf("====== HGetall ======\n");
  nemo::Options options;
  options.create_if_missing = true;
  nemo::Nemo* db = new nemo::Nemo("./db", options);

  if (!db) {
    printf("Open db failed\n");
    return;
  }

  int32_t ret = 0;
  nemo::FV fv;
  std::vector<nemo::FV> fvs_in;
  std::vector<nemo::FV> fvs_out;

  // 1. Create the hash table then insert hash table 100000 field
  // 2. HGetall the hash table 100000 field (statistics cost time)
  fvs_in.clear();
  for (size_t i = 0; i < ONE_HUNDRED_THOUSAND; ++i) {
    fv.field = "FIELD_" + std::to_string(i);
    fv.val = "VALUE_" + std::to_string(i);
    fvs_in.push_back(fv);
  }
  db->HMSet("HGETALL_KEY1", fvs_in);

  fvs_out.clear();
  auto start = system_clock::now();
  db->HGetall("HGETALL_KEY1", fvs_out);
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test case 1, HGetall " << fvs_out.size()
    << " Field HashTable Cost: "<< cost << "ms" << std::endl;


  // 1. Create the hash table then insert hash table 10000000 field
  // 2. Delete the hash table
  // 3. Create the hash table whos key same as before,
  //    then insert the hash table 100000 field
  // 4. HGetall the hash table 100000 field (statistics cost time)
  fvs_in.clear();
  for (size_t i = 0; i < TEN_MILLION; ++i) {
    fv.field = "FIELD_" + std::to_string(i);
    fv.val = "VALUE_" + std::to_string(i);
    fvs_in.push_back(fv);
  }
  db->HMSet("HGETALL_KEY2", fvs_in);
  int64_t count;
  db->Del("HGETALL_KEY2", &count);
  fvs_in.clear();
  for (size_t i = 0; i < ONE_HUNDRED_THOUSAND; ++i) {
    fv.field = "FIELD_" + std::to_string(i);
    fv.val = "VALUE_" + std::to_string(i);
    fvs_in.push_back(fv);
  }
  db->HMSet("HGETALL_KEY2", fvs_in);

  fvs_out.clear();
  start = system_clock::now();
  db->HGetall("HGETALL_KEY2", fvs_out);
  end = system_clock::now();
  elapsed_seconds = end - start;
  cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test case 2, HGetall " << fvs_out.size()
    << " Field HashTable Cost: "<< cost << "ms" << std::endl;

  // 1. Create the hash table then insert hash table 10000000 field
  // 2. Delete hash table 9990000 field, the hash table remain 100000 field
  // 3. HGetall the hash table 100000 field (statistics cost time)
  fvs_in.clear();
  for (size_t i = 0; i < TEN_MILLION; ++i) {
    fv.field = "FIELD_" + std::to_string(i);
    fv.val = "VALUE_" + std::to_string(i);
    fvs_in.push_back(fv);
  }
  db->HMSet("HGETALL_KEY3", fvs_in);
  for (size_t i = 0; i < TEN_MILLION - ONE_HUNDRED_THOUSAND; ++i) {
    db->HDel("HGETALL_KEY3", "FIELD_" + std::to_string(i));
  }

  fvs_out.clear();
  start = system_clock::now();
  db->HGetall("HGETALL_KEY3", fvs_out);
  end = system_clock::now();
  elapsed_seconds = end - start;
  cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test case 3, HGetall " << fvs_out.size()
    << " Field HashTable Cost: "<< cost << "ms" << std::endl;

  delete db;
}

void BenchSAdd() {
  printf("====== SAdd ======\n");
  nemo::Options options;
  options.create_if_missing = true;
  nemo::Nemo* db = new nemo::Nemo("./db", options);

  if (!db) {
    printf("Open db failed\n");
    return;
  }

  int64_t ret;
  std::vector<std::string> members_in;
  for (int i = 0; i < TEN_MILLION; i++) {
    members_in.push_back("MEMBER_" + std::to_string(i));
  }

  auto start = system_clock::now();
  for (const auto& member : members_in) {
    db->SAdd("SADD_KEY", member, &ret);
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test SAdd " << TEN_MILLION << " Cost: " << cost << "ms" << std::endl;

  delete db;
}

void BenchSPop() {
  printf("====== SPop ======\n");
  nemo::Options options;
  options.create_if_missing = true;
  nemo::Nemo* db = new nemo::Nemo("./db", options);

  if (!db) {
    printf("Open db failed\n");
    return;
  }

  int64_t ret;
  std::vector<std::string> members_in;
  std::string members_out;
  for (int i = 0; i < TEN_MILLION; i++) {
    members_in.push_back("MEMBER_" + std::to_string(i));
  }
  for (const auto& member : members_in) {
    db->SAdd("SPOP_KEY", member, &ret);
  }

  auto start = system_clock::now();
  for (uint32_t i = 0; i < ONE_HUNDRED_THOUSAND; ++i) {
    db->SPop("SPOP_KEY", members_out);
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test SPop " << TEN_THOUSAND << " Cost: " << cost << "ms" << std::endl;

  delete db;
}

void BenchSMembers() {
  printf("====== SMembers ======\n");
  nemo::Options options;
  options.create_if_missing = true;
  nemo::Nemo* db = new nemo::Nemo("./db", options);

  if (!db) {
    printf("Open db failed\n");
    return;
  }

  // Test Case 1
  int64_t ret;
  std::vector<std::string> members_out;
  for (int i = 0; i < ONE_HUNDRED_THOUSAND; i++) {
    db->SAdd("SMEMBERS_KEY1", "MEMBER_" + std::to_string(i), &ret);
  }

  auto start = system_clock::now();
  db->SMembers("SMEMBERS_KEY1", members_out);
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test Case 1, SMembers " << ONE_HUNDRED_THOUSAND << " Cost: " << cost << "ms" << std::endl;


  // Test Case 2
  for (size_t i = 0; i < TEN_MILLION; i++) {
    db->SAdd("SMEMBERS_KEY2", "MEMBER_" + std::to_string(i), &ret);
  }
  int64_t count;
  db->Del("SMEMBERS_KEY2", &count);
  for (size_t i = 0; i < ONE_HUNDRED_THOUSAND; i++) {
    db->SAdd("SMEMBERS_KEY2", "MEMBER_" + std::to_string(i), &ret);
  }

  start = system_clock::now();
  db->SMembers("SMEMBERS_KEY2", members_out);
  end = system_clock::now();
  elapsed_seconds = end - start;
  cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test Case 2, SMembers " << ONE_HUNDRED_THOUSAND << " Cost: " << cost << "ms" << std::endl;
  delete db;
}

int main() {
  // keys
  //BenchSet();

  // hashes
  //BenchHDel();
  //BenchHGetall();

  // sets
  //BenchSAdd();
  //BenchSPop();
  //BenchSMembers();
}
