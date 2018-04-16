//  Copyright (c) 2017-present The gilmour Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
#include <vector>
#include <thread>
#include <random>
#include <functional>

#include "nemo.h"

const int KEY_SIZE = 50;
const int VALUE_SIZE = 50;
const int MEMBER_SIZE = 50;
const int FIELD_SIZE = 50;
const int THREADNUM = 20;
const int ONE_HUNDRED = 100;
const int ONE_THOUSAND = 1000;
const int TEN_THOUSAND = 10000;
const int ONE_HUNDRED_THOUSAND = 100000;
const int ONE_MILLION = 1000000;
const int TEN_MILLION = 10000000;

using namespace std::chrono;
using std::default_random_engine;

static int32_t last_seed = 0;
const std::string KEY_PREFIX = "KEY_";
const std::string VALUE_PREFIX = "VALUE_";
const std::string FIELD_PREFIX = "FIELD_";
const std::string MEMBER_PREFIX = "MEMBER_";

void GenerateRandomString(const std::string& prefix,
                          int32_t len,
                          std::string* target) {
  target->clear();
  char c_map[67] = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'g',
                    'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
                    'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D',
                    'E', 'F', 'G', 'H', 'I', 'G', 'K', 'L', 'M', 'N',
                    'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
                    'Y', 'Z', '~', '!', '@', '#', '$', '%', '^', '&',
                    '*', '(', ')', '-', '=', '_', '+'};

  if (prefix.size() >= len) {
    *target = prefix.substr(0, len);
  } else {
    *target = prefix;
    default_random_engine e;
    for (int i = 0; i < len - prefix.size(); i++) {
      e.seed(last_seed);
      last_seed = e();
      int32_t rand_num = last_seed % 67;
      target->push_back(c_map[rand_num]);
    }
  }
}

void BenchSet() {
  printf("====== Set ======\n");
  nemo::Options options;
  options.create_if_missing = true;
  nemo::Nemo* db = new nemo::Nemo("./db", options);

  if (!db) {
    printf("Open db failed\n");
    return;
  }
  nemo::KV kv;
  std::vector<nemo::KV> kvs;
  for (int i = 0; i < TEN_MILLION; i++) {
    GenerateRandomString(KEY_PREFIX, KEY_SIZE, &kv.key);
    GenerateRandomString(KEY_PREFIX, KEY_SIZE, &kv.val);
    kvs.push_back(kv);
  }

  auto start = system_clock::now();
  for (const auto& kv : kvs) {
    db->Set(kv.key, kv.val);
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<std::chrono::seconds>(elapsed_seconds).count();
  std::cout << "Test Set " << kvs.size() << " KV Cost: " << cost << "s QPS: "
    << kvs.size() / cost << std::endl;
  delete db;
}

void BenchMultiThreadSet() {
  printf("====== Multi Thread Set ======\n");
  nemo::Options options;
  options.create_if_missing = true;
  nemo::Nemo* db = new nemo::Nemo("./db", options);

  if (!db) {
    printf("Open db failed\n");
    return;
  }

  nemo::KV kv;
  std::vector<nemo::KV> kvs;
  for (int i = 0; i < TEN_MILLION; i++) {
    GenerateRandomString(KEY_PREFIX, KEY_SIZE, &kv.key);
    GenerateRandomString(KEY_PREFIX, KEY_SIZE, &kv.val);
    kvs.push_back(kv);
  }

  std::vector<std::thread> jobs;
  auto start = system_clock::now();
  for (size_t i = 0; i < THREADNUM; ++i) {
    jobs.emplace_back([&db](std::vector<nemo::KV> kvs) {
      for (const auto& kv : kvs) {
        db->Set(kv.key, kv.val);
      }
    }, kvs);
  }
  for (auto& job : jobs) {
    job.join();
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<std::chrono::seconds>(elapsed_seconds).count();
  std::cout << "Test MultiThread Set " << THREADNUM * kvs.size() << " KV Cost: "
    << cost << "s QPS: " << (THREADNUM * kvs.size()) / cost << std::endl;
  delete db;
}

void BenchScan() {
  printf("====== Scan ======\n");
  nemo::Options options;
  options.create_if_missing = true;
  nemo::Nemo* db = new nemo::Nemo("./db", options);

  if (!db) {
    printf("Open db failed\n");
    return;
  }

  int32_t ret = 0;
  nemo::FV fv;
  std::vector<nemo::FV> fvs;

  for (size_t i = 0; i < TEN_THOUSAND; ++i) {
    fv.field = "FIELD_" + std::to_string(i);
    fv.val   = "VALUE_" + std::to_string(i);
    fvs.push_back(fv);
  }
  for (size_t i = 0; i < TEN_THOUSAND; ++i) {
    db->HMSet("SCAN_KEY" + std::to_string(i), fvs);
  }

  int64_t total = 0;
  std::string pattern = "SCAN_KEY*";
  int64_t cursor_origin, cursor_ret = 0;
  std::vector<std::string> keys;
  auto start = system_clock::now();
  for (; ;) {
    total += keys.size();
    keys.clear();
    cursor_origin = cursor_ret;
    db->Scan(cursor_origin, pattern, 10, keys, &cursor_ret);
    if (cursor_ret == 0) {
      break;
    }
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test Scan " << total
    << " Keys Cost: "<< cost << "ms" << std::endl;

  delete db;
}

void BenchKeys() {
  printf("====== Keys * ======\n");
  nemo::Options options;
  options.create_if_missing = true;
  nemo::Nemo* db = new nemo::Nemo("./db", options);

  if (!db) {
    printf("Open db failed\n");
    return;
  }

  int64_t ret = 0;
  std::string member;
  std::vector<std::string> members_in;
  nemo::FV fv;
  std::vector<nemo::FV> fvs;
  for (size_t i = 0; i < TEN_THOUSAND; ++i) {
    GenerateRandomString(MEMBER_PREFIX, MEMBER_SIZE, &member);
    members_in.push_back(member);
    GenerateRandomString(FIELD_PREFIX, FIELD_SIZE, &fv.field);
    GenerateRandomString(VALUE_PREFIX, VALUE_SIZE, &fv.val);
    fvs.push_back(fv);
  }

  std::string key, value;
  for (size_t i = 0; i < ONE_THOUSAND; ++i) {
    GenerateRandomString(KEY_PREFIX, KEY_SIZE, &key);
    for (const auto& member : members_in) {
      db->SAdd(key, member, &ret);
    }

    GenerateRandomString(KEY_PREFIX, KEY_SIZE, &key);
    db->HMSet(key, fvs);

    GenerateRandomString(KEY_PREFIX, KEY_SIZE, &key);
    GenerateRandomString(KEY_PREFIX, KEY_SIZE, &value);
    db->Set(key, value);
  }

  int64_t total = 0;
  std::string pattern = "*";
  int64_t cursor_origin, cursor_ret = 0;
  std::vector<std::string> keys;
  auto start = system_clock::now();
  for (; ;) {
    keys.clear();
    cursor_origin = cursor_ret;
    db->Scan(cursor_origin, pattern, 10, keys, &cursor_ret);
    total += keys.size();
    if (cursor_ret == 0) {
      break;
    }
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test Keys " << total
    << " Keys Cost: "<< cost << "ms" << std::endl;

  delete db;
}

void BenchHSet() {
  printf("====== HSet ======\n");
  nemo::Options options;
  options.create_if_missing = true;
  nemo::Nemo* db = new nemo::Nemo("./db", options);

  if (!db) {
    printf("Open db failed\n");
    return;
  }

  nemo::FV fv;
  std::vector<nemo::FV> fvs;

  for (size_t i = 0; i < 100; ++i) {
    fv.field = "FIELD_" + std::to_string(i);
    fv.val   = "VALUE_" + std::to_string(i);
    fvs.push_back(fv);
  }

  std::vector<std::thread> jobs;
  auto start = system_clock::now();
  for (size_t i = 0; i < THREADNUM; ++i) {
    jobs.emplace_back([&db](size_t index, std::vector<nemo::FV> fvs) {
      for (size_t j = 0; j < TEN_THOUSAND ; ++j) {
        int32_t ret;
        std::string cur_key = "KEYS_HSET_" + std::to_string(index * TEN_THOUSAND + j);
        for (const auto& fv : fvs) {
          db->HSet(cur_key, fv.field, fv.val);
        }
      }
    }, i, fvs);
  }
  for (auto& job : jobs) {
    job.join();
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<std::chrono::seconds>(elapsed_seconds).count();
  std::cout << "Test HSet " << THREADNUM * TEN_THOUSAND << " Hashes Table Cost: "
    << cost << "s QPS: " << (THREADNUM * TEN_THOUSAND) / cost << std::endl;
  delete db;
}

void BenchHMSet() {
  printf("====== HMSet ======\n");
  nemo::Options options;
  options.create_if_missing = true;
  nemo::Nemo* db = new nemo::Nemo("./db", options);

  if (!db) {
    printf("Open db failed\n");
    return;
  }

  nemo::FV fv;
  std::vector<nemo::FV> fvs;

  for (size_t i = 0; i < 100; ++i) {
    fv.field = "FIELD_" + std::to_string(i);
    fv.val   = "VALUE_" + std::to_string(i);
    fvs.push_back(fv);
  }

  std::vector<std::thread> jobs;
  auto start = system_clock::now();
  for (size_t i = 0; i < THREADNUM; ++i) {
    jobs.emplace_back([&db](size_t index, std::vector<nemo::FV> fvs) {
      for (size_t j = 0; j < TEN_THOUSAND ; ++j) {
        db->HMSet("KEYS_HMSET_" + std::to_string(index * TEN_THOUSAND + j), fvs);
      }
    }, i, fvs);
  }
  for (auto& job : jobs) {
    job.join();
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<std::chrono::seconds>(elapsed_seconds).count();
  std::cout << "Test HMSet " << THREADNUM * TEN_THOUSAND << " Hashes Table Cost: "
    << cost << "s QPS: " << (THREADNUM * TEN_THOUSAND) / cost << std::endl;
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
  std::cout << "Test HDel " << fvs.size()
    << " Hash Field Cost: "<< cost << "ms" << std::endl;
  delete db;
}

void BenchHKeys() {
  printf("====== HKeys ======\n");
  nemo::Options options;
  options.create_if_missing = true;
  nemo::Nemo* db = new nemo::Nemo("./db", options);

  if (!db) {
    printf("Open db failed\n");
    return;
  }

  nemo::FV fv;
  std::vector<std::string> field;
  std::vector<nemo::FV> fvs;

  std::string field_prefix = "FIELD_";
  std::string value_prefix = "VALUE_";
  for (size_t i = 0; i < TEN_MILLION; ++i) {
    GenerateRandomString(field_prefix, FIELD_SIZE, &fv.field);
    GenerateRandomString(value_prefix, VALUE_SIZE, &fv.val);
    fvs.push_back(fv);
  }

  db->HMSet("HDEL_KEY", fvs);
  int64_t count;
  db->Del("HDEL_KEY", &count);

  fvs.resize(TEN_THOUSAND);
  db->HMSet("HDEL_KEY", fvs);

  auto start = system_clock::now();
  db->HKeys("HDEL_KEY", field);
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<std::chrono::milliseconds>(elapsed_seconds).count();
  std::cout << "Test HKeys " << field.size() << " Field Hash Table Cost: " << cost
    << "ms" << std::endl;
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
  for (size_t i = 0; i < TEN_THOUSAND; ++i) {
    fv.field = "FIELD_" + std::to_string(i);
    fv.val   = "VALUE_" + std::to_string(i);
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

  std::vector<std::string> members_in;
  for (int i = 0; i < 100; i++) {
    members_in.push_back("MEMBER_" + std::to_string(i));
  }

  std::vector<std::thread> jobs;
  auto start = system_clock::now();
  for (size_t i = 0; i < THREADNUM; ++i) {
    jobs.emplace_back([&db](size_t index, std::vector<std::string> members_in) {
      for (size_t j = 0; j < TEN_THOUSAND ; ++j) {
        int64_t ret;
        std::string cur_key = "SADD_KEY" + std::to_string(index * TEN_THOUSAND + j);
        for (const auto& member : members_in) {
          db->SAdd(cur_key, member, &ret);
        }
      }
    }, i, members_in);
  }
  for (auto& job : jobs) {
    job.join();
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<std::chrono::seconds>(elapsed_seconds).count();
  std::cout << "Test SAdd " << (THREADNUM * TEN_THOUSAND) << " Sets Cost: " << cost
    << "s QPS:" << (THREADNUM * TEN_THOUSAND) / cost << std::endl;

  delete db;
}

void BenchSRem() {
  printf("====== SRem ======\n");
  nemo::Options options;
  options.create_if_missing = true;
  nemo::Nemo* db = new nemo::Nemo("./db", options);

  if (!db) {
    printf("Open db failed\n");
    return;
  }

  int64_t ret;
  std::string set_key, member;
  std::vector<std::string> keys;
  std::vector<std::vector<std::string>> sets(TEN_THOUSAND * THREADNUM);
  for (int i = 0; i < TEN_THOUSAND * THREADNUM; i++) {
    GenerateRandomString(KEY_PREFIX, KEY_SIZE, &set_key);
    keys.push_back(set_key);
    for (int j = 0; j < ONE_HUNDRED; j++) {
      GenerateRandomString(MEMBER_PREFIX, MEMBER_SIZE, &member);
      sets[i].push_back(member);
      db->SAdd(set_key, member, &ret);
    }
  }

  std::vector<std::thread> jobs;
  auto start = system_clock::now();
  for (size_t i = 0; i < THREADNUM; ++i) {
    jobs.emplace_back([&db](size_t index,
                            std::vector<std::string> keys,
                            std::vector<std::vector<std::string>> sets) {
      int64_t ret;
      for (int j = 0; j < TEN_THOUSAND; j++) {
        std::string key = keys[index * TEN_THOUSAND + j];
        for (const auto& member : sets[index * TEN_THOUSAND + j]) {
          db->SRem(key, member, &ret);
        }
      }
    }, i, keys, sets);
  }
  for (auto& job : jobs) {
    job.join();
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<std::chrono::seconds>(elapsed_seconds).count();
  std::cout << "Test SRem " << (THREADNUM * TEN_THOUSAND) << " Sets Cost: " << cost
    << "s QPS:" << (THREADNUM * TEN_THOUSAND) / cost << std::endl;

  delete db;
}

void BenchSMove() {
  printf("====== SMove ======\n");
  nemo::Options options;
  options.create_if_missing = true;
  nemo::Nemo* db = new nemo::Nemo("./db", options);

  if (!db) {
    printf("Open db failed\n");
    return;
  }

  int64_t ret;
  std::string set_key, member;
  std::vector<std::string> keys_source;
  std::vector<std::string> keys_destination;
  std::vector<std::vector<std::string>> sets(TEN_THOUSAND * THREADNUM);
  for (int i = 0; i < TEN_THOUSAND * THREADNUM; i++) {
    GenerateRandomString(KEY_PREFIX + "sou_", KEY_SIZE, &set_key);
    keys_source.push_back(set_key);
    for (int j = 0; j < ONE_HUNDRED; j++) {
      GenerateRandomString(MEMBER_PREFIX, MEMBER_SIZE, &member);
      sets[i].push_back(member);
      db->SAdd(set_key, member, &ret);
    }

    GenerateRandomString(KEY_PREFIX + "des_", KEY_SIZE, &set_key);
    keys_destination.push_back(set_key);
  }

  std::vector<std::thread> jobs;
  auto start = system_clock::now();
  for (size_t i = 0; i < THREADNUM; ++i) {
    jobs.emplace_back([&db](size_t index,
                            std::vector<std::string> keys_source,
                            std::vector<std::string> keys_destination,
                            std::vector<std::vector<std::string>> sets) {
      int64_t ret;
      for (int j = 0; j < TEN_THOUSAND; j++) {
        std::string source = keys_source[index * TEN_THOUSAND + j];
        std::string destination = keys_destination[index * TEN_THOUSAND + j];
        for (const auto& member : sets[index * TEN_THOUSAND + j]) {
          db->SMove(source, destination, member, &ret);
        }
      }
    }, i, keys_source, keys_destination, sets);
  }
  for (auto& job : jobs) {
    job.join();
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<std::chrono::seconds>(elapsed_seconds).count();
  std::cout << "Test SMove " << (THREADNUM * TEN_THOUSAND) << " Sets Cost: " << cost
    << "s QPS:" << (THREADNUM * TEN_THOUSAND) / cost << std::endl;

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
  std::cout << "Test Case 1, SMembers " << members_out.size() << " Cost: " << cost << "ms" << std::endl;


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
  std::cout << "Test Case 2, SMembers " << members_out.size() << " Cost: " << cost << "ms" << std::endl;
  delete db;
}

void BenchLRange() {
	printf("====== LRange ======\n");
	nemo::Options options;
	options.create_if_missing = true;
	nemo::Nemo* db = new nemo::Nemo("./db", options);

	if (!db) {
		printf("Open db failed\n");
		return;
	}
	std::vector<std::string> values;
	for (int i = 0; i < 10 * TEN_MILLION; i++) {
		std::string value;
		GenerateRandomString(VALUE_PREFIX, KEY_SIZE, &value);
		values.push_back(value);
	}
	int64_t num;
	for (auto v : values) {
		db->RPush("BENCHMARK_LRANGE", v, &num);
	}
	std::vector<nemo::IV> result;

	// 100000
	auto start = system_clock::now();
	db->LRange("BENCHMARK_LRANGE", 0, ONE_HUNDRED_THOUSAND, result);
	auto end = system_clock::now();
	duration<double> elapsed_seconds = end - start;
	auto cost = duration_cast<std::chrono::seconds>(elapsed_seconds).count();
	std::cout << "Test LRange " << ONE_HUNDRED_THOUSAND << " KV Cost: " << cost << "s QPS: "
		<< ONE_HUNDRED_THOUSAND / cost << std::endl;

	// 1000000
	start = system_clock::now();
	db->LRange("BENCHMARK_LRANGE", 0, ONE_MILLION, result);
	end = system_clock::now();
	elapsed_seconds = end - start;
	cost = duration_cast<std::chrono::seconds>(elapsed_seconds).count();
	std::cout << "Test LRange " << ONE_MILLION << " KV Cost: " << cost << "s QPS: "
		<< ONE_MILLION / cost << std::endl;

	// 10000000
	start = system_clock::now();
	db->LRange("BENCHMARK_LRANGE", 0, TEN_MILLION, result);
	end = system_clock::now();
	elapsed_seconds = end - start;
	cost = duration_cast<std::chrono::seconds>(elapsed_seconds).count();
	std::cout << "Test LRange " << TEN_MILLION << " KV Cost: " << cost << "s QPS: "
		<< TEN_MILLION / cost << std::endl;

	// 10 * 10000000
	start = system_clock::now();
	db->LRange("BENCHMARK_LRANGE", 0, 10 * TEN_MILLION, result);
	end = system_clock::now();
	elapsed_seconds = end - start;
	cost = duration_cast<std::chrono::seconds>(elapsed_seconds).count();
	std::cout << "Test LRange " << 10 * TEN_MILLION << " KV Cost: " << cost << "s QPS: "
		<< 10 * TEN_MILLION / cost << std::endl;
}

int main() {
  // keys
  //BenchSet();
  //BenchMultiThreadSet();
  //BenchScan();
  BenchKeys();

  // hashes
  //BenchHSet();
  //BenchHMSet();
  //BenchHDel();
  //BenchHKeys();
  //BenchHGetall();

  // sets
  //BenchSAdd();
  //BenchSRem();
  //BenchSMove();
  //BenchSPop();
  //BenchSMembers();
}
