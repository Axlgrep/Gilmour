//  Copyright (c) 2017-present The gilmour Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
#include <vector>
#include <thread>
#include <functional>

#include "blackwidow/blackwidow.h"

const int THREADNUM = 20;
const int ONE_THOUSAND = 1000;
const int TEN_THOUSAND = 10000;
const int ONE_HUNDRED_THOUSAND = 100000;
const int ONE_MILLION = 1000000;
const int TEN_MILLION = 10000000;

using namespace blackwidow;
using namespace std::chrono;

// Blackwidow : Test Set 10000000 Cost: 41s QPS: 250000
// Nemo       : Test Set 10000000 Cost: 42s QPS: 238095
// 单线程情况下Set()接口BlackWidow相对于Nemo有略微提升,
// 可能是Blackwidow用了新版本的Rocksdb, 而新版本Rocksdb性能有提升导致的
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

  auto start = system_clock::now();
  for (uint32_t i = 0; i < TEN_MILLION; ++i) {
    db.Set(keys[i], values[i]);
  }

  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<std::chrono::seconds>(elapsed_seconds).count();
  std::cout << "Test Set " << TEN_MILLION << " Cost: " << cost << "s QPS: "
    << TEN_MILLION / cost << std::endl;
}

// Blackwidow : Test MultiThread Set 20000000 Cost: 44s QPS: 454545
// Nemo       : Test MultiThread Set 20000000 Cost: 66s QPS: 303030
// 单线程情况下Set()接口BlackWidow相对于Nemo有较大的提升,
// 可能是Blackwidow用了新版本的Rocksdb, 而新版本Rocksdb性能有提升导致的
void BenchMultiThreadSet() {
  printf("====== Multi Thread Set ======\n");
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
  for (int i = 0; i < ONE_MILLION; i++) {
    keys.push_back("KEY_" + std::to_string(i));
    values.push_back("VALUE_" + std::to_string(i));
  }

  std::vector<std::thread> jobs;
  auto start = system_clock::now();
  for (size_t i = 0; i < THREADNUM; ++i) {
    jobs.emplace_back([&db](std::vector<std::string> keys, std::vector<std::string> values) {
      for (size_t j = 0; j < ONE_MILLION; ++j) {
        db.Set(keys[j], values[j]);
      }
    }, keys, values);
  }
  for (auto& job : jobs) {
    job.join();
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<std::chrono::seconds>(elapsed_seconds).count();
  std::cout << "Test MultiThread Set " << THREADNUM * ONE_MILLION << " Cost: "
    << cost << "s QPS: " << (THREADNUM * ONE_MILLION) / cost << std::endl;
}

// Blackwidow : Test Scan 10000 Keys Cost: 27ms
// Nemo       : Test Scan 10000 Keys Cost: 84ms
// 由于Blackwidow在设计之初利用了Rocksdb Column Families新特性,
// 我们将元数据和实际数据进行分组存放(将所有的Key放在一个组里,
// 将Sets里面的Members和Hashes里面的Field和Value等等数据放在另外
// 一个组里面, 这样存储元数据组里的数据是少量的, 存储实际数据对
// 应的组的数据通常是大量的), 当我们需要筛选出我们需要的Key的时
// 候我们只需要在元数据组中进行查找(元数据组里的数据少, 查找起来
// 非常的快速)
// 而Nemo将元数据和实际数据混在一起存放, 这样当我们在大量的数据
// 中查找我们所需要的Key速度是非常慢的.
// 上面的测试结果是在数据库中存有少量数据的情况下测试的, 如果DB中
// 数据量非常大的情况下, 差距会更加明显
void BenchScan() {
  printf("====== Scan ======\n");
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
  std::vector<BlackWidow::FieldValue> fvs;

  for (size_t i = 0; i < TEN_THOUSAND; ++i) {
    fv.field = "FIELD_" + std::to_string(i);
    fv.value = "VALUE_" + std::to_string(i);
    fvs.push_back(fv);
  }
  for (size_t i = 0; i < TEN_THOUSAND; ++i) {
    db.HMSet("SCAN_KEY" + std::to_string(i), fvs);
  }

  int64_t total = 0;
  int64_t cursor_origin, cursor_ret = 0;
  std::vector<std::string> keys;
  auto start = system_clock::now();
  for (; ;) {
    total += keys.size();
    keys.clear();
    cursor_origin = cursor_ret;
    cursor_ret = db.Scan(cursor_ret, "SCAN_KEY*", 10, &keys);
    if (cursor_ret == 0) {
      break;
    }
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test Scan " << total
    << " Keys Cost: "<< cost << "ms" << std::endl;
}

void BenchKeys() {
  printf("====== Scan Keys ======\n");
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
  std::vector<std::string> members_in;
  std::vector<BlackWidow::FieldValue> fvs;
  for (size_t i = 0; i < TEN_THOUSAND; ++i) {
    fv.field = "FIELD_" + std::to_string(i);
    fv.value = "VALUE_" + std::to_string(i);
    fvs.push_back(fv);
    members_in.push_back("MEMBER_" + std::to_string(i));
  }

  for (size_t i = 0; i < 5000; ++i) {
    db.HMSet("KEYS_HASHES_KEY_" + std::to_string(i), fvs);
    db.SAdd("KEYS_SETS_KEY_" + std::to_string(i), members_in, &ret);
    db.Set("KEYS_STRING_KEY_" + std::to_string(i), "VALUE");
    printf("index = %d\n", i);
  }

  int64_t total = 0;
  int64_t cursor_origin, cursor_ret = 0;
  std::vector<std::string> keys;
  auto start = system_clock::now();
  for (; ;) {
    total += keys.size();
    keys.clear();
    cursor_origin = cursor_ret;
    cursor_ret = db.Scan(cursor_ret, "*", 10, &keys);
    if (cursor_ret == 0) {
      break;
    }
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test Keys " << total
    << " Keys Cost: "<< cost << "ms" << std::endl;
}

// Blackwidow : Test HMSet 200000 Hashes Table Cost: 10s QPS: 20000
// Nemo       : Test HMSet 200000 Hashes Table Cost: 126s QPS: 1587
// Nemo中HMSet的做法是遍历当前需要添加的所有FieldValue, 然后逐一调用
// HSet方法, 这样的话, 由于每次调用都要对当前Hash表的Key加锁解锁, 所
// 以速度非常慢, 并且没法保证整个HMSet操作的原子性.
// BlackWidow中首先对当前Hash表的Key进行上锁, 然后将修改Meta信息以及
// 添加FieldValue的操作都放到RockDB中的一个WriteBatch中去完成, 既保证
// 了整个HMSet操作的原子性, 速度对比与Nemo也有很大的提升.
void BenchHMSet() {
  printf("====== HMSet ======\n");
  blackwidow::Options options;
  options.create_if_missing = true;
  blackwidow::BlackWidow db;
  blackwidow::Status s = db.Open(options, "./db");

  if (!s.ok()) {
    printf("Open db failed, error: %s\n", s.ToString().c_str());
    return;
  }

  BlackWidow::FieldValue fv;
  std::vector<BlackWidow::FieldValue> fvs;
  for (size_t i = 0; i < 100; ++i) {
    fv.field = "FIELD_" + std::to_string(i);
    fv.value = "VALUE_" + std::to_string(i);
    fvs.push_back(fv);
  }

  std::vector<std::thread> jobs;
  auto start = system_clock::now();
  for (size_t i = 0; i < THREADNUM; ++i) {
    jobs.emplace_back([&db](size_t index, std::vector<BlackWidow::FieldValue> fvs) {
      for (size_t j = 0; j < TEN_THOUSAND ; ++j) {
        db.HMSet("KEYS_HMSET_" + std::to_string(index * TEN_THOUSAND + j), fvs);
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
}


// Blackwidow performance more stronger
// bw 156507ms | nm 198454ms
//
// 单个删除Hash表中的Field测试中, Blackwidow比nemo性能要快，
// 由于Nemo中EncodeHashKey的做法是将数据逐一Append到string
// 后面, 这样可能会导致string扩容然后重新拷贝已有数据, 再加
// EncodeHashKey直接返回的是String对象，这样会多走一次构造
// 函数引起性能消耗.
// 而BlackWidow会提前计算出HashKey所需的空间, 一次性进行分配
// 所需空间, 没有扩容重新拷贝以后数据的情况存在, 并且在
// Blackwidow当中广泛采用Slice, 这样只传递String中数据指针以
// 及大小的方法避免了重新构造String类型对象, 也是BlackWidow
// 性能提升的原因之一
// 另外Nemo中的HDel方法只支持一次删除一个Hash表中的Field, 而
// BlackWidow兼容了最新的Redis版本, 一次可以删除Hash表中的多
// 个Field, 这样一次删除多个Field肯定比Nemo中一次删除一个Field
// 要快很多
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
  for (const auto& field : fields) {
    db.HDel("HDEL_KEY", {field}, &ret);
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test HDel " << fvs.size()
    << " Hash Field Cost: "<< cost << "ms" << std::endl;
}

// Case 1
// Blackwidow : Test case 1, HGetall 100000 Field HashTable Cost: 30ms
// Nemo       : Test case 1, HGetall 100000 Field HashTable Cost: 57ms
// 创建一个大小为100000的Hash表, 然后新旧引擎进行对比测试, Blackwidow
// 要稍微快一点的原因是由于采用了Slice以及一次性分配HashKey所需要的空
// 间, 减少了重新分配空间以及String构造函数引起的性能消耗
//
// Case 2
// Blackwidow : Test case 2, HGetall 10000 Field HashTable Cost: 5ms
// Nemo       : Test case 2, HGetall 10000 Field HashTable Cost: 4899ms
// 创建一个大的Hash表, 然后删除该Hash表, 再创建一个同名Hash表, 这时候再
// 用新旧引擎进行HGetall对比测试, Blackwidow性能要明显高于Nemo.
// 由于Blackwidow在设计的时候将Version放到了HashKey当中, 由于新旧Hash表的
// version不一样, 所以在RocksDB中存储的位置也不一样, 我们可以快速的Seek
// 到新的Hash的数据块然后取出来
// Nemo中由于没有将Version提前放置, 所以在RocksDB没有做Compaction之前, 新
// 旧Hash表中的数据混在一起存放, 这时候要从一大堆的数据中筛选出我们想要
// 的数据, 这个操作是很慢的.
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

  // 1. Create the hash table then insert hash table 100000 field
  // 2. HGetall the hash table 100000 field (statistics cost time)
  fvs_in.clear();
  for (size_t i = 0; i < ONE_HUNDRED_THOUSAND; ++i) {
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
  //    then insert the hash table 100000 field
  // 4. HGetall the hash table 100000 field (statistics cost time)
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
  for (size_t i = 0; i < TEN_THOUSAND; ++i) {
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
}

// Blackwidow performance more stronger
// bw 388640ms | nm 165896ms
//
// 因为blackwidow内的SAdd可以支持一次添加多个member, 而nemo
// 一次SAdd操作只能想集合中添加一个member，期间每次操作都要
// 上锁解锁, 所以比较慢
void BenchSAdd() {
  printf("====== SAdd ======\n");
  blackwidow::Options options;
  options.create_if_missing = true;
  blackwidow::BlackWidow db;
  blackwidow::Status s = db.Open(options, "./db");

  if (!s.ok()) {
    printf("Open db failed, error: %s\n", s.ToString().c_str());
    return;
  }

  int32_t ret;
  std::vector<std::string> members_in;
  for (int i = 0; i < TEN_MILLION; i++) {
    members_in.push_back("MEMBER_" + std::to_string(i));
  }

  auto start = system_clock::now();
  for (const auto& member : members_in) {
    db.SAdd("SADD_KEY", {member}, &ret);
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test SAdd " << TEN_MILLION << " Cost: " << cost << "ms" << std::endl;
}

void BenchSPop() {
  printf("====== SPop ======\n");
  blackwidow::Options options;
  options.create_if_missing = true;
  blackwidow::BlackWidow db;
  blackwidow::Status s = db.Open(options, "./db");

  if (!s.ok()) {
    printf("Open db failed, error: %s\n", s.ToString().c_str());
    return;
  }

  int32_t ret;
  std::vector<std::string> members_in;
  std::vector<std::string> members_out;
  for (int i = 0; i < TEN_MILLION; i++) {
    members_in.push_back("MEMBER_" + std::to_string(i));
  }
  db.SAdd("SPOP_KEY", members_in, &ret);

  auto start = system_clock::now();
  for (uint32_t i = 0; i < ONE_HUNDRED_THOUSAND; ++i) {
    db.SPop("SPOP_KEY", i, &members_out);
  }
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test SPop " << TEN_THOUSAND << " Cost: " << cost << "ms" << std::endl;
}

// Case 1
// Performance is equal
// bw 36ms | nm 56ms
//
// Case 2
// Blackwidow performance more stronger
// bw 18ms | nm 4338ms
// Set中也将version提前放置了, 所以速度有明显的提升,
// 原理和HGetall中的是一样的
void BenchSMembers() {
  printf("====== SMembers ======\n");
  blackwidow::Options options;
  options.create_if_missing = true;
  blackwidow::BlackWidow db;
  blackwidow::Status s = db.Open(options, "./db");

  if (!s.ok()) {
    printf("Open db failed, error: %s\n", s.ToString().c_str());
    return;
  }

  // Test Case 1
  int32_t ret;
  std::vector<std::string> members_in;
  std::vector<std::string> members_out;
  for (int i = 0; i < ONE_HUNDRED_THOUSAND; i++) {
    members_in.push_back("MEMBER_" + std::to_string(i));
  }
  db.SAdd("SMEMBERS_KEY1", members_in, &ret);

  auto start = system_clock::now();
  db.SMembers("SMEMBERS_KEY1", &members_out);
  auto end = system_clock::now();
  duration<double> elapsed_seconds = end - start;
  auto cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test Case 1, SMembers " << members_out.size() << " Cost: " << cost << "ms" << std::endl;


  // Test Case 2
  members_in.clear();
  for (size_t i = 0; i < TEN_MILLION; i++) {
    members_in.push_back("MEMBER_" + std::to_string(i));
  }
  db.SAdd("SMEMBERS_KEY2", members_in, &ret);
  std::vector<Slice> del_keys({"SMEMBERS_KEY2"});
  std::map<BlackWidow::DataType, Status> type_status;
  db.Del(del_keys, &type_status);
  members_in.clear();
  for (size_t i = 0; i < ONE_HUNDRED_THOUSAND; ++i) {
    members_in.push_back("MEMBER_" + std::to_string(i));
  }
  db.SAdd("SMEMBERS_KEY2", members_in, &ret);

  start = system_clock::now();
  db.SMembers("SMEMBERS_KEY2", &members_out);
  end = system_clock::now();
  elapsed_seconds = end - start;
  cost = duration_cast<milliseconds>(elapsed_seconds).count();
  std::cout << "Test Case 2, SMembers " << members_out.size() << " Cost: " << cost << "ms" << std::endl;
}

int main() {
  // keys
  //BenchSet();
  //BenchMultiThreadSet();
  //BenchScan();
  //BenchKeys();

  // hashes
  //BenchHMSet();
  //BenchHDel();
  BenchHGetall();

  // sets
  //BenchSAdd();
  //BenchSPop();
  //BenchSMembers();
}
