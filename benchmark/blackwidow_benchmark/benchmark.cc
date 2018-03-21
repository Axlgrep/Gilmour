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
// 测试场景 : 单线程情况下向存储引擎写入10000000个键值对
// 测试结果 : Blackwidow的性能与Nemo相差不大
//
// 原因分析 : 可能是Blackwidow用了新版本的Rocksdb, 而新版
// 本Rocksdb性能有提升导致的
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
// 测试场景 : 开20个线程向存储引擎中写入总量为20000000的键值对 
// 测试结果 : Blackwidow的性能明显高于Nemo
//
// 原因分析 : 可能是Blackwidow用了新版本的Rocksdb, 而新版本Rocksdb性
// 能有提升导致的
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
// 测试场景 : 先向引擎中写入10000个大小为10000的Hash Table, 然后Scan
// 数据库中符合模式匹配的所有Key.
// 测试结果 : Blackwidow的性能明显高于Nemo
//
// 原因分析 : 由于Blackwidow在设计之初使用了Rocksdb 的Column
// Families新特性, 我们将元数据和实际数据进行分组存放(将所有的
// Key放在一个组里, 将Sets里面的Members和Hashes里面的Field和
// Value等等数据放在另外一个组里面, 这样存储元数据组里的数据是
// 少量的, 存储实际数据对应的组的数据通常是大量的), 当我们需要
// 筛选出我们需要的Key的时候我们只需要在元数据组中进行查找(元数
// 据组里的数据少, 查找起来非常的快速)
// 而Nemo将元数据和实际数据混在一起存放, 这样当我们在大量的数据
// 中查找我们所需要的Key速度是非常慢的.
//
// 补充说明 : 上面的测试结果是在数据库中存有少量数据的情况下测试的,
// 如果DB中数据量非常大的情况下, Blackwidow的优势会更加明显.
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

// Blackwidow : Test HSet 200000 Hashes Table Cost: 94s  QPS: 2127
// Nemo       : Test HSet 200000 Hashes Table Cost: 129s QPS: 1550
// 测试场景 :  开20个线程调用HSet向引擎中写入总量为200000大小为100
// 的Hash Table.
// 测试结果 :  Blackwidow的性能明显高于Nemo
//
// 原因分析 : Blackwidow中使用了Slice, 减少了没必要的String对象的创建
// 并且一次性将当前Sets的Field, Verion以及TTL信息写入到了数据Key当中,
// 而Nemo的HSet方法中中存在一些返回String对象的方法, 这样会构造String
// 对象, 然后先是通过Nemo用KV模拟多数据结构,  然后再调用nemo-rocksdb追
// 加Version以及TTL等信息, 这样有可能会导致重新分配内存, 并且会拷贝数据
// 造成性能开销.
void BenchHSet() {
  printf("====== HSet ======\n");
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
        int32_t ret;
        std::string cur_key = "KEYS_HSET_" + std::to_string(index * TEN_THOUSAND + j);
        for (const auto& fv : fvs) {
          db.HSet(cur_key, fv.field, fv.value, &ret);
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
}

// Blackwidow : Test HMSet 200000 Hashes Table Cost: 10s  QPS: 20000
// Nemo       : Test HMSet 200000 Hashes Table Cost: 126s QPS: 1587
// 测试场景 :  开20个线程向调用HMSet向引擎中写入总量为200000大小为100
// 的Hash Table.
// 测试结果 :  Blackwidow的性能明显高于Nemo
//
// 测试分析 : Nemo中HMSet的做法是遍历当前需要添加的所有FieldValue, 然
// 后逐一调用HSet方法, 这样的话, 由于每次调用都要对当前Hash表的Key加
// 锁解锁, 所以速度非常慢, 并且没法保证整个HMSet操作的原子性.
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


// Blackwidow : Test HDel 10000000 Hash Field Cost: 158895ms
// Nemo       : Test HDel 10000000 Hash Field Cost: 197500ms
// 测试场景 :  先向引擎中写入一个大小为10000000的Hash Table
// 然后逐一删除这个Hash表中的10000000个Field
// 测试结果 : Blackwidow的性能略微高于Nemo
//
// 原因分析 :  由于Nemo中EncodeHashKey的做法是将数据逐一
// Append到string后面, 这样可能会导致string重新分配内存然后
// 重新拷贝已有数据, 再加上EncodeHashKey直接返回的是String
// 对象，这样会多走一次构造函数引起性能消耗.
// 而BlackWidow会提前计算出HashKey所需的空间, 一次性进行分配
// 所需空间, 没有重新分配内存拷贝数据的情况存在, 并且在
// Blackwidow当中广泛采用Slice, 这样只传递String中数据指针以
// 及大小的方法避免了重新构造String类型对象, 也是BlackWidow
// 性能提升的原因之一
//
// 补充说明 : Nemo中的HDel方法只支持一次删除一个Hash表中的Field,
// 而BlackWidow兼容了最新的Redis版本, 一次可以删除Hash表中的多
// 个Field, 并且这个操作就是作为Rocksdb的一个WriteBatch进行操作,
// 速度十分快, 所以在一次需要删除一个Hash Table中的多个Field的场
// 景下Blackwidow相对于Nemo的优势将更加的明显
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
// 测试场景 : 创建一个大小为100000的Hash表, 然后新旧引擎进行HGetall对比
// 测试.
// 测试结果 : Blackwidow的性能略微高于Nemo
//
// 结果分析 : 要稍微快一点的原因是由于采用了Slice以及一次性分配HashKey
// 所需要的空间, 减少了重新分配空间以及String构造函数引起的性能消耗
//
// Case 2
// Blackwidow : Test case 2, HGetall 10000 Field HashTable Cost: 5ms
// Nemo       : Test case 2, HGetall 10000 Field HashTable Cost: 4899ms
// 测试场景 : 创建一个大的Hash表, 然后删除该Hash表, 再创建一个同名Hash表,
// 这时候再用新旧引擎进行HGetall对比测试.
// 测试结果 : Blackwidow性能要明显高于Nemo.
//
// 结果分析 : 由于Blackwidow在设计的时候将Version放到了HashKey当中, 由于新
// 旧Hash表的version不一样, 所以在没有Compaction之前新旧Hash Table的数据在
// RocksDB中存储的位置也不一样, 我们可以快速的Seek到新的Hash Table的数据块
// 然后取出来.
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

// Blackwidow : Test SAdd 200000 Sets Cost: 95s  QPS:2105
// Nemo       : Test SAdd 200000 Sets Cost: 123s QPS:1626
// 测试场景 : 开20个线程向引擎中写入总量为200000的大小为1000的Set集合
// 测试结果 : Blackwidow的性能略微高于Nemo
//
// 结果分析 : Nemo内部EncodeSetKey是向String后面Append内容, 这样可能
// 会导致重新分配内存, 并且该方法返回的是String对象, 会额外调用一次
// String的构造函数造成性能消耗.
// BlackWidow内部一次性分配我们需要拼接Key的空间, 并且返回的是数据内容
// 的指针, 而不是重新构造这个String对象, 所以性能明显提升.
// 补充说明 : Blackwidow的SAdd接口兼容最新版本的Redis, 一次可以向集合中
// 添加多个Member, 并且添加的多个Member在Blackwidow中是做一次Bacth的操
// 作, 所以在一次需要向同一个集合中添加多个Member这种情景下Blackwidow会
// 明显优于Nemo.
//
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

  std::vector<std::string> members_in;
  for (int i = 0; i < 100; i++) {
    members_in.push_back("MEMBER_" + std::to_string(i));
  }

  std::vector<std::thread> jobs;
  auto start = system_clock::now();
  for (size_t i = 0; i < THREADNUM; ++i) {
    jobs.emplace_back([&db](size_t index, std::vector<std::string> members_in) {
      for (size_t j = 0; j < TEN_THOUSAND ; ++j) {
        int32_t ret;
        std::string cur_key = "SADD_KEY" + std::to_string(index * TEN_THOUSAND + j);
        for (const auto& member : members_in) {
          db.SAdd(cur_key, {member}, &ret);
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
// Blackwidow : Test Case 1, SMembers 100000 Cost: 38ms
// Nemo       : Test Case 1, SMembers 100000 Cost: 56ms
// 测试场景 : 创建一个大小为100000的Set集合, 然后新旧引擎进行Smembers
// 对比测试.
// 测试结果 : Blackwidow的性能略微高于Nemo
//
// 原因分析 : Blackwidow要稍微快一点是由于新版Blackwidow在设计之初将
// Version放在Key中进行存放, 这样保证当前数据结构的所有数据在Rocksdb
// 中都是存放在一起的, 这样查找起来比较快速
// Nemo将元数据以及真实数据放在一起存放, 并且由于设计原因会导致不同
// 的Sets中的Members数据在Rocksdb中混在一起存放, 这样查找起来效率
// 不高
//
// Case 2
// Blackwidow : Test Case 2, SMembers 200000 Cost: 36ms
// Nemo       : Test Case 2, SMembers 200000 Cost: 4373ms
// 测试场景 : 创建一个大的Hash表, 然后删除该Hash表, 再创建一个同名Hash
// 表, 这时候再用新旧引擎进行HGetall对比测试
// 测试结果 : Blackwidow的性能明显高于Nemo
//
// 原因分析 : 由于Blackwidow在设计的时候将Version放到了SetsMemberKey
// 当中, 由于新旧Set集合的version不一样, 所以新旧集合的SetsMemberKey
// 在Rocksdb中也会分开存放, 我们可以快速的Seek到新的Set的数据块然后取
// 出来
// Nemo中由于没有将Version提前放置, 所以在RocksDB没有做Compaction之前,
// 新旧Sets中的数据混在一起存放, 这时候要从一大堆的数据中筛选出我们想要
// 的数据, 这个操作是很慢的.
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
  BenchHSet();
  //BenchHMSet();
  //BenchHDel();
  //BenchHGetall();

  // sets
  //BenchSAdd();
  //BenchSPop();
  //BenchSMembers();
}
