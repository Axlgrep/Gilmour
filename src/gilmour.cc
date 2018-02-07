//  Copyright (c) 2017-present The gilmour Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "gilmour/gilmour.h"

#include "leveldb/db.h"

int main() {
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;

  std::string path = "./db";
  leveldb::Status s = leveldb::DB::Open(options, path, &db);

  std::string key = "key";
  std::string value = "value";
  s = db->Put(leveldb::WriteOptions(), key, value);
  assert(s.ok());

  value.clear();
  s = db->Get(leveldb::ReadOptions(), key, &value);
  assert(s.ok());
  std::cout << key << " : " << value << std::endl;

  s = db->Delete(leveldb::WriteOptions(), key);
  assert(s.ok());

  delete db;
  return 0;
}
