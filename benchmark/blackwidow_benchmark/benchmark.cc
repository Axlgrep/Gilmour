//  Copyright (c) 2017-present The gilmour Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
#include <vector>
#include <thread>
#include <functional>

#include "blackwidow/blackwidow.h"

using namespace blackwidow;
using namespace std::chrono;

int main() {
  blackwidow::Options options;
  blackwidow::BlackWidow db;
  blackwidow::Status s;

  options.create_if_missing = true;
  s = db.Open(options, "./db");
}
