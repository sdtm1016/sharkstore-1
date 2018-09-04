// Copyright (c) 2018 The SharkStore Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

_Pragma("once");

#include <mutex>
#include "histogram.h"

namespace sharkstore {
namespace monitor {

enum class HistogramType : uint32_t {
    kQWait = 0,
    kDeal,
    kStore,
    kRaft,
    kMax,
};

const char *HistogramTypeName(HistogramType type);

static constexpr uint32_t kHistogramTypeNum = static_cast<uint32_t>(HistogramType::kMax);

class Statistics {
public:
    void PushTime(HistogramType type, uint64_t time);

    void GetData(HistogramType type, HistogramData *data);

    std::string ToString(HistogramType type) const;
    std::string ToString() const;

    void Reset();

private:
    Histogram histograms_[kHistogramTypeNum];
    mutable std::mutex aggregate_lock_;
};

}  // namespace monitor
}  // namespace sharkstore
