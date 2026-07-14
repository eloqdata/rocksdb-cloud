/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program.  If not, see
 *    <http://www.gnu.org/licenses/>.
 *
 */

#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <utility>

#include "rocksdb/listener.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class CloudFileSystemImpl;
class CloudScheduler;

// Basename prefix of the guard object; the full basename is
// <prefix><epoch>. Exposed so the purger's marker selector matches listing
// entries with the same definition the key builder uses.
inline constexpr char kSmallestFileNumberFilePrefix[] =
    "smallest_new_file_number-";

// The single source of truth for the purger guard object's S3 key. Both the
// writer (FileNumberGuardPublisher) and the reader (S3FileNumberReader in
// eloq_purger.cc) build the key through this function; the format is part of
// the on-cloud protocol and must not change:
//   <object_path>/smallest_new_file_number-<epoch>
// The object's value is an ASCII uint64. Semantics: no in-flight job on the
// epoch's writer node can create an SST with a file number below this value.
// 0 means "purging blocked for this epoch"; UINT64_MAX means "nothing in
// flight".
std::string SmallestFileNumberObjectKey(const std::string &object_path,
                                        const std::string &epoch);

// Tracks the file-number snapshots of in-flight flush/compaction jobs, keyed
// by (thread_id, job_id). A completed job's entry lingers for entry_duration
// before it stops contributing to the minimum, giving the job's MANIFEST
// update time to reach the cloud. Pure in-memory bookkeeping: no locking
// (the owner locks) and no I/O, so it is unit-testable in isolation.
class FileNumberSlidingWindow {
 public:
  using Clock = std::chrono::steady_clock;

  explicit FileNumberSlidingWindow(std::chrono::milliseconds entry_duration)
      : entry_duration_(entry_duration) {}

  // Registers a job's begin snapshot. Re-adding the same (thread_id, job_id)
  // keeps the existing (earlier, hence smaller and safer) entry.
  void Add(uint64_t file_number, uint64_t thread_id, int job_id);

  // Marks a job complete. The entry keeps contributing to the minimum until
  // it has lingered for entry_duration past `now`.
  void MarkRemoved(uint64_t thread_id, int job_id,
                   Clock::time_point now = Clock::now());

  // Smallest file number among live and still-lingering entries; expired
  // entries are dropped as a side effect. UINT64_MAX when nothing remains.
  uint64_t SmallestFileNumber(Clock::time_point now = Clock::now());

  size_t Size() const { return entries_.size(); }

 private:
  struct Entry {
    uint64_t file_number;
    Clock::time_point removed_at;
    bool removed = false;
  };

  std::map<std::pair<uint64_t, int>, Entry> entries_;
  std::chrono::milliseconds entry_duration_;
};

// Publishes the smallest_new_file_number-<epoch> guard object for the epoch
// this node is writing. The epoch is always read from the cloud manifest at
// publish time, never injected externally.
//
// Locking design (load-bearing, see the purger safety argument):
//  - state_mutex_ guards the window and remote-publication state; critical
//    sections are O(microseconds) and never perform I/O.
//  - publish_mutex_ serializes every S3 PUT. Two concurrent PUTs could land
//    out of order and reinstate a dangerously high threshold; the publish
//    mutex plus the post-acquire staleness re-check make that impossible.
//  - ProtectFileUpload serializes a required downward PUT and the protected
//    SST upload callback. Stop() first marks the publisher stopped, then waits
//    on the same publish gate. Thus Stop wins before the callback or waits for
//    an upload that already crossed the stopped_ check. A successful PUT
//    records its value; an ambiguous failure resets the state to unknown/MAX.
//
// cfs_ is non-owning. CloudFileSystemImpl owns an installed publisher and
// synchronously stops it before destroying the manifest and storage-provider
// members reached by publisher callbacks.
class FileNumberGuardPublisher {
 public:
  FileNumberGuardPublisher(CloudFileSystemImpl *cfs,
                           std::chrono::milliseconds publish_interval,
                           std::chrono::milliseconds entry_duration);
  ~FileNumberGuardPublisher();

  FileNumberGuardPublisher(const FileNumberGuardPublisher &) = delete;
  FileNumberGuardPublisher &operator=(const FileNumberGuardPublisher &) =
      delete;

  // Schedules the periodic publish job. Idempotent.
  void Start();

  // Cancels the periodic job, unblocks any in-progress downward retry loop,
  // and waits for any protected SST upload already in progress. Idempotent;
  // called from the destructor.
  void Stop();

  // Registers a flush/compaction job in the live window. This bookkeeping-only
  // listener path performs no remote I/O.
  void OnJobBegin(uint64_t file_number, uint64_t thread_id, int job_id);

  // The job completed; its entry starts lingering.
  void OnJobEnd(uint64_t thread_id, int job_id);

  // Ensures the current live-window minimum is published at or below the SST
  // file number, then invokes upload while holding the serialized publish
  // gate. A required downward PUT retries until it succeeds or Stop()
  // interrupts it. Tests may omit upload to exercise protection alone.
  Status ProtectFileUpload(
      uint64_t file_number,
      const std::function<Status()> &upload = std::function<Status()>());

  // Publishes the 0 sentinel ("purging blocked") for the current epoch.
  // Does not advance last_published_. Protected SST uploads preserve the
  // known-safe sentinel; the next periodic pass replaces it with the current
  // live-window minimum. Called at DB open before recovery can flush, and by
  // the embedder around leader transfer.
  Status BlockPurger();

  // The periodic publish body: computes the window minimum (UINT64_MAX when
  // idle) and publishes it if it differs from the last published value or if
  // the remote value is a sentinel/unknown after an ambiguous PUT failure.
  // Public so tests can drive it without waiting for the timer.
  void PeriodicPublish();

  uint64_t TEST_LastPublished();

 private:
  // Serialized by publish_mutex_ (caller must hold it). Refuses to publish
  // when the epoch is empty.
  Status PutValue(uint64_t value, const std::string &epoch);

  // Current epoch from the cloud manifest; empty when unavailable.
  std::string CurrentEpoch() const;

  void MarkRemoteUnknown();

  CloudFileSystemImpl *cfs_;
  const std::chrono::milliseconds publish_interval_;

  std::mutex state_mutex_;
  FileNumberSlidingWindow window_;
  uint64_t last_published_ = std::numeric_limits<uint64_t>::max();
  // The remote object is known to contain the safe 0 sentinel. Protected SST
  // uploads preserve it until PeriodicPublish installs a real watermark.
  bool sentinel_active_ = false;
  // A PUT reported failure and may nevertheless have committed remotely.
  // No comparison against last_published_ is safe until a real PUT succeeds.
  bool remote_unknown_ = false;

  std::mutex publish_mutex_;

  std::mutex stop_mutex_;
  std::condition_variable stop_cv_;
  bool stopped_ = false;

  std::shared_ptr<CloudScheduler> scheduler_;
  long job_handle_ = -1;
};

// EventListener bridging RocksDB job events to the publisher. Registered
// automatically by DBCloudImpl::Open when
// CloudFileSystemOptions::publish_file_number_guard is set.
class FileNumberGuardListener : public EventListener {
 public:
  explicit FileNumberGuardListener(
      std::shared_ptr<FileNumberGuardPublisher> publisher)
      : publisher_(std::move(publisher)) {}

  const char *Name() const override { return "FileNumberGuardListener"; }

  void OnFlushBegin(DB *db, const FlushJobInfo &info) override;
  void OnFlushFinished(DB *db, const FlushJobEndInfo &info) override;
  void OnCompactionBegin(DB *db, const CompactionJobInfo &info) override;
  void OnCompactionCompleted(DB *db, const CompactionJobInfo &info) override;
  void OnExternalFileIngestionStarted(DB *db, uint64_t file_number) override;
  void OnExternalFileIngestionFinished(DB *db, uint64_t file_number) override;
  void OnTableFileCreationStarted(const TableFileCreationBriefInfo &) override;
  void OnTableFileCreated(const TableFileCreationInfo &info) override;

private:
  void JobBegin(uint64_t file_number, uint64_t thread_id, int job_id);

  static constexpr int kExternalFileJobId = -1;
  static constexpr int kRecoveryJobId = -2;

  struct RecoveryEntry {
    uint64_t file_number;
    uint64_t thread_id;
  };
  using RecoveryKey = std::tuple<std::string, std::string, int>;

  std::shared_ptr<FileNumberGuardPublisher> publisher_;
  std::mutex recovery_mutex_;
  std::map<RecoveryKey, RecoveryEntry> recovery_files_;
};

}  // namespace ROCKSDB_NAMESPACE
