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

#include "cloud/file_number_guard.h"

#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <sstream>

#include "cloud/cloud_manifest.h"
#include "cloud/cloud_scheduler.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/db.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

std::string SmallestFileNumberObjectKey(const std::string &object_path,
                                        const std::string &epoch) {
  std::ostringstream oss;
  oss << object_path;
  if (!object_path.empty() && object_path.back() != '/') {
    oss << "/";
  }
  oss << kSmallestFileNumberFilePrefix << epoch;
  return oss.str();
}

// One-shot PUT of the guard object. Shared by the publisher and by
// CloudFileSystemImpl::BlockPurger when no publisher is registered. Callers
// that care about PUT ordering must serialize calls themselves (the
// publisher's publish_mutex_ does this).
IOStatus PutSmallestFileNumberObject(CloudFileSystemImpl *cfs, uint64_t value,
                                     const std::string &epoch) {
  if (epoch.empty()) {
    // Publishing to "smallest_new_file_number-" (empty epoch) would create a
    // malformed guard object no reader ever consults. Should be impossible
    // now that the epoch is sourced from the cloud manifest, but keep the
    // guard.
    Log(InfoLogLevel::ERROR_LEVEL, cfs->info_log_,
        "[fng] Refusing to publish smallest file number: epoch is empty");
    return IOStatus::InvalidArgument("empty epoch for file number guard");
  }

  std::string content = std::to_string(value);
  std::string object_key =
      SmallestFileNumberObjectKey(cfs->GetDestObjectPath(), epoch);

  char tmp_template[] = "/tmp/smallest_file_number_upload_XXXXXX";
  int fd = mkstemp(tmp_template);
  if (fd == -1) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs->info_log_,
        "[fng] Failed to create temp file for publishing smallest file "
        "number: %s",
        strerror(errno));
    return IOStatus::IOError("Failed to create temp file");
  }
  std::string temp_file_path = tmp_template;
  if (fchmod(fd, S_IRUSR | S_IWUSR) != 0) {
    Log(InfoLogLevel::WARN_LEVEL, cfs->info_log_,
        "[fng] Failed to set restricted permissions on temp file: %s",
        strerror(errno));
  }
  ssize_t written = write(fd, content.c_str(), content.size());
  close(fd);
  if (written < 0 || static_cast<size_t>(written) != content.size()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs->info_log_,
        "[fng] Failed to write temp file %s: %s", temp_file_path.c_str(),
        strerror(errno));
    std::remove(temp_file_path.c_str());
    return IOStatus::IOError("Failed to write temp file");
  }

  IOStatus st = cfs->GetStorageProvider()->PutCloudObject(
      temp_file_path, cfs->GetDestBucketName(), object_key);
  TEST_SYNC_POINT_CALLBACK(
      "FileNumberGuard::PutSmallestFileNumberObject:Status", &st);

  if (std::remove(temp_file_path.c_str()) != 0) {
    Log(InfoLogLevel::WARN_LEVEL, cfs->info_log_,
        "[fng] Failed to remove temp file %s", temp_file_path.c_str());
  }

  if (st.ok()) {
    Log(InfoLogLevel::INFO_LEVEL, cfs->info_log_,
        "[fng] Published smallest file number %llu for epoch %s (%s)",
        static_cast<unsigned long long>(value), epoch.c_str(),
        object_key.c_str());
  } else {
    Log(InfoLogLevel::ERROR_LEVEL, cfs->info_log_,
        "[fng] Failed to publish smallest file number %llu for epoch %s: %s",
        static_cast<unsigned long long>(value), epoch.c_str(),
        st.ToString().c_str());
  }
  return st;
}

// ---------------- FileNumberSlidingWindow ----------------

void FileNumberSlidingWindow::Add(uint64_t file_number, uint64_t thread_id,
                                  int job_id) {
  // emplace keeps an existing entry: for multi-CF jobs firing several begin
  // events, the earliest (smallest, hence safest) snapshot wins.
  entries_.emplace(std::make_pair(thread_id, job_id), Entry{file_number, {}});
}

void FileNumberSlidingWindow::MarkRemoved(uint64_t thread_id, int job_id,
                                          Clock::time_point now) {
  auto it = entries_.find(std::make_pair(thread_id, job_id));
  if (it != entries_.end()) {
    it->second.removed = true;
    it->second.removed_at = now;
  }
}

uint64_t FileNumberSlidingWindow::SmallestFileNumber(Clock::time_point now) {
  uint64_t smallest = std::numeric_limits<uint64_t>::max();
  for (auto it = entries_.begin(); it != entries_.end();) {
    if (it->second.removed && now - it->second.removed_at >= entry_duration_) {
      it = entries_.erase(it);
      continue;
    }
    if (it->second.file_number < smallest) {
      smallest = it->second.file_number;
    }
    ++it;
  }
  return smallest;
}

// ---------------- FileNumberGuardPublisher ----------------

FileNumberGuardPublisher::FileNumberGuardPublisher(
    CloudFileSystemImpl *cfs, std::chrono::milliseconds publish_interval,
    std::chrono::milliseconds entry_duration)
    : cfs_(cfs),
      publish_interval_(publish_interval),
      window_(entry_duration),
      scheduler_(CloudScheduler::Get()) {}

FileNumberGuardPublisher::~FileNumberGuardPublisher() { Stop(); }

void FileNumberGuardPublisher::Start() {
  std::lock_guard<std::mutex> lk(stop_mutex_);
  if (stopped_ || job_handle_ >= 0) {
    return;
  }
  auto interval = std::chrono::duration_cast<std::chrono::microseconds>(
      publish_interval_);
  job_handle_ = scheduler_->ScheduleRecurringJob(
      interval, interval, [this](void *) { PeriodicPublish(); }, nullptr);
}

void FileNumberGuardPublisher::Stop() {
  long handle = -1;
  {
    std::lock_guard<std::mutex> lk(stop_mutex_);
    if (stopped_) {
      return;
    }
    stopped_ = true;
    handle = job_handle_;
    job_handle_ = -1;
  }
  stop_cv_.notify_all();
  if (handle >= 0) {
    // Waits for a currently running periodic callback to finish, so after
    // this returns no callback can touch this object.
    scheduler_->CancelJob(handle);
  }
}

std::string FileNumberGuardPublisher::CurrentEpoch() const {
  auto *manifest = cfs_->GetCloudManifest();
  if (manifest == nullptr) {
    return "";
  }
  return manifest->GetCurrentEpoch();
}

Status FileNumberGuardPublisher::PutValue(uint64_t value,
                                          const std::string &epoch) {
  return PutSmallestFileNumberObject(cfs_, value, epoch);
}

Status FileNumberGuardPublisher::OnJobBegin(uint64_t file_number,
                                            uint64_t thread_id, int job_id) {
  bool below_watermark;
  {
    std::lock_guard<std::mutex> lk(state_mutex_);
    window_.Add(file_number, thread_id, job_id);
    below_watermark = file_number < last_published_;
  }
  if (!below_watermark) {
    return Status::OK();
  }

  std::string epoch = CurrentEpoch();
  if (epoch.empty()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "[fng] Cannot publish downward for job %d: epoch unavailable",
        job_id);
    return Status::InvalidArgument("empty epoch for file number guard");
  }

  // Downward publish. This closes the race where the published value is
  // UINT64_MAX (idle) and a new job starts: the guard must reach S3 before
  // the job proceeds to upload anything.
  std::lock_guard<std::mutex> publish_lk(publish_mutex_);
  {
    // Re-check: a concurrent downward publish may have already lowered the
    // watermark below our value while we waited for the publish mutex.
    std::lock_guard<std::mutex> lk(state_mutex_);
    if (file_number >= last_published_) {
      return Status::OK();
    }
  }

  // Retry with backoff until the PUT succeeds or the publisher is stopped.
  // last_published_ must not advance past a failed downward PUT: if the one
  // PUT that lowers the value fails and we advanced anyway, the downward
  // path would never re-fire and the purger could delete an in-flight
  // upload.
  Status st;
  auto backoff = std::chrono::milliseconds(100);
  const auto max_backoff = std::chrono::milliseconds(2000);
  while (true) {
    st = PutValue(file_number, epoch);
    if (st.ok()) {
      std::lock_guard<std::mutex> lk(state_mutex_);
      if (file_number < last_published_) {
        last_published_ = file_number;
      }
      sentinel_dirty_ = false;
      return Status::OK();
    }
    Log(InfoLogLevel::WARN_LEVEL, cfs_->info_log_,
        "[fng] Downward publish of %llu failed (%s); retrying in %lld ms "
        "while blocking job %d",
        static_cast<unsigned long long>(file_number), st.ToString().c_str(),
        static_cast<long long>(backoff.count()), job_id);
    std::unique_lock<std::mutex> lk(stop_mutex_);
    if (stop_cv_.wait_for(lk, backoff, [this] { return stopped_; })) {
      // Shutting down. The job proceeds, but the DB is closing; report the
      // failure to the caller.
      return st;
    }
    backoff = std::min(backoff * 2, max_backoff);
  }
}

void FileNumberGuardPublisher::OnJobEnd(uint64_t thread_id, int job_id) {
  std::lock_guard<std::mutex> lk(state_mutex_);
  window_.MarkRemoved(thread_id, job_id);
}

Status FileNumberGuardPublisher::BlockPurger() {
  std::string epoch = CurrentEpoch();
  if (epoch.empty()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "[fng] Cannot block purger: epoch unavailable");
    return Status::InvalidArgument("empty epoch for file number guard");
  }
  std::lock_guard<std::mutex> publish_lk(publish_mutex_);
  // Deliberately does NOT touch last_published_: the sentinel is transient
  // ("blocked until the writer proves otherwise") and any future smaller
  // real value must still trigger a downward publish.
  Status st = PutValue(0, epoch);
  if (st.ok()) {
    // S3 now holds 0 while last_published_ is unchanged; mark the mismatch
    // so the next periodic publish refreshes the object even if the window
    // minimum still equals last_published_ (e.g. idle -> UINT64_MAX).
    std::lock_guard<std::mutex> lk(state_mutex_);
    sentinel_dirty_ = true;
  }
  return st;
}

void FileNumberGuardPublisher::PeriodicPublish() {
  {
    std::lock_guard<std::mutex> lk(stop_mutex_);
    if (stopped_) {
      return;
    }
  }

  uint64_t desired;
  {
    std::lock_guard<std::mutex> lk(state_mutex_);
    desired = window_.SmallestFileNumber();
    if (desired == last_published_ && !sentinel_dirty_) {
      return;
    }
  }

  std::string epoch = CurrentEpoch();
  if (epoch.empty()) {
    Log(InfoLogLevel::WARN_LEVEL, cfs_->info_log_,
        "[fng] Skipping periodic publish: epoch unavailable");
    return;
  }

  // try_lock: if a downward publish is in flight (possibly retrying with
  // backoff), skip this tick instead of stalling the shared scheduler
  // thread behind S3 latency.
  std::unique_lock<std::mutex> publish_lk(publish_mutex_, std::try_to_lock);
  if (!publish_lk.owns_lock()) {
    return;
  }

  {
    // Staleness re-check after acquiring the publish mutex: a downward
    // publish that landed while we waited must not be overwritten with our
    // older, higher value.
    std::lock_guard<std::mutex> lk(state_mutex_);
    desired = window_.SmallestFileNumber();
    if (desired == last_published_ && !sentinel_dirty_) {
      return;
    }
  }

  Status st = PutValue(desired, epoch);
  if (st.ok()) {
    std::lock_guard<std::mutex> lk(state_mutex_);
    last_published_ = desired;
    sentinel_dirty_ = false;
  }
  // On failure last_published_ is unchanged; the next tick retries. Upward
  // movement is not urgent, so no synchronous retry here.
}

uint64_t FileNumberGuardPublisher::TEST_LastPublished() {
  std::lock_guard<std::mutex> lk(state_mutex_);
  return last_published_;
}

// ---------------- FileNumberGuardListener ----------------

void FileNumberGuardListener::JobBegin(DB *db, uint64_t thread_id,
                                       int job_id) {
  if (db == nullptr || !publisher_) {
    return;
  }
  // Output file numbers are allocated after this callback fires, so every
  // in-flight output number of this job is > this snapshot.
  uint64_t snapshot = db->GetNextFileNumber() - 1;
  Status s = publisher_->OnJobBegin(snapshot, thread_id, job_id);
  s.PermitUncheckedError();
}

void FileNumberGuardListener::OnFlushBegin(DB *db, const FlushJobInfo &info) {
  JobBegin(db, info.thread_id, info.job_id);
}

void FileNumberGuardListener::OnFlushCompleted(DB * /*db*/,
                                               const FlushJobInfo &info) {
  if (publisher_) {
    publisher_->OnJobEnd(info.thread_id, info.job_id);
  }
}

void FileNumberGuardListener::OnCompactionBegin(DB *db,
                                                const CompactionJobInfo &info) {
  JobBegin(db, info.thread_id, info.job_id);
}

void FileNumberGuardListener::OnCompactionCompleted(
    DB * /*db*/, const CompactionJobInfo &info) {
  if (publisher_) {
    publisher_->OnJobEnd(info.thread_id, info.job_id);
  }
}

// ---------------- CloudFileSystemImpl glue ----------------

void CloudFileSystemImpl::SetFileNumberGuardPublisher(
    std::shared_ptr<FileNumberGuardPublisher> publisher) {
  auto previous = std::atomic_exchange(&file_number_guard_, publisher);
  if (previous) {
    previous->Stop();
  }
}

std::shared_ptr<FileNumberGuardPublisher>
CloudFileSystemImpl::GetFileNumberGuardPublisher() const {
  return std::atomic_load(&file_number_guard_);
}

void CloudFileSystemImpl::StopFileNumberGuard() {
  auto publisher = std::atomic_exchange(
      &file_number_guard_, std::shared_ptr<FileNumberGuardPublisher>());
  if (publisher) {
    publisher->Stop();
  }
}

Status CloudFileSystemImpl::BlockPurger() {
  auto publisher = GetFileNumberGuardPublisher();
  if (publisher) {
    return publisher->BlockPurger();
  }
  // No publisher registered (guard disabled): one-shot sentinel with no
  // ordering concerns, since nothing else on this node writes the object.
  auto *manifest = GetCloudManifest();
  if (manifest == nullptr) {
    return Status::InvalidArgument(
        "BlockPurger requires a loaded cloud manifest");
  }
  return PutSmallestFileNumberObject(this, 0, manifest->GetCurrentEpoch());
}

}  // namespace ROCKSDB_NAMESPACE
