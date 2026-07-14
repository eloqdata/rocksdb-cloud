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

#include <atomic>
#include <condition_variable>
#include <fstream>
#include <future>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cloud/cloud_manifest.h"
#include "file/writable_file_writer.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/cloud/cloud_storage_provider_impl.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/utilities/options_type.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

namespace {

constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();
constexpr auto kAsyncWaitTimeout = std::chrono::seconds(5);

// Records every PutCloudObject; everything else is unsupported. The
// publisher only ever needs PUT.
class RecordingStorageProvider : public CloudStorageProvider {
 public:
  const char *Name() const override { return "recording"; }

  void SetPutStatus(const std::string &object_path, IOStatus status) {
    std::lock_guard<std::mutex> lk(mu_);
    put_statuses_[object_path] = std::move(status);
  }

  IOStatus PutCloudObject(const std::string &local_path,
                          const std::string & /*bucket_name*/,
                          const std::string &object_path) override {
    TEST_SYNC_POINT_CALLBACK("FileNumberGuardTest::PutCloudObject",
                             const_cast<std::string *>(&object_path));
    std::ifstream f(local_path);
    std::string content((std::istreambuf_iterator<char>(f)),
                        std::istreambuf_iterator<char>());
    std::lock_guard<std::mutex> lk(mu_);
    ++put_count_;
    auto status = put_statuses_.find(object_path);
    if (status != put_statuses_.end() && !status->second.ok()) {
      return status->second;
    }
    objects_[object_path] = content;
    return IOStatus::OK();
  }

  int PutCount() {
    std::lock_guard<std::mutex> lk(mu_);
    return put_count_;
  }

  std::string Content(const std::string &key) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = objects_.find(key);
    return it == objects_.end() ? "" : it->second;
  }

  bool HasObject(const std::string &key) {
    std::lock_guard<std::mutex> lk(mu_);
    return objects_.find(key) != objects_.end();
  }

  // -- everything below is unused by the publisher --
  IOStatus CreateBucket(const std::string &) override { return NotSup(); }
  IOStatus ExistsBucket(const std::string &) override { return NotSup(); }
  IOStatus EmptyBucket(const std::string &, const std::string &) override {
    return NotSup();
  }
  IOStatus DeleteCloudObject(const std::string &,
                             const std::string &) override {
    return NotSup();
  }
  IOStatus ListCloudObjects(const std::string &, const std::string &,
                            std::vector<std::string> *) override {
    return NotSup();
  }
  IOStatus ListCloudObjects(
      const std::string &, const std::string &,
      std::vector<std::pair<std::string, CloudObjectInformation>> *) override {
    return NotSup();
  }
  IOStatus ListCloudObjectsWithPrefix(const std::string &, const std::string &,
                                      const std::string &,
                                      std::vector<std::string> *) override {
    return NotSup();
  }
  IOStatus ExistsCloudObject(const std::string &,
                             const std::string &) override {
    return NotSup();
  }
  IOStatus GetCloudObjectSize(const std::string &, const std::string &,
                              uint64_t *) override {
    return NotSup();
  }
  IOStatus GetCloudObjectModificationTime(const std::string &,
                                          const std::string &,
                                          uint64_t *) override {
    return NotSup();
  }
  IOStatus GetCloudObjectMetadata(const std::string &, const std::string &,
                                  CloudObjectInformation *) override {
    return NotSup();
  }
  IOStatus CopyCloudObject(const std::string &, const std::string &,
                           const std::string &, const std::string &) override {
    return NotSup();
  }
  IOStatus GetCloudObject(const std::string &, const std::string &,
                          const std::string &) override {
    return NotSup();
  }
  IOStatus PutCloudObjectMetadata(
      const std::string &, const std::string &,
      const std::unordered_map<std::string, std::string> &) override {
    return NotSup();
  }
  IOStatus NewCloudWritableFile(
      const std::string &, const std::string &, const std::string &,
      const FileOptions &, std::unique_ptr<CloudStorageWritableFile> *,
      IODebugContext *) override {
    return NotSup();
  }
  IOStatus NewCloudReadableFile(const std::string &, const std::string &,
                                const FileOptions &,
                                std::unique_ptr<CloudStorageReadableFile> *,
                                IODebugContext *) override {
    return NotSup();
  }

 private:
  static IOStatus NotSup() {
    return IOStatus::NotSupported("RecordingStorageProvider");
  }

  std::mutex mu_;
  int put_count_ = 0;
  std::unordered_map<std::string, IOStatus> put_statuses_;
  std::unordered_map<std::string, std::string> objects_;
};

}  // namespace

// ---------------- Window unit tests ----------------

TEST(FileNumberSlidingWindowTest, MinAddRemoveLingerExpiry) {
  using Clock = FileNumberSlidingWindow::Clock;
  FileNumberSlidingWindow w(std::chrono::milliseconds(1000));
  auto t0 = Clock::now();

  // Empty window == UINT64_MAX.
  ASSERT_EQ(w.SmallestFileNumber(t0), kMax);

  w.Add(100, /*thread*/ 1, /*job*/ 1);
  w.Add(50, 2, 2);
  ASSERT_EQ(w.SmallestFileNumber(t0), 50u);

  // Re-adding the same job keeps the original entry.
  w.Add(10, 1, 1);
  ASSERT_EQ(w.SmallestFileNumber(t0), 50u);

  // A removed entry lingers for entry_duration...
  w.MarkRemoved(2, 2, t0);
  ASSERT_EQ(w.SmallestFileNumber(t0 + std::chrono::milliseconds(500)), 50u);
  // ...and stops contributing once expired.
  ASSERT_EQ(w.SmallestFileNumber(t0 + std::chrono::milliseconds(1500)), 100u);

  // Removing an unknown job is a no-op.
  w.MarkRemoved(9, 9, t0);
  ASSERT_EQ(w.SmallestFileNumber(t0 + std::chrono::milliseconds(1500)), 100u);

  auto t1 = t0 + std::chrono::milliseconds(1500);
  w.MarkRemoved(1, 1, t1);
  ASSERT_EQ(w.SmallestFileNumber(t1 + std::chrono::milliseconds(900)), 100u);
  ASSERT_EQ(w.SmallestFileNumber(t1 + std::chrono::milliseconds(1100)), kMax);
  ASSERT_EQ(w.Size(), 0u);
}

// ---------------- Key format ----------------

TEST(FileNumberGuardKeyTest, KeyFormatIsStable) {
  // This format is the on-cloud protocol shared with pre-existing external
  // writers; it must remain byte-identical.
  ASSERT_EQ(SmallestFileNumberObjectKey("db/path", "abc123"),
            "db/path/smallest_new_file_number-abc123");
  ASSERT_EQ(SmallestFileNumberObjectKey("db/path/", "abc123"),
            "db/path/smallest_new_file_number-abc123");
  ASSERT_EQ(SmallestFileNumberObjectKey("", "e"),
            "smallest_new_file_number-e");
  ASSERT_EQ(std::string(kSmallestFileNumberFilePrefix),
            "smallest_new_file_number-");
}

// ---------------- Cloud file system option tests ----------------

TEST(CloudFileSystemGuardOptionsTest, DurationsRoundTripInMilliseconds) {
  ConfigOptions config_options;
  CloudFileSystemOptions options;

  ASSERT_OK(options.Configure(
      config_options,
      "guard_publish_interval_ms=1234;guard_entry_duration_ms=5678"));
  ASSERT_EQ(options.guard_publish_interval, std::chrono::milliseconds(1234));
  ASSERT_EQ(options.guard_entry_duration, std::chrono::milliseconds(5678));

  std::string serialized;
  ASSERT_OK(options.Serialize(config_options, &serialized));
  CloudFileSystemOptions copy;
  ASSERT_OK(copy.Configure(config_options, serialized));
  ASSERT_EQ(copy.guard_publish_interval, std::chrono::milliseconds(1234));
  ASSERT_EQ(copy.guard_entry_duration, std::chrono::milliseconds(5678));
}

TEST(CloudFileSystemGuardOptionsTest, DurationsParticipateInEquality) {
  ConfigOptions config_options;
  CloudFileSystemOptions expected;
  CloudFileSystemOptions actual;
  std::string mismatch;

  actual.guard_publish_interval = std::chrono::milliseconds(1);
  ASSERT_FALSE(OptionTypeInfo::TypesAreEqual(
      config_options, CloudFileSystemOptions::cloud_fs_option_type_info,
      &expected, &actual, &mismatch));
  ASSERT_EQ(mismatch, "guard_publish_interval_ms");

  actual = expected;
  actual.guard_entry_duration = std::chrono::milliseconds(1);
  mismatch.clear();
  ASSERT_FALSE(OptionTypeInfo::TypesAreEqual(
      config_options, CloudFileSystemOptions::cloud_fs_option_type_info,
      &expected, &actual, &mismatch));
  ASSERT_EQ(mismatch, "guard_entry_duration_ms");
}

TEST(CloudFileSystemGuardOptionsTest, DurationsMustBePositive) {
  auto validate = [](std::chrono::milliseconds publish_interval,
                     std::chrono::milliseconds entry_duration) {
    CloudFileSystemOptions options;
    options.storage_provider = std::make_shared<RecordingStorageProvider>();
    options.guard_publish_interval = publish_interval;
    options.guard_entry_duration = entry_duration;
    CloudFileSystemImpl cfs(options, FileSystem::Default(), nullptr);
    return cfs.ValidateOptions(DBOptions(), ColumnFamilyOptions());
  };

  for (int value : {0, -1}) {
    Status status = validate(std::chrono::milliseconds(value),
                             std::chrono::milliseconds(1));
    ASSERT_TRUE(status.IsInvalidArgument()) << status.ToString();
    ASSERT_NE(status.ToString().find("guard_publish_interval_ms"),
              std::string::npos);

    status = validate(std::chrono::milliseconds(1),
                      std::chrono::milliseconds(value));
    ASSERT_TRUE(status.IsInvalidArgument()) << status.ToString();
    ASSERT_NE(status.ToString().find("guard_entry_duration_ms"),
              std::string::npos);
  }

  ASSERT_OK(validate(std::chrono::milliseconds(1),
                     std::chrono::milliseconds(1)));
}

// ---------------- Publisher tests ----------------

class FileNumberGuardTest : public testing::Test {
 public:
  FileNumberGuardTest() {
    tmp_dir_ = test::TmpDir() + "/file_number_guard_test_" +
               std::to_string(Env::Default()->NowMicros());
    Env::Default()->CreateDirIfMissing(tmp_dir_);
    provider_ = std::make_shared<RecordingStorageProvider>();

    CloudFileSystemOptions opts;
    opts.storage_provider = provider_;
    opts.dest_bucket.SetBucketName("guard-bucket");
    opts.dest_bucket.SetBucketPrefix("");
    opts.dest_bucket.SetObjectPath("dbpath");
    opts.cloud_file_deletion_delay = std::nullopt;
    cfs_ = std::make_unique<CloudFileSystemImpl>(opts, FileSystem::Default(),
                                                 nullptr);
  }

  ~FileNumberGuardTest() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }

  void LoadManifestWithEpoch(const std::string &epoch) {
    std::unique_ptr<CloudManifest> manifest;
    ASSERT_OK(CloudManifest::CreateForEmptyDatabase(epoch, &manifest));
    std::unique_ptr<WritableFileWriter> writer;
    ASSERT_OK(WritableFileWriter::Create(FileSystem::Default(),
                                         tmp_dir_ + "/CLOUDMANIFEST",
                                         FileOptions(), &writer, nullptr));
    ASSERT_OK(manifest->WriteToLog(std::move(writer)));
    ASSERT_OK(cfs_->LoadLocalCloudManifest(tmp_dir_));
  }

  static const std::string &GuardKey() {
    static const std::string key =
        SmallestFileNumberObjectKey("dbpath", "epoch1");
    return key;
  }

  std::string tmp_dir_;
  std::shared_ptr<RecordingStorageProvider> provider_;
  std::unique_ptr<CloudFileSystemImpl> cfs_;
};

TEST_F(FileNumberGuardTest, JobRegistrationDoesNotPublish) {
  LoadManifestWithEpoch("epoch1");
  FileNumberGuardPublisher pub(cfs_.get(), std::chrono::seconds(30),
                               std::chrono::seconds(15));

  pub.OnJobBegin(42, 1, 1);
  ASSERT_EQ(provider_->PutCount(), 0);
  ASSERT_EQ(pub.TEST_LastPublished(), kMax);
}

TEST_F(FileNumberGuardTest, SentinelDoesNotAdvanceWatermark) {
  LoadManifestWithEpoch("epoch1");
  FileNumberGuardPublisher pub(cfs_.get(), std::chrono::seconds(30),
                               std::chrono::seconds(15));

  ASSERT_OK(pub.BlockPurger());
  ASSERT_EQ(provider_->Content(GuardKey()), "0");
  // The sentinel must not advance the watermark: a later real value
  // (necessarily > 0) still has to be published.
  ASSERT_EQ(pub.TEST_LastPublished(), kMax);

  // Protected uploads keep the known-safe sentinel in place until the
  // post-open periodic publish replaces it.
  pub.OnJobBegin(42, 1, 1);
  ASSERT_EQ(provider_->Content(GuardKey()), "0");
  bool uploaded = false;
  ASSERT_OK(pub.ProtectFileUpload(42, [&] {
    uploaded = true;
    return Status::OK();
  }));
  ASSERT_TRUE(uploaded);
  ASSERT_EQ(provider_->Content(GuardKey()), "0");
  ASSERT_EQ(pub.TEST_LastPublished(), kMax);

  pub.PeriodicPublish();
  ASSERT_EQ(provider_->Content(GuardKey()), "42");
  ASSERT_EQ(pub.TEST_LastPublished(), 42u);
}

TEST_F(FileNumberGuardTest, SentinelOverwrittenByNextPeriodicPublish) {
  LoadManifestWithEpoch("epoch1");
  FileNumberGuardPublisher pub(cfs_.get(), std::chrono::seconds(30),
                               std::chrono::seconds(15));

  ASSERT_OK(pub.BlockPurger());
  ASSERT_EQ(provider_->Content(GuardKey()), "0");
  // Idle: the next periodic publish must replace the sentinel with
  // UINT64_MAX even though the window minimum (MAX) equals last_published_.
  pub.PeriodicPublish();
  ASSERT_EQ(provider_->Content(GuardKey()), std::to_string(kMax));
  // And once refreshed, further idle ticks are no-ops.
  int puts = provider_->PutCount();
  pub.PeriodicPublish();
  ASSERT_EQ(provider_->PutCount(), puts);
}

TEST_F(FileNumberGuardTest, DownwardPublishTriggerCondition) {
  LoadManifestWithEpoch("epoch1");
  FileNumberGuardPublisher pub(cfs_.get(), std::chrono::seconds(30),
                               std::chrono::seconds(15));

  pub.OnJobBegin(100, 1, 1);
  ASSERT_EQ(provider_->PutCount(), 0);
  ASSERT_OK(pub.ProtectFileUpload(100));
  int puts_after_first = provider_->PutCount();
  ASSERT_EQ(provider_->Content(GuardKey()), "100");

  // A job at or above the watermark publishes nothing.
  pub.OnJobBegin(200, 2, 2);
  pub.OnJobBegin(100, 3, 3);
  ASSERT_EQ(provider_->PutCount(), puts_after_first);

  // A job below the watermark publishes synchronously.
  pub.OnJobBegin(50, 4, 4);
  ASSERT_OK(pub.ProtectFileUpload(50));
  ASSERT_EQ(provider_->Content(GuardKey()), "50");
  ASSERT_EQ(pub.TEST_LastPublished(), 50u);
}

TEST_F(FileNumberGuardTest, ProtectPublishesLiveWindowMinimum) {
  LoadManifestWithEpoch("epoch1");
  FileNumberGuardPublisher pub(cfs_.get(), std::chrono::seconds(30),
                               std::chrono::seconds(15));

  pub.OnJobBegin(100, 1, 1);
  pub.OnJobBegin(200, 2, 2);
  ASSERT_OK(pub.ProtectFileUpload(200));

  ASSERT_EQ(provider_->Content(GuardKey()), "100");
  ASSERT_EQ(pub.TEST_LastPublished(), 100u);
}

TEST_F(FileNumberGuardTest, ProtectWaitsForInFlightUpwardPublish) {
  LoadManifestWithEpoch("epoch1");
  FileNumberGuardPublisher pub(cfs_.get(), std::chrono::seconds(30),
                               std::chrono::milliseconds(0));

  pub.OnJobBegin(50, 1, 1);
  ASSERT_OK(pub.ProtectFileUpload(50));
  pub.OnJobEnd(1, 1);
  pub.OnJobBegin(100, 2, 2);

  std::mutex block_mutex;
  std::condition_variable block_cv;
  bool upward_put_reached = false;
  bool release_upward_put = false;
  SyncPoint::GetInstance()->SetCallBack(
      "FileNumberGuard::PutSmallestFileNumberObject:Status", [&](void*) {
        if (provider_->Content(GuardKey()) != "100") {
          return;
        }
        std::unique_lock<std::mutex> lock(block_mutex);
        upward_put_reached = true;
        block_cv.notify_all();
        block_cv.wait(lock, [&] { return release_upward_put; });
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::thread periodic([&] { pub.PeriodicPublish(); });
  {
    std::unique_lock<std::mutex> lock(block_mutex);
    block_cv.wait(lock, [&] { return upward_put_reached; });
  }

  pub.OnJobBegin(60, 3, 3);
  auto protection = std::async(std::launch::async,
                               [&] { return pub.ProtectFileUpload(60); });
  const auto state = protection.wait_for(std::chrono::milliseconds(100));

  {
    std::lock_guard<std::mutex> lock(block_mutex);
    release_upward_put = true;
  }
  block_cv.notify_all();
  periodic.join();
  Status protection_status = protection.get();

  ASSERT_EQ(state, std::future_status::timeout);
  ASSERT_OK(protection_status);
  ASSERT_EQ(provider_->Content(GuardKey()), "60");
  ASSERT_EQ(pub.TEST_LastPublished(), 60u);
}

TEST_F(FileNumberGuardTest, ProtectRejectsMissingOrTooHighWindow) {
  LoadManifestWithEpoch("epoch1");
  FileNumberGuardPublisher pub(cfs_.get(), std::chrono::seconds(30),
                               std::chrono::seconds(15));

  ASSERT_TRUE(pub.ProtectFileUpload(100).IsInvalidArgument());
  pub.OnJobBegin(200, 1, 1);
  ASSERT_TRUE(pub.ProtectFileUpload(100).IsInvalidArgument());
  ASSERT_EQ(provider_->PutCount(), 0);
}

TEST_F(FileNumberGuardTest, PeriodicPublishesWindowMinThenMaxWhenIdle) {
  LoadManifestWithEpoch("epoch1");
  // Short entry duration so completed entries expire quickly.
  FileNumberGuardPublisher pub(cfs_.get(), std::chrono::seconds(30),
                               std::chrono::milliseconds(50));

  pub.OnJobBegin(100, 1, 1);
  ASSERT_OK(pub.ProtectFileUpload(100));  // publishes 100 downward
  pub.OnJobBegin(200, 2, 2);              // no publish

  // Window min is 100 == last published: periodic publish is a no-op.
  int puts = provider_->PutCount();
  pub.PeriodicPublish();
  ASSERT_EQ(provider_->PutCount(), puts);

  // Job 1 completes; entry lingers, so min is still 100.
  pub.OnJobEnd(1, 1);
  pub.PeriodicPublish();
  ASSERT_EQ(provider_->PutCount(), puts);

  // After the linger expires, the min rises to job 2's snapshot.
  std::this_thread::sleep_for(std::chrono::milliseconds(80));
  pub.PeriodicPublish();
  ASSERT_EQ(provider_->Content(GuardKey()), "200");

  // All jobs done and expired: idle publishes UINT64_MAX.
  pub.OnJobEnd(2, 2);
  std::this_thread::sleep_for(std::chrono::milliseconds(80));
  pub.PeriodicPublish();
  ASSERT_EQ(provider_->Content(GuardKey()), std::to_string(kMax));
  ASSERT_EQ(pub.TEST_LastPublished(), kMax);
}

TEST_F(FileNumberGuardTest,
       AmbiguousPeriodicFailureRepairsFiniteAndRetriesIdle) {
  LoadManifestWithEpoch("epoch1");
  FileNumberGuardPublisher pub(cfs_.get(), std::chrono::seconds(30),
                               std::chrono::milliseconds(0));

  pub.OnJobBegin(100, 1, 1);
  ASSERT_OK(pub.ProtectFileUpload(100));
  pub.OnJobEnd(1, 1);
  pub.OnJobBegin(200, 2, 2);

  std::atomic<bool> fail_next{true};
  SyncPoint::GetInstance()->SetCallBack(
      "FileNumberGuard::PutSmallestFileNumberObject:Status", [&](void *arg) {
        if (fail_next.exchange(false)) {
          *static_cast<IOStatus *>(arg) =
              IOStatus::IOError("injected post-store failure");
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // The provider stored 200, but the caller cannot know that after the error.
  pub.PeriodicPublish();
  ASSERT_EQ(provider_->Content(GuardKey()), "200");
  ASSERT_EQ(pub.TEST_LastPublished(), kMax);

  // Unknown remote state forces a finite repair even though MAX would
  // otherwise suppress a downward comparison.
  pub.OnJobBegin(150, 3, 3);
  pub.PeriodicPublish();
  ASSERT_EQ(provider_->Content(GuardKey()), "150");
  ASSERT_EQ(pub.TEST_LastPublished(), 150u);

  pub.OnJobEnd(2, 2);
  pub.OnJobEnd(3, 3);
  fail_next = true;
  pub.PeriodicPublish();
  ASSERT_EQ(provider_->Content(GuardKey()), std::to_string(kMax));
  int puts_after_ambiguous_idle = provider_->PutCount();
  ASSERT_EQ(pub.TEST_LastPublished(), kMax);

  // desired == last == MAX must still retry while the remote state is
  // unknown.
  pub.PeriodicPublish();
  ASSERT_EQ(provider_->PutCount(), puts_after_ambiguous_idle + 1);
  ASSERT_EQ(pub.TEST_LastPublished(), kMax);
}

TEST_F(FileNumberGuardTest, ListenerSeparatesIngestionAndRecoveryDomains) {
  LoadManifestWithEpoch("epoch1");
  auto pub = std::make_shared<FileNumberGuardPublisher>(
      cfs_.get(), std::chrono::seconds(30), std::chrono::milliseconds(0));
  FileNumberGuardListener listener(pub);

  listener.OnExternalFileIngestionStarted(nullptr, 77);
  TableFileCreationBriefInfo started;
  started.db_name = "db";
  started.cf_name = "default";
  started.file_path = "/tmp/000077.sst";
  started.job_id = 9;
  started.reason = TableFileCreationReason::kRecovery;
  listener.OnTableFileCreationStarted(started);

  listener.OnExternalFileIngestionFinished(nullptr, 77);
  ASSERT_OK(pub->ProtectFileUpload(77));

  TableFileCreationInfo finished;
  finished.db_name = started.db_name;
  finished.cf_name = started.cf_name;
  finished.file_path = "(nil)";
  finished.job_id = started.job_id;
  finished.reason = started.reason;
  finished.status = Status::Aborted("empty recovery output");
  listener.OnTableFileCreated(finished);
  finished.status.PermitUncheckedError();

  ASSERT_TRUE(pub->ProtectFileUpload(77).IsInvalidArgument());

  started.file_path = "/tmp/000078.sst";
  started.job_id = 10;
  listener.OnTableFileCreationStarted(started);
  ASSERT_OK(pub->ProtectFileUpload(78));

  finished.file_path = started.file_path;
  finished.job_id = started.job_id;
  finished.status = Status::OK();
  listener.OnTableFileCreated(finished);
  ASSERT_TRUE(pub->ProtectFileUpload(78).IsInvalidArgument());
}

TEST_F(FileNumberGuardTest, ConditionalPublisherRemovalPreservesReplacement) {
  LoadManifestWithEpoch("epoch1");
  auto old = std::make_shared<FileNumberGuardPublisher>(
      cfs_.get(), std::chrono::seconds(30), std::chrono::seconds(15));
  auto replacement = std::make_shared<FileNumberGuardPublisher>(
      cfs_.get(), std::chrono::seconds(30), std::chrono::seconds(15));
  cfs_->SetFileNumberGuardPublisher(old);
  cfs_->SetFileNumberGuardPublisher(replacement);

  ASSERT_TRUE(old->ProtectFileUpload(1).IsShutdownInProgress());
  ASSERT_FALSE(cfs_->StopFileNumberGuardPublisher(old));
  ASSERT_FALSE(cfs_->RemoveFileNumberGuardPublisher(old));
  ASSERT_EQ(cfs_->GetFileNumberGuardPublisher(), replacement);
  ASSERT_TRUE(cfs_->StopFileNumberGuardPublisher(replacement));
  ASSERT_EQ(cfs_->GetFileNumberGuardPublisher(), replacement);
  ASSERT_TRUE(replacement->ProtectFileUpload(1).IsShutdownInProgress());
  ASSERT_TRUE(cfs_->RemoveFileNumberGuardPublisher(replacement));
  ASSERT_EQ(cfs_->GetFileNumberGuardPublisher(), nullptr);
}

TEST_F(FileNumberGuardTest, InstallWaitsForOldPublisherBeforeSentinel) {
  LoadManifestWithEpoch("epoch1");
  auto old = std::make_shared<FileNumberGuardPublisher>(
      cfs_.get(), std::chrono::seconds(30), std::chrono::milliseconds(0));
  old->OnJobBegin(10, 1, 1);
  cfs_->SetFileNumberGuardPublisher(old);
  ASSERT_OK(old->ProtectFileUpload(10));
  old->OnJobEnd(1, 1);

  std::mutex mutex;
  std::condition_variable cv;
  bool old_put_reached = false;
  bool release_old_put = false;
  SyncPoint::GetInstance()->SetCallBack(
      "FileNumberGuard::PutSmallestFileNumberObject:Status", [&](void *) {
        if (provider_->Content(GuardKey()) != std::to_string(kMax)) {
          return;
        }
        std::unique_lock<std::mutex> lock(mutex);
        old_put_reached = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release_old_put; });
      });
  SyncPoint::GetInstance()->EnableProcessing();

  auto old_publish =
      std::async(std::launch::async, [&] { old->PeriodicPublish(); });
  {
    std::unique_lock<std::mutex> lock(mutex);
    const bool reached =
        cv.wait_for(lock, kAsyncWaitTimeout, [&] { return old_put_reached; });
    EXPECT_TRUE(reached);
  }

  auto replacement = std::make_shared<FileNumberGuardPublisher>(
      cfs_.get(), std::chrono::seconds(30), std::chrono::seconds(15));
  auto install = std::async(std::launch::async, [&] {
    return cfs_->InstallFileNumberGuardPublisher(replacement);
  });
  const auto install_state = install.wait_for(std::chrono::milliseconds(100));
  EXPECT_EQ(install_state, std::future_status::timeout);

  {
    std::lock_guard<std::mutex> lock(mutex);
    release_old_put = true;
  }
  cv.notify_all();
  old_publish.get();
  ASSERT_OK(install.get());

  ASSERT_EQ(cfs_->GetFileNumberGuardPublisher(), replacement);
  ASSERT_EQ(provider_->Content(GuardKey()), "0");
  ASSERT_TRUE(old->ProtectFileUpload(10).IsShutdownInProgress());
}

TEST_F(FileNumberGuardTest, FailedInstallKeepsOldPublisherFailClosed) {
  LoadManifestWithEpoch("epoch1");
  auto old = std::make_shared<FileNumberGuardPublisher>(
      cfs_.get(), std::chrono::seconds(30), std::chrono::seconds(15));
  auto replacement = std::make_shared<FileNumberGuardPublisher>(
      cfs_.get(), std::chrono::seconds(30), std::chrono::seconds(15));
  cfs_->SetFileNumberGuardPublisher(old);
  provider_->SetPutStatus(GuardKey(), IOStatus::IOError("sentinel failure"));

  ASSERT_NOK(cfs_->InstallFileNumberGuardPublisher(replacement));
  ASSERT_EQ(cfs_->GetFileNumberGuardPublisher(), old);
  ASSERT_TRUE(old->ProtectFileUpload(1).IsShutdownInProgress());
}

TEST_F(FileNumberGuardTest, ConcurrentInstallsPublishSentinelsInOrder) {
  LoadManifestWithEpoch("epoch1");
  auto first = std::make_shared<FileNumberGuardPublisher>(
      cfs_.get(), std::chrono::seconds(30), std::chrono::seconds(15));
  auto second = std::make_shared<FileNumberGuardPublisher>(
      cfs_.get(), std::chrono::seconds(30), std::chrono::seconds(15));

  std::mutex mutex;
  std::condition_variable cv;
  bool block_first_sentinel = true;
  bool first_sentinel_reached = false;
  bool release_first_sentinel = false;
  SyncPoint::GetInstance()->SetCallBack(
      "FileNumberGuard::PutSmallestFileNumberObject:Status", [&](void *) {
        std::unique_lock<std::mutex> lock(mutex);
        if (!block_first_sentinel || provider_->Content(GuardKey()) != "0") {
          return;
        }
        block_first_sentinel = false;
        first_sentinel_reached = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release_first_sentinel; });
      });
  SyncPoint::GetInstance()->EnableProcessing();

  auto first_install = std::async(std::launch::async, [&] {
    return cfs_->InstallFileNumberGuardPublisher(first);
  });
  {
    std::unique_lock<std::mutex> lock(mutex);
    const bool reached = cv.wait_for(lock, kAsyncWaitTimeout,
                                     [&] { return first_sentinel_reached; });
    EXPECT_TRUE(reached);
  }
  auto second_install = std::async(std::launch::async, [&] {
    return cfs_->InstallFileNumberGuardPublisher(second);
  });
  const auto second_install_state =
      second_install.wait_for(std::chrono::milliseconds(100));
  EXPECT_EQ(second_install_state, std::future_status::timeout);

  {
    std::lock_guard<std::mutex> lock(mutex);
    release_first_sentinel = true;
  }
  cv.notify_all();
  ASSERT_OK(first_install.get());
  ASSERT_OK(second_install.get());

  ASSERT_EQ(cfs_->GetFileNumberGuardPublisher(), second);
  ASSERT_EQ(provider_->Content(GuardKey()), "0");
  ASSERT_TRUE(first->ProtectFileUpload(1).IsShutdownInProgress());
}

// Failure injection: the downward PUT fails; the publisher must retry and
// must not advance the watermark past the failure.
TEST_F(FileNumberGuardTest, DownwardPublishFailureRetriesWithoutAdvancing) {
  LoadManifestWithEpoch("epoch1");
  FileNumberGuardPublisher pub(cfs_.get(), std::chrono::seconds(30),
                               std::chrono::seconds(15));

  std::atomic<int> failures_left{2};
  std::atomic<int> attempts{0};
  SyncPoint::GetInstance()->SetCallBack(
      "FileNumberGuard::PutSmallestFileNumberObject:Status", [&](void *arg) {
        ++attempts;
        if (failures_left.fetch_sub(1) > 0) {
          *static_cast<IOStatus *>(arg) =
              IOStatus::IOError("injected PUT failure");
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Run the downward publish on a separate thread: it blocks (retrying)
  // until the PUT succeeds.
  pub.OnJobBegin(10, 1, 1);
  Status job_status;
  std::thread job([&] { job_status = pub.ProtectFileUpload(10); });

  // While failures are being injected, the watermark must not advance.
  while (failures_left.load() > 0) {
    ASSERT_EQ(pub.TEST_LastPublished(), kMax);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  job.join();

  ASSERT_OK(job_status);
  ASSERT_GE(attempts.load(), 3);  // two failures + at least one success
  ASSERT_EQ(pub.TEST_LastPublished(), 10u);
  ASSERT_EQ(provider_->Content(GuardKey()), "10");
}

TEST_F(FileNumberGuardTest, StopUnblocksFailingDownwardPublish) {
  LoadManifestWithEpoch("epoch1");
  FileNumberGuardPublisher pub(cfs_.get(), std::chrono::seconds(30),
                               std::chrono::seconds(15));

  SyncPoint::GetInstance()->SetCallBack(
      "FileNumberGuard::PutSmallestFileNumberObject:Status", [&](void *arg) {
        *static_cast<IOStatus *>(arg) =
            IOStatus::IOError("injected PUT failure");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::atomic<bool> returned{false};
  Status job_status;
  pub.OnJobBegin(10, 1, 1);
  std::thread job([&] {
    job_status = pub.ProtectFileUpload(10);
    returned = true;
  });

  // The job stays blocked while the PUT keeps failing.
  std::this_thread::sleep_for(std::chrono::milliseconds(250));
  ASSERT_FALSE(returned.load());
  ASSERT_EQ(pub.TEST_LastPublished(), kMax);

  pub.Stop();
  job.join();
  ASSERT_TRUE(returned.load());
  // The failure is reported, and the watermark was never advanced past it.
  ASSERT_FALSE(job_status.ok());
  ASSERT_EQ(pub.TEST_LastPublished(), kMax);
}

TEST_F(FileNumberGuardTest, EmptyEpochPublishRefused) {
  // A manifest whose current epoch is empty: publishing would create the
  // malformed key "smallest_new_file_number-".
  cfs_->TEST_InitEmptyCloudManifest();
  FileNumberGuardPublisher pub(cfs_.get(), std::chrono::seconds(30),
                               std::chrono::seconds(15));

  pub.OnJobBegin(10, 1, 1);
  ASSERT_TRUE(pub.ProtectFileUpload(10).IsInvalidArgument());
  ASSERT_TRUE(pub.BlockPurger().IsInvalidArgument());
  pub.PeriodicPublish();
  ASSERT_EQ(provider_->PutCount(), 0);
}

TEST_F(FileNumberGuardTest, BlockPurgerOnFileSystemWithoutPublisher) {
  LoadManifestWithEpoch("epoch1");
  // No publisher registered: BlockPurger takes the direct one-shot path.
  ASSERT_OK(cfs_->BlockPurger());
  ASSERT_EQ(provider_->Content(GuardKey()), "0");
}

TEST_F(FileNumberGuardTest, StartStopSchedulerSmoke) {
  LoadManifestWithEpoch("epoch1");
  auto pub = std::make_shared<FileNumberGuardPublisher>(
      cfs_.get(), std::chrono::milliseconds(20),
      std::chrono::milliseconds(10));
  pub->Start();
  pub->OnJobBegin(7, 1, 1);
  ASSERT_OK(pub->ProtectFileUpload(7));
  pub->OnJobEnd(1, 1);
  // Let the recurring job run at least once (idle -> MAX eventually).
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  pub->Stop();
  int puts_at_stop = provider_->PutCount();
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  // No publishes after Stop.
  ASSERT_EQ(provider_->PutCount(), puts_at_stop);
}

TEST_F(FileNumberGuardTest, SstUploadWithoutPublisherPassesThrough) {
  const std::string local_path = tmp_dir_ + "/000123.sst-epoch1";
  const std::string object_path = "dbpath/000123.sst-epoch1";
  CloudStorageWritableFileImpl file(cfs_.get(), local_path, "guard-bucket",
                                    object_path, FileOptions());
  ASSERT_OK(file.status());
  ASSERT_OK(file.Append(Slice("sst contents"), IOOptions(), nullptr));

  ASSERT_OK(file.Close(IOOptions(), nullptr));
  ASSERT_TRUE(provider_->HasObject(object_path));
}

TEST_F(FileNumberGuardTest, StoppedPublisherRejectsSstUpload) {
  LoadManifestWithEpoch("epoch1");
  auto pub = std::make_shared<FileNumberGuardPublisher>(
      cfs_.get(), std::chrono::seconds(30), std::chrono::seconds(15));
  pub->OnJobBegin(123, 1, 1);
  cfs_->SetFileNumberGuardPublisher(pub);
  pub->Stop();

  const std::string local_path = tmp_dir_ + "/000123.sst-epoch1";
  const std::string object_path = "dbpath/000123.sst-epoch1";
  CloudStorageWritableFileImpl file(cfs_.get(), local_path, "guard-bucket",
                                    object_path, FileOptions());
  ASSERT_OK(file.status());
  ASSERT_OK(file.Append(Slice("sst contents"), IOOptions(), nullptr));

  ASSERT_NOK(file.Close(IOOptions(), nullptr));
  ASSERT_FALSE(provider_->HasObject(object_path));
}

TEST_F(FileNumberGuardTest, StopDuringGuardPublishPreventsSstUpload) {
  LoadManifestWithEpoch("epoch1");
  auto pub = std::make_shared<FileNumberGuardPublisher>(
      cfs_.get(), std::chrono::seconds(30), std::chrono::seconds(15));
  pub->OnJobBegin(123, 1, 1);
  cfs_->SetFileNumberGuardPublisher(pub);

  std::mutex mutex;
  std::condition_variable cv;
  bool guard_put_reached = false;
  bool release_guard_put = false;
  bool guard_put_wait_timed_out = false;
  bool stop_reached = false;
  SyncPoint::GetInstance()->SetCallBack(
      "FileNumberGuard::PutSmallestFileNumberObject:Status", [&](void *) {
        std::unique_lock<std::mutex> lock(mutex);
        guard_put_reached = true;
        cv.notify_all();
        guard_put_wait_timed_out = !cv.wait_for(
            lock, kAsyncWaitTimeout, [&] { return release_guard_put; });
      });
  SyncPoint::GetInstance()->SetCallBack(
      "FileNumberGuardPublisher::Stop:Stopped", [&](void *) {
        std::lock_guard<std::mutex> lock(mutex);
        stop_reached = true;
        cv.notify_all();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  const std::string local_path = tmp_dir_ + "/000123.sst-epoch1";
  const std::string object_path = "dbpath/000123.sst-epoch1";
  CloudStorageWritableFileImpl file(cfs_.get(), local_path, "guard-bucket",
                                    object_path, FileOptions());
  ASSERT_OK(file.status());
  ASSERT_OK(file.Append(Slice("sst contents"), IOOptions(), nullptr));

  auto close = std::async(std::launch::async,
                          [&] { return file.Close(IOOptions(), nullptr); });
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, kAsyncWaitTimeout,
                            [&] { return guard_put_reached; }));
  }
  auto stop = std::async(std::launch::async, [&] { pub->Stop(); });
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, kAsyncWaitTimeout,
                            [&] { return stop_reached; }));
  }
  ASSERT_EQ(stop.wait_for(std::chrono::milliseconds(0)),
            std::future_status::timeout);

  {
    std::lock_guard<std::mutex> lock(mutex);
    release_guard_put = true;
  }
  cv.notify_all();
  const IOStatus close_status = close.get();
  stop.get();

  ASSERT_FALSE(guard_put_wait_timed_out);
  ASSERT_NOK(close_status);
  ASSERT_FALSE(provider_->HasObject(object_path));
}

TEST_F(FileNumberGuardTest, StopWaitsForProtectedSstUpload) {
  LoadManifestWithEpoch("epoch1");
  auto pub = std::make_shared<FileNumberGuardPublisher>(
      cfs_.get(), std::chrono::seconds(30), std::chrono::seconds(15));
  pub->OnJobBegin(123, 1, 1);
  cfs_->SetFileNumberGuardPublisher(pub);

  const std::string local_path = tmp_dir_ + "/000123.sst-epoch1";
  const std::string object_path = "dbpath/000123.sst-epoch1";
  std::mutex mutex;
  std::condition_variable cv;
  bool upload_reached = false;
  bool release_upload = false;
  bool upload_wait_timed_out = false;
  bool stop_reached = false;
  SyncPoint::GetInstance()->SetCallBack(
      "FileNumberGuardTest::PutCloudObject", [&](void *arg) {
        if (*static_cast<std::string *>(arg) != object_path) {
          return;
        }
        std::unique_lock<std::mutex> lock(mutex);
        upload_reached = true;
        cv.notify_all();
        upload_wait_timed_out = !cv.wait_for(
            lock, kAsyncWaitTimeout, [&] { return release_upload; });
      });
  SyncPoint::GetInstance()->SetCallBack(
      "FileNumberGuardPublisher::Stop:Stopped", [&](void *) {
        std::lock_guard<std::mutex> lock(mutex);
        stop_reached = true;
        cv.notify_all();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  CloudStorageWritableFileImpl file(cfs_.get(), local_path, "guard-bucket",
                                    object_path, FileOptions());
  ASSERT_OK(file.status());
  ASSERT_OK(file.Append(Slice("sst contents"), IOOptions(), nullptr));

  auto close = std::async(std::launch::async,
                          [&] { return file.Close(IOOptions(), nullptr); });
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, kAsyncWaitTimeout,
                            [&] { return upload_reached; }));
  }
  auto stop = std::async(std::launch::async, [&] { pub->Stop(); });
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, kAsyncWaitTimeout,
                            [&] { return stop_reached; }));
  }
  ASSERT_EQ(stop.wait_for(std::chrono::milliseconds(0)),
            std::future_status::timeout);

  {
    std::lock_guard<std::mutex> lock(mutex);
    release_upload = true;
  }
  cv.notify_all();
  ASSERT_OK(close.get());
  stop.get();

  ASSERT_FALSE(upload_wait_timed_out);
  ASSERT_TRUE(provider_->HasObject(object_path));
}

TEST_F(FileNumberGuardTest, GuardEnabledIdentityUploadPassesThrough) {
  LoadManifestWithEpoch("epoch1");
  auto pub = std::make_shared<FileNumberGuardPublisher>(
      cfs_.get(), std::chrono::seconds(30), std::chrono::seconds(15));
  cfs_->SetFileNumberGuardPublisher(pub);

  const std::string local_path = tmp_dir_ + "/IDENTITY";
  const std::string object_path = "dbpath/IDENTITY";
  CloudStorageWritableFileImpl file(cfs_.get(), local_path, "guard-bucket",
                                    object_path, FileOptions());
  ASSERT_OK(file.status());
  ASSERT_OK(file.Append(Slice("db-id"), IOOptions(), nullptr));

  ASSERT_OK(file.Close(IOOptions(), nullptr));
  ASSERT_TRUE(provider_->HasObject(object_path));
  ASSERT_EQ(provider_->PutCount(), 1);
}

TEST_F(FileNumberGuardTest, MalformedEpochSstUploadFailsClosed) {
  LoadManifestWithEpoch("epoch1");
  auto pub = std::make_shared<FileNumberGuardPublisher>(
      cfs_.get(), std::chrono::seconds(30), std::chrono::seconds(15));
  cfs_->SetFileNumberGuardPublisher(pub);

  const std::string local_path = tmp_dir_ + "/not-a-number.sst-epoch1";
  const std::string object_path = "dbpath/not-a-number.sst-epoch1";
  CloudStorageWritableFileImpl file(cfs_.get(), local_path, "guard-bucket",
                                    object_path, FileOptions());
  ASSERT_OK(file.status());
  ASSERT_OK(file.Append(Slice("sst contents"), IOOptions(), nullptr));

  const IOStatus status = file.Close(IOOptions(), nullptr);
  ASSERT_TRUE(status.IsInvalidArgument()) << status.ToString();
  ASSERT_FALSE(provider_->HasObject(object_path));
  ASSERT_EQ(provider_->PutCount(), 0);
}

TEST_F(FileNumberGuardTest, ProtectedSstUploadFailurePropagates) {
  LoadManifestWithEpoch("epoch1");
  auto pub = std::make_shared<FileNumberGuardPublisher>(
      cfs_.get(), std::chrono::seconds(30), std::chrono::seconds(15));
  pub->OnJobBegin(123, 1, 1);
  cfs_->SetFileNumberGuardPublisher(pub);

  const std::string local_path = tmp_dir_ + "/000123.sst-epoch1";
  const std::string object_path = "dbpath/000123.sst-epoch1";
  provider_->SetPutStatus(object_path,
                          IOStatus::IOError("injected SST upload failure"));
  CloudStorageWritableFileImpl file(cfs_.get(), local_path, "guard-bucket",
                                    object_path, FileOptions());
  ASSERT_OK(file.status());
  ASSERT_OK(file.Append(Slice("sst contents"), IOOptions(), nullptr));

  const IOStatus status = file.Close(IOOptions(), nullptr);
  ASSERT_TRUE(status.IsIOError()) << status.ToString();
  ASSERT_FALSE(provider_->HasObject(object_path));
  ASSERT_EQ(provider_->Content(GuardKey()), "123");
}

}  //  namespace ROCKSDB_NAMESPACE

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
