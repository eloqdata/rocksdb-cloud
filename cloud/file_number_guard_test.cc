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
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "cloud/cloud_manifest.h"
#include "file/writable_file_writer.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/env.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

namespace {

constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();

// Records every PutCloudObject; everything else is unsupported. The
// publisher only ever needs PUT.
class RecordingStorageProvider : public CloudStorageProvider {
 public:
  const char *Name() const override { return "recording"; }

  IOStatus PutCloudObject(const std::string &local_path,
                          const std::string & /*bucket_name*/,
                          const std::string &object_path) override {
    std::ifstream f(local_path);
    std::string content((std::istreambuf_iterator<char>(f)),
                        std::istreambuf_iterator<char>());
    std::lock_guard<std::mutex> lk(mu_);
    ++put_count_;
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

TEST_F(FileNumberGuardTest, SentinelDoesNotAdvanceWatermark) {
  LoadManifestWithEpoch("epoch1");
  FileNumberGuardPublisher pub(cfs_.get(), std::chrono::seconds(30),
                               std::chrono::seconds(15));

  ASSERT_OK(pub.BlockPurger());
  ASSERT_EQ(provider_->Content(GuardKey()), "0");
  // The sentinel must not advance the watermark: a later real value
  // (necessarily > 0) still has to be published.
  ASSERT_EQ(pub.TEST_LastPublished(), kMax);

  // First job after the sentinel publishes its (smaller-than-MAX) snapshot.
  ASSERT_OK(pub.OnJobBegin(42, 1, 1));
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

  ASSERT_OK(pub.OnJobBegin(100, 1, 1));
  int puts_after_first = provider_->PutCount();
  ASSERT_EQ(provider_->Content(GuardKey()), "100");

  // A job at or above the watermark publishes nothing.
  ASSERT_OK(pub.OnJobBegin(200, 2, 2));
  ASSERT_OK(pub.OnJobBegin(100, 3, 3));
  ASSERT_EQ(provider_->PutCount(), puts_after_first);

  // A job below the watermark publishes synchronously.
  ASSERT_OK(pub.OnJobBegin(50, 4, 4));
  ASSERT_EQ(provider_->Content(GuardKey()), "50");
  ASSERT_EQ(pub.TEST_LastPublished(), 50u);
}

TEST_F(FileNumberGuardTest, PeriodicPublishesWindowMinThenMaxWhenIdle) {
  LoadManifestWithEpoch("epoch1");
  // Short entry duration so completed entries expire quickly.
  FileNumberGuardPublisher pub(cfs_.get(), std::chrono::seconds(30),
                               std::chrono::milliseconds(50));

  ASSERT_OK(pub.OnJobBegin(100, 1, 1));  // publishes 100 downward
  ASSERT_OK(pub.OnJobBegin(200, 2, 2));  // no publish

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
  std::thread job([&] {
    Status s = pub.OnJobBegin(10, 1, 1);
    ASSERT_OK(s);
  });

  // While failures are being injected, the watermark must not advance.
  while (failures_left.load() > 0) {
    ASSERT_EQ(pub.TEST_LastPublished(), kMax);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  job.join();

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
  std::thread job([&] {
    job_status = pub.OnJobBegin(10, 1, 1);
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

  ASSERT_TRUE(pub.OnJobBegin(10, 1, 1).IsInvalidArgument());
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
  ASSERT_OK(pub->OnJobBegin(7, 1, 1));
  pub->OnJobEnd(1, 1);
  // Let the recurring job run at least once (idle -> MAX eventually).
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  pub->Stop();
  int puts_at_stop = provider_->PutCount();
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  // No publishes after Stop.
  ASSERT_EQ(provider_->PutCount(), puts_at_stop);
}

}  //  namespace ROCKSDB_NAMESPACE

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
