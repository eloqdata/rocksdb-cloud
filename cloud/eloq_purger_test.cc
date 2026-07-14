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

#include "cloud/eloq_purger.h"

#include <algorithm>
#include <cstdarg>
#include <cstdio>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cloud/cloud_manifest.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

namespace {

constexpr uint64_t kHourMs = 3600ULL * 1000;
// The S3-derived clock reading for all tests. Absolute value is arbitrary;
// only differences against object mtimes matter.
constexpr uint64_t kNow = 1000ULL * kHourMs;

CloudObjectInformation MakeInfo(uint64_t mtime) {
  CloudObjectInformation info;
  info.size = 0;
  info.modification_time = mtime;
  return info;
}

class RecordingPurgerStorageProvider : public CloudStorageProvider {
 public:
  using PurgerAllFiles = EloqPurger::PurgerAllFiles;

  explicit RecordingPurgerStorageProvider(PurgerAllFiles files)
      : files_(std::move(files)) {}

  const char *Name() const override { return "recording-purger"; }

  void SetDeleteStatus(const std::string &path, IOStatus status) {
    delete_statuses_[path] = std::move(status);
  }

  const std::vector<std::string> &delete_attempts() const {
    return delete_attempts_;
  }

  IOStatus DeleteCloudObject(const std::string & /*bucket_name*/,
                             const std::string &object_path) override {
    if (clock_objects_.erase(object_path) != 0) {
      return IOStatus::OK();
    }

    delete_attempts_.push_back(object_path);
    auto status_it = delete_statuses_.find(object_path);
    IOStatus status = status_it == delete_statuses_.end() ? IOStatus::OK()
                                                          : status_it->second;
    if (status.ok() || status.IsNotFound()) {
      const std::string prefix = object_path_ + "/";
      const std::string relative_path =
          object_path.compare(0, prefix.size(), prefix) == 0
              ? object_path.substr(prefix.size())
              : object_path;
      files_.erase(std::remove_if(files_.begin(), files_.end(),
                                  [&](const auto &file) {
                                    return file.first == relative_path;
                                  }),
                   files_.end());
    }
    return status;
  }

  IOStatus ListCloudObjects(const std::string & /*bucket_name*/,
                            const std::string &object_path,
                            PurgerAllFiles *files) override {
    object_path_ = object_path;
    *files = files_;
    return IOStatus::OK();
  }

  IOStatus ListCloudObjectsWithPrefix(
      const std::string & /*bucket_name*/, const std::string & /*object_path*/,
      const std::string &object_prefix,
      std::vector<std::string> *paths) override {
    for (const auto &file : files_) {
      if (file.first.compare(0, object_prefix.size(), object_prefix) == 0) {
        paths->push_back(file.first);
      }
    }
    return IOStatus::OK();
  }

  IOStatus PutCloudObject(const std::string & /*local_path*/,
                          const std::string & /*bucket_name*/,
                          const std::string &object_path) override {
    clock_objects_.insert(object_path);
    return IOStatus::OK();
  }

  IOStatus GetCloudObjectModificationTime(const std::string & /*bucket_name*/,
                                          const std::string & /*object_path*/,
                                          uint64_t * /*time*/) override {
    return NotSupported();
  }

  IOStatus GetCloudObjectMetadata(const std::string & /*bucket_name*/,
                                  const std::string &object_path,
                                  CloudObjectInformation *info) override {
    if (clock_objects_.find(object_path) == clock_objects_.end()) {
      return IOStatus::NotFound();
    }
    *info = MakeInfo(kNow);
    return IOStatus::OK();
  }

  IOStatus CreateBucket(const std::string &) override { return NotSupported(); }
  IOStatus ExistsBucket(const std::string &) override { return NotSupported(); }
  IOStatus EmptyBucket(const std::string &, const std::string &) override {
    return NotSupported();
  }
  IOStatus ListCloudObjects(const std::string &, const std::string &,
                            std::vector<std::string> *) override {
    return NotSupported();
  }
  IOStatus ExistsCloudObject(const std::string &,
                             const std::string &) override {
    return NotSupported();
  }
  IOStatus GetCloudObjectSize(const std::string &, const std::string &,
                              uint64_t *) override {
    return NotSupported();
  }
  IOStatus CopyCloudObject(const std::string &, const std::string &,
                           const std::string &, const std::string &) override {
    return NotSupported();
  }
  IOStatus GetCloudObject(const std::string &, const std::string &,
                          const std::string &) override {
    return NotSupported();
  }
  IOStatus PutCloudObjectMetadata(
      const std::string &, const std::string &,
      const std::unordered_map<std::string, std::string> &) override {
    return NotSupported();
  }
  IOStatus NewCloudWritableFile(const std::string &, const std::string &,
                                const std::string &, const FileOptions &,
                                std::unique_ptr<CloudStorageWritableFile> *,
                                IODebugContext *) override {
    return NotSupported();
  }
  IOStatus NewCloudReadableFile(const std::string &, const std::string &,
                                const FileOptions &,
                                std::unique_ptr<CloudStorageReadableFile> *,
                                IODebugContext *) override {
    return NotSupported();
  }

 private:
  static IOStatus NotSupported() {
    return IOStatus::NotSupported("RecordingPurgerStorageProvider");
  }

  PurgerAllFiles files_;
  std::string object_path_;
  std::unordered_map<std::string, IOStatus> delete_statuses_;
  std::unordered_set<std::string> clock_objects_;
  std::vector<std::string> delete_attempts_;
};

class RecordingLogger : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char *format, va_list ap) override {
    char buffer[2048];
    vsnprintf(buffer, sizeof(buffer), format, ap);
    log_.append(buffer).push_back('\n');
  }

  const std::string &log() const { return log_; }

 private:
  std::string log_;
};

std::unique_ptr<CloudFileSystemImpl> MakeCloudFileSystem(
    const std::shared_ptr<CloudStorageProvider> &provider,
    const std::shared_ptr<Logger> &logger = nullptr) {
  CloudFileSystemOptions opts;
  opts.storage_provider = provider;
  opts.dest_bucket.SetBucketName("test-bucket");
  opts.dest_bucket.SetObjectPath("dbpath");
  opts.cloud_file_deletion_delay = std::nullopt;
  return std::make_unique<CloudFileSystemImpl>(opts, FileSystem::Default(),
                                               logger);
}

}  // namespace

class EloqPurgerTest : public testing::Test {
 public:
  EloqPurgerTest()
      : cfs_(TestOptions(), FileSystem::Default(), nullptr /*logger*/),
        purger_(&cfs_, "test-bucket", "dbpath", true /*dry_run*/,
                kHourMs /*cloudmanifest_retention_ms*/,
                kHourMs /*dead_epoch_file_age_ms*/,
                10000 /*max_deletions_per_cycle*/) {}

 protected:
  static CloudFileSystemOptions TestOptions() {
    CloudFileSystemOptions opts;
    // Keep the constructor from spinning up the file-deletion scheduler.
    opts.cloud_file_deletion_delay = std::nullopt;
    return opts;
  }

  CloudFileSystemImpl cfs_;
  EloqPurger purger_;
};

// A living epoch (one with a threshold entry) must keep the pre-existing
// watermark semantics: non-live files below the watermark are deleted, files
// at or above it are spared regardless of age, live files are untouchable.
TEST_F(EloqPurgerTest, LivingEpochThresholdSemanticsUnchanged) {
  EloqPurger::PurgerFileNumberThresholds thresholds{{"epochA", 100}};
  EloqPurger::PurgerLiveFileSet live{"000042.sst-epochA"};
  EloqPurger::PurgerAllFiles all_files{
      // Live: kept even though it is below the watermark and old.
      {"000042.sst-epochA", MakeInfo(kNow - 10 * kHourMs)},
      // Non-live, below watermark: deleted even though it is brand new --
      // the watermark, not age, is the guard for living epochs.
      {"000050.sst-epochA", MakeInfo(kNow)},
      // Non-live, at/above watermark: possibly still in flight, kept.
      {"000150.sst-epochA", MakeInfo(kNow - 10 * kHourMs)},
      // Not an SST: ignored by this selector.
      {"MANIFEST-epochA", MakeInfo(kNow - 10 * kHourMs)},
  };

  std::vector<std::string> obsolete;
  purger_.SelectObsoleteSSTFilesWithThreshold(all_files, live, thresholds,
                                              kNow, &obsolete);
  ASSERT_EQ(obsolete, std::vector<std::string>{"000050.sst-epochA"});
}

// A published watermark of UINT64_MIN means "unknown": nothing in that epoch
// may be deleted.
TEST_F(EloqPurgerTest, UnknownThresholdBlocksLivingEpoch) {
  EloqPurger::PurgerFileNumberThresholds thresholds{
      {"epochA", std::numeric_limits<uint64_t>::min()}};
  EloqPurger::PurgerAllFiles all_files{
      {"000050.sst-epochA", MakeInfo(kNow - 10 * kHourMs)},
  };

  std::vector<std::string> obsolete;
  purger_.SelectObsoleteSSTFilesWithThreshold(all_files, {}, thresholds, kNow,
                                              &obsolete);
  ASSERT_TRUE(obsolete.empty());
}

// Regression test for the dead-epoch leak: a non-live SST whose epoch is no
// loaded CLOUDMANIFEST's current epoch used to be blocked forever; it must
// now be reclaimed once older than the age guard, while young files (a node
// mid-open) and live files stay protected.
TEST_F(EloqPurgerTest, DeadEpochSstReclaimedOnceOldEnough) {
  EloqPurger::PurgerFileNumberThresholds thresholds{{"epochA", 100}};
  EloqPurger::PurgerLiveFileSet live{"000010.sst-epochDead"};
  EloqPurger::PurgerAllFiles all_files{
      // Dead epoch, older than the age guard: reclaimed.
      {"000200.sst-epochDead", MakeInfo(kNow - 2 * kHourMs)},
      // Dead epoch but younger than the age guard: kept (mid-open race).
      {"000201.sst-epochDead", MakeInfo(kNow - kHourMs + 1)},
      // Live files are protected regardless of epoch.
      {"000010.sst-epochDead", MakeInfo(kNow - 2 * kHourMs)},
  };

  std::vector<std::string> obsolete;
  purger_.SelectObsoleteSSTFilesWithThreshold(all_files, live, thresholds,
                                              kNow, &obsolete);
  ASSERT_EQ(obsolete, std::vector<std::string>{"000200.sst-epochDead"});
}

// MANIFEST selection: current-epoch manifests are untouchable, dead-epoch
// manifests are reclaimed only once old enough. The young-manifest case is
// the open-sequencing race: MANIFEST-<epoch> is uploaded BEFORE the
// CLOUDMANIFEST that makes the epoch current.
TEST_F(EloqPurgerTest, ManifestAgeGuard) {
  EloqPurger::PurgerEpochManifestMap current_epochs{
      {"epochA", MakeInfo(kNow)}};
  EloqPurger::PurgerAllFiles all_files{
      // Current epoch: kept no matter how old.
      {"MANIFEST-epochA", MakeInfo(kNow - 10 * kHourMs)},
      // Dead epoch, old: reclaimed.
      {"MANIFEST-epochDead", MakeInfo(kNow - 2 * kHourMs)},
      // Unknown epoch but freshly uploaded: a node mid-open, kept.
      {"MANIFEST-epochOpening", MakeInfo(kNow - kHourMs / 2)},
      // Not a manifest: ignored by this selector.
      {"000200.sst-epochDead", MakeInfo(kNow - 2 * kHourMs)},
  };

  std::vector<std::string> obsolete;
  purger_.SelectObsoleteManifestFiles(all_files, current_epochs, kNow,
                                      &obsolete);
  ASSERT_EQ(obsolete, std::vector<std::string>{"MANIFEST-epochDead"});
}

// Regression test for the read-only-zombie bug: retention must be measured
// from the moment the old term was superseded (the successor CLOUDMANIFEST's
// own mtime), not from the old generation's MANIFEST mtime. A generation that
// was write-idle for a day before failover must still get the full grace
// window after failover.
TEST_F(EloqPurgerTest, SupersededCloudManifestKeptUntilSuccessorAges) {
  EloqPurger::PurgerCloudManifestMap cloudmanifests;
  std::unique_ptr<CloudManifest> old_cm;
  std::unique_ptr<CloudManifest> new_cm;
  ASSERT_OK(CloudManifest::CreateForEmptyDatabase("epochOld", &old_cm));
  ASSERT_OK(CloudManifest::CreateForEmptyDatabase("epochNew", &new_cm));
  cloudmanifests["CLOUDMANIFEST-db-1"] = std::move(old_cm);
  cloudmanifests["CLOUDMANIFEST-db-2"] = std::move(new_cm);

  EloqPurger::PurgerEpochManifestMap current_epochs{
      // The old generation served read-only traffic: last write a day ago.
      {"epochOld", MakeInfo(kNow - 24 * kHourMs)},
      {"epochNew", MakeInfo(kNow)},
  };

  // Successor born 30 minutes ago: within retention, keep the old term even
  // though its MANIFEST heartbeat has been silent for a day.
  {
    EloqPurger::PurgerAllFiles all_files{
        {"CLOUDMANIFEST-db-1", MakeInfo(kNow - 25 * kHourMs)},
        {"CLOUDMANIFEST-db-2", MakeInfo(kNow - kHourMs / 2)},
    };
    std::vector<std::string> obsolete;
    purger_.SelectObsoleteCloudManifestFiles(all_files, cloudmanifests,
                                             current_epochs, kNow, &obsolete);
    ASSERT_TRUE(obsolete.empty());
  }

  // Successor born two hours ago: retention expired, the old term goes and
  // the max term stays.
  {
    EloqPurger::PurgerAllFiles all_files{
        {"CLOUDMANIFEST-db-1", MakeInfo(kNow - 25 * kHourMs)},
        {"CLOUDMANIFEST-db-2", MakeInfo(kNow - 2 * kHourMs)},
    };
    std::vector<std::string> obsolete;
    purger_.SelectObsoleteCloudManifestFiles(all_files, cloudmanifests,
                                             current_epochs, kNow, &obsolete);
    ASSERT_EQ(obsolete, std::vector<std::string>{"CLOUDMANIFEST-db-1"});
  }
}

// smallest_new_file_number markers: kept while their epoch is living or the
// marker is young (a node mid-open may publish the marker before its
// CLOUDMANIFEST lands), reclaimed once the epoch is dead and the marker old.
TEST_F(EloqPurgerTest, DeadEpochMarkersReclaimed) {
  EloqPurger::PurgerFileNumberThresholds thresholds{{"epochA", 100}};
  EloqPurger::PurgerAllFiles all_files{
      // Living epoch: marker in use, kept.
      {"smallest_new_file_number-epochA", MakeInfo(kNow - 10 * kHourMs)},
      // Dead epoch, old: reclaimed.
      {"smallest_new_file_number-epochDead", MakeInfo(kNow - 2 * kHourMs)},
      // Unknown epoch but young: kept.
      {"smallest_new_file_number-epochOpening", MakeInfo(kNow - kHourMs / 2)},
      // Not a marker: ignored by this selector.
      {"000050.sst-epochDead", MakeInfo(kNow - 2 * kHourMs)},
  };

  std::vector<std::string> obsolete;
  purger_.SelectObsoleteFileNumberMarkers(all_files, thresholds, kNow,
                                          &obsolete);
  ASSERT_EQ(obsolete,
            std::vector<std::string>{"smallest_new_file_number-epochDead"});
}

TEST_F(EloqPurgerTest, FutureTimestampsAreRetainedByEveryAgeGuard) {
  EloqPurger::PurgerCloudManifestMap cloudmanifests;
  std::unique_ptr<CloudManifest> old_cm;
  std::unique_ptr<CloudManifest> new_cm;
  ASSERT_OK(CloudManifest::CreateForEmptyDatabase("epochOld", &old_cm));
  ASSERT_OK(CloudManifest::CreateForEmptyDatabase("epochNew", &new_cm));
  cloudmanifests["CLOUDMANIFEST-db-1"] = std::move(old_cm);
  cloudmanifests["CLOUDMANIFEST-db-2"] = std::move(new_cm);

  EloqPurger::PurgerAllFiles all_files{
      {"000001.sst-epochDead", MakeInfo(kNow + 1)},
      {"MANIFEST-epochDead", MakeInfo(kNow + 1)},
      {"CLOUDMANIFEST-db-1", MakeInfo(kNow - 2 * kHourMs)},
      {"CLOUDMANIFEST-db-2", MakeInfo(kNow + 1)},
      {"smallest_new_file_number-epochDead", MakeInfo(kNow + 1)},
  };
  EloqPurger::PurgerEpochManifestMap current_epochs{
      {"epochOld", MakeInfo(kNow)}, {"epochNew", MakeInfo(kNow)}};

  std::vector<std::string> obsolete;
  purger_.SelectObsoleteSSTFilesWithThreshold(all_files, {}, {}, kNow,
                                              &obsolete);
  purger_.SelectObsoleteManifestFiles(all_files, current_epochs, kNow,
                                      &obsolete);
  purger_.SelectObsoleteCloudManifestFiles(all_files, cloudmanifests,
                                           current_epochs, kNow, &obsolete);
  purger_.SelectObsoleteFileNumberMarkers(all_files, {}, kNow, &obsolete);
  ASSERT_TRUE(obsolete.empty());
}

TEST_F(EloqPurgerTest, ZeroThresholdBlocksLivingEpochSstDeletion) {
  EloqPurger::PurgerAllFiles all_files{
      {"000001.sst-epochA", MakeInfo(kNow - 10 * kHourMs)},
      {"000002.sst-epochA", MakeInfo(kNow - 10 * kHourMs)},
  };

  std::vector<std::string> obsolete;
  purger_.SelectObsoleteSSTFilesWithThreshold(all_files, {}, {{"epochA", 0}},
                                              kNow, &obsolete);
  ASSERT_TRUE(obsolete.empty());
}

TEST(EloqPurgerCycleTest, NotFoundDeletionCountsAsSuccess) {
  const std::string file = "000001.sst-epochDead";
  auto provider = std::make_shared<RecordingPurgerStorageProvider>(
      EloqPurger::PurgerAllFiles{{file, MakeInfo(kNow - 2 * kHourMs)}});
  provider->SetDeleteStatus("dbpath/" + file,
                            IOStatus::NotFound("already deleted"));
  auto cfs = MakeCloudFileSystem(provider);
  EloqPurger purger(cfs.get(), "test-bucket", "dbpath", false /*dry_run*/,
                    kHourMs, kHourMs, 10000);

  ASSERT_TRUE(purger.RunSinglePurgeCycle());
  ASSERT_EQ(provider->delete_attempts(),
            std::vector<std::string>{"dbpath/" + file});
}

TEST(EloqPurgerCycleTest, DeletionCapConsumesDeterministicPrefixThenConverges) {
  auto provider = std::make_shared<RecordingPurgerStorageProvider>(
      EloqPurger::PurgerAllFiles{
          {"smallest_new_file_number-epochDead", MakeInfo(kNow - 2 * kHourMs)},
          {"000001.sst-epochDead", MakeInfo(kNow - 2 * kHourMs)},
          {"MANIFEST-epochDead", MakeInfo(kNow - 2 * kHourMs)},
          {"000002.sst-epochDead", MakeInfo(kNow - 2 * kHourMs)},
      });
  auto cfs = MakeCloudFileSystem(provider);
  EloqPurger purger(cfs.get(), "test-bucket", "dbpath", false /*dry_run*/,
                    kHourMs, kHourMs, 2 /*max_deletions_per_cycle*/);

  ASSERT_TRUE(purger.RunSinglePurgeCycle());
  ASSERT_EQ(provider->delete_attempts(),
            std::vector<std::string>({"dbpath/000001.sst-epochDead",
                                      "dbpath/000002.sst-epochDead"}));

  ASSERT_TRUE(purger.RunSinglePurgeCycle());
  ASSERT_EQ(provider->delete_attempts(),
            std::vector<std::string>(
                {"dbpath/000001.sst-epochDead", "dbpath/000002.sst-epochDead",
                 "dbpath/MANIFEST-epochDead",
                 "dbpath/smallest_new_file_number-epochDead"}));
}

TEST(EloqPurgerCycleTest, BulkDeleteFailureFailsCycle) {
  const std::string file = "000001.sst-epochDead";
  auto provider = std::make_shared<RecordingPurgerStorageProvider>(
      EloqPurger::PurgerAllFiles{{file, MakeInfo(kNow - 2 * kHourMs)}});
  provider->SetDeleteStatus("dbpath/" + file,
                            IOStatus::IOError("injected delete failure"));
  auto logger = std::make_shared<RecordingLogger>();
  auto cfs = MakeCloudFileSystem(provider, logger);
  EloqPurger purger(cfs.get(), "test-bucket", "dbpath", false /*dry_run*/,
                    kHourMs, kHourMs, 10000);

  ASSERT_FALSE(purger.RunSinglePurgeCycle());
  ASSERT_EQ(provider->delete_attempts(),
            std::vector<std::string>{"dbpath/" + file});
  ASSERT_NE(logger->log().find(
                "obsolete_selected=1 deleted=0 failed=1"),
            std::string::npos);
}

}  //  namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
