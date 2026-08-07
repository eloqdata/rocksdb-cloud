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
#include <cstring>
#include <fstream>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cloud/cloud_manifest.h"
#include "cloud/manifest_reader.h"
#include "db/log_writer.h"
#include "db/version_edit.h"
#include "file/writable_file_writer.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/cloud/cloud_storage_provider_impl.h"
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

class StringCloudReadableFile : public CloudStorageReadableFileImpl {
public:
  StringCloudReadableFile(const std::string &name, std::string contents)
      : CloudStorageReadableFileImpl(nullptr, "test-bucket", name,
                                     contents.size()),
        contents_(std::move(contents)) {}

protected:
  IOStatus DoCloudRead(uint64_t offset, size_t n, const IOOptions &,
                       char *scratch, uint64_t *bytes_read,
                       IODebugContext *) const override {
    const size_t available = contents_.size() - static_cast<size_t>(offset);
    const size_t to_read = std::min(n, available);
    std::memcpy(scratch, contents_.data() + offset, to_read);
    *bytes_read = to_read;
    return IOStatus::OK();
  }

private:
  std::string contents_;
};

class RecordingPurgerStorageProvider : public CloudStorageProvider {
 public:
  using PurgerAllFiles = EloqPurger::PurgerAllFiles;

  explicit RecordingPurgerStorageProvider(PurgerAllFiles files)
      : files_(std::move(files)) {}

  const char *Name() const override { return "recording-purger"; }

  void SetDeleteStatus(const std::string &path, IOStatus status) {
    delete_statuses_[path] = std::move(status);
  }

  void SetGetStatus(const std::string &path, IOStatus status) {
    get_statuses_[path] = std::move(status);
  }

  void SetObjectContents(const std::string &path, std::string contents) {
    object_contents_[path] = std::move(contents);
  }

  void SetListAllStatus(IOStatus status) {
    list_all_status_ = std::move(status);
  }

  void SetListPrefixStatus(IOStatus status) {
    list_prefix_status_ = std::move(status);
  }

  void SetPutStatus(IOStatus status) { put_status_ = std::move(status); }

  void SetClockMetadataStatus(IOStatus status) {
    clock_metadata_status_ = std::move(status);
  }

  void SetMetadataStatus(const std::string &path, IOStatus status) {
    metadata_statuses_[path] = std::move(status);
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
    if (!list_all_status_.ok()) {
      return list_all_status_;
    }
    object_path_ = object_path;
    *files = files_;
    return IOStatus::OK();
  }

  IOStatus ListCloudObjectsWithPrefix(
      const std::string & /*bucket_name*/, const std::string & /*object_path*/,
      const std::string &object_prefix,
      std::vector<std::string> *paths) override {
    if (!list_prefix_status_.ok()) {
      return list_prefix_status_;
    }
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
    if (!put_status_.ok()) {
      return put_status_;
    }
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
    auto status = metadata_statuses_.find(object_path);
    if (status != metadata_statuses_.end()) {
      return status->second;
    }
    if (clock_objects_.find(object_path) != clock_objects_.end()) {
      if (!clock_metadata_status_.ok()) {
        return clock_metadata_status_;
      }
      *info = MakeInfo(kNow);
      return IOStatus::OK();
    }

    const std::string prefix = object_path_ + "/";
    const std::string relative_path =
        object_path.compare(0, prefix.size(), prefix) == 0
            ? object_path.substr(prefix.size())
            : object_path;
    for (const auto &file : files_) {
      if (file.first == relative_path) {
        *info = file.second;
        return IOStatus::OK();
      }
    }
    return IOStatus::NotFound(object_path);
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
  IOStatus GetCloudObject(const std::string &, const std::string &object_path,
                          const std::string &local_path) override {
    auto status = get_statuses_.find(object_path);
    if (status != get_statuses_.end()) {
      return status->second;
    }
    auto contents = object_contents_.find(object_path);
    if (contents == object_contents_.end()) {
      return NotSupported();
    }
    std::ofstream out(local_path, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
      return IOStatus::IOError(local_path);
    }
    out.write(contents->second.data(),
              static_cast<std::streamsize>(contents->second.size()));
    out.close();
    return IOStatus::OK();
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
  IOStatus
  NewCloudReadableFile(const std::string &, const std::string &object_path,
                       const FileOptions &,
                       std::unique_ptr<CloudStorageReadableFile> *result,
                       IODebugContext *) override {
    auto status = get_statuses_.find(object_path);
    if (status != get_statuses_.end()) {
      return status->second;
    }
    auto contents = object_contents_.find(object_path);
    if (contents == object_contents_.end()) {
      return IOStatus::NotFound(object_path);
    }
    result->reset(new StringCloudReadableFile(object_path, contents->second));
    return IOStatus::OK();
  }

 private:
  static IOStatus NotSupported() {
    return IOStatus::NotSupported("RecordingPurgerStorageProvider");
  }

  PurgerAllFiles files_;
  std::string object_path_;
  std::unordered_map<std::string, IOStatus> delete_statuses_;
  std::unordered_map<std::string, IOStatus> get_statuses_;
  std::unordered_map<std::string, IOStatus> metadata_statuses_;
  std::unordered_map<std::string, std::string> object_contents_;
  std::unordered_set<std::string> clock_objects_;
  std::vector<std::string> delete_attempts_;
  IOStatus list_all_status_;
  IOStatus list_prefix_status_;
  IOStatus put_status_;
  IOStatus clock_metadata_status_;
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

std::string ReadTestFileAndRemove(const std::string &path) {
  std::ifstream in(path, std::ios::binary);
  std::string contents((std::istreambuf_iterator<char>(in)),
                       std::istreambuf_iterator<char>());
  in.close();
  EXPECT_EQ(std::remove(path.c_str()), 0);
  return contents;
}

// Serialized CLOUDMANIFEST bytes. `epoch_transitions` uses the same exclusive
// file-number boundaries as CloudManifest::AddEpoch, allowing cycle tests to
// verify that live files from earlier epochs are remapped and preserved.
std::string MakeCloudManifestContents(
    const std::string &initial_epoch,
    const std::vector<std::pair<uint64_t, std::string>> &epoch_transitions =
        {}) {
  std::unique_ptr<CloudManifest> manifest;
  EXPECT_OK(CloudManifest::CreateForEmptyDatabase(initial_epoch, &manifest));
  for (const auto &transition : epoch_transitions) {
    EXPECT_TRUE(manifest->AddEpoch(transition.first, transition.second));
  }
  static uint64_t serial = 0;
  const std::string path = test::TmpDir() + "/purger_test_cloud_manifest_" +
                           std::to_string(Env::Default()->NowMicros()) + "_" +
                           std::to_string(++serial);
  std::unique_ptr<WritableFileWriter> writer;
  EXPECT_OK(WritableFileWriter::Create(FileSystem::Default(), path,
                                       FileOptions(), &writer, nullptr));
  EXPECT_OK(manifest->WriteToLog(std::move(writer)));
  return ReadTestFileAndRemove(path);
}

// Serialized RocksDB MANIFEST bytes containing one add record for every live
// file number. The purger reads these exact bytes through CloudFileSystemImpl,
// so cycle tests cover decoding and CloudManifest epoch remapping rather than
// injecting a precomputed live-file set.
std::string MakeManifestContents(const std::vector<uint64_t> &live_files) {
  static uint64_t serial = 0;
  const std::string path = test::TmpDir() + "/purger_test_manifest_" +
                           std::to_string(Env::Default()->NowMicros()) + "_" +
                           std::to_string(++serial);
  std::unique_ptr<WritableFileWriter> writer;
  EXPECT_OK(WritableFileWriter::Create(FileSystem::Default(), path,
                                       FileOptions(), &writer, nullptr));
  {
    log::Writer log_writer(std::move(writer), 0, false);
    for (uint64_t file_number : live_files) {
      VersionEdit edit;
      edit.AddFile(
          0 /*level*/, file_number, 0 /*path_id*/, 1 /*file_size*/,
          InternalKey("a", 1, kTypeValue), InternalKey("z", 1, kTypeValue),
          1 /*smallest_seqno*/, 1 /*largest_seqno*/,
          false /*marked_for_compaction*/, Temperature::kUnknown,
          kInvalidBlobFileNumber, kUnknownOldestAncesterTime,
          kUnknownFileCreationTime, file_number /*epoch_number*/,
          kUnknownFileChecksum, kUnknownFileChecksumFuncName, kNullUniqueId64x2,
          0 /*compensated_range_deletion_size*/, 0 /*tail_size*/,
          true /*user_defined_timestamps_persisted*/);
      std::string record;
      EXPECT_TRUE(edit.EncodeTo(&record, 0 /*timestamp_size*/));
      EXPECT_OK(log_writer.AddRecord(WriteOptions(), record));
    }
    EXPECT_OK(log_writer.file()->Sync(IOOptions(), false));
  }
  return ReadTestFileAndRemove(path);
}

std::shared_ptr<RecordingPurgerStorageProvider> MakeValidCycleProvider() {
  auto provider = std::make_shared<RecordingPurgerStorageProvider>(
      EloqPurger::PurgerAllFiles{
          {"CLOUDMANIFEST-db-1", MakeInfo(kNow - 2 * kHourMs)},
          {"MANIFEST-epochA", MakeInfo(kNow - 2 * kHourMs)},
          {"000001.sst-epochA", MakeInfo(kNow - 2 * kHourMs)},
          {"000002.sst-epochA", MakeInfo(kNow - 2 * kHourMs)},
          {"smallest_new_file_number-epochA",
           MakeInfo(kNow - 2 * kHourMs)},
      });
  provider->SetObjectContents("dbpath/CLOUDMANIFEST-db-1",
                              MakeCloudManifestContents("epochA"));
  provider->SetObjectContents("dbpath/MANIFEST-epochA",
                              MakeManifestContents({1}));
  provider->SetObjectContents("dbpath/smallest_new_file_number-epochA", "10");
  return provider;
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

TEST(EloqPurgerCycleTest, GuardReadIoErrorFailsClosed) {
  const std::string cloud_manifest_name = "CLOUDMANIFEST-db-1";
  const std::string sst_name = "000001.sst-epochA";
  auto provider = std::make_shared<RecordingPurgerStorageProvider>(
      EloqPurger::PurgerAllFiles{
          {cloud_manifest_name, MakeInfo(kNow - 2 * kHourMs)},
          {sst_name, MakeInfo(kNow - 2 * kHourMs)}});

  std::unique_ptr<CloudManifest> manifest;
  ASSERT_OK(CloudManifest::CreateForEmptyDatabase("epochA", &manifest));
  const std::string manifest_path = test::TmpDir() +
                                    "/purger_guard_read_error_cloud_manifest_" +
                                    std::to_string(Env::Default()->NowMicros());
  std::unique_ptr<WritableFileWriter> writer;
  ASSERT_OK(WritableFileWriter::Create(FileSystem::Default(), manifest_path,
                                       FileOptions(), &writer, nullptr));
  ASSERT_OK(manifest->WriteToLog(std::move(writer)));
  std::ifstream manifest_file(manifest_path, std::ios::binary);
  const std::string manifest_contents(
      (std::istreambuf_iterator<char>(manifest_file)),
      std::istreambuf_iterator<char>());
  ASSERT_FALSE(manifest_contents.empty());
  ASSERT_EQ(std::remove(manifest_path.c_str()), 0);
  provider->SetObjectContents("dbpath/" + cloud_manifest_name,
                              manifest_contents);
  provider->SetGetStatus("dbpath/smallest_new_file_number-epochA",
                         IOStatus::IOError("injected guard read failure"));
  auto cfs = MakeCloudFileSystem(provider);
  S3FileNumberReader reader("test-bucket", "dbpath", "epochA", cfs.get());

  uint64_t threshold = std::numeric_limits<uint64_t>::max();
  ASSERT_TRUE(reader.ReadSmallestFileNumber(&threshold).IsIOError());
  ASSERT_EQ(threshold, std::numeric_limits<uint64_t>::min());

  EloqPurger purger(cfs.get(), "test-bucket", "dbpath", false /*dry_run*/,
                    kHourMs, kHourMs, 10000);
  ASSERT_FALSE(purger.RunSinglePurgeCycle());
  ASSERT_TRUE(provider->delete_attempts().empty());
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

// Exhaust the decision boundary for a living epoch. This is the central safety
// property of the guard protocol: being live dominates every other signal, and
// an unreferenced file is eligible only when it is strictly below a nonzero
// published watermark.
TEST_F(EloqPurgerTest, LivingEpochSelectionSafetyMatrix) {
  for (uint64_t threshold : {uint64_t{0}, uint64_t{1}, uint64_t{10}}) {
    for (uint64_t file_number :
         {uint64_t{1}, uint64_t{9}, uint64_t{10}, uint64_t{11}}) {
      for (bool is_live : {false, true}) {
        char name_buffer[64];
        snprintf(name_buffer, sizeof(name_buffer), "%06llu.sst-epochA",
                 static_cast<unsigned long long>(file_number));
        const std::string name(name_buffer);
        const EloqPurger::PurgerAllFiles all_files{
            {name, MakeInfo(kNow - 10 * kHourMs)}};
        EloqPurger::PurgerLiveFileSet live_files;
        if (is_live) {
          live_files.insert(name);
        }

        std::vector<std::string> obsolete;
        purger_.SelectObsoleteSSTFilesWithThreshold(
            all_files, live_files, {{"epochA", threshold}}, kNow, &obsolete);

        const bool expected_delete =
            !is_live && threshold != 0 && file_number < threshold;
        SCOPED_TRACE(testing::Message()
                     << "threshold=" << threshold
                     << " file_number=" << file_number
                     << " live=" << is_live);
        ASSERT_EQ(!obsolete.empty(), expected_delete);
        if (expected_delete) {
          ASSERT_EQ(obsolete, std::vector<std::string>{name});
        }
      }
    }
  }
}

TEST_F(EloqPurgerTest, AgeGuardsUseInclusiveBoundaryAndRejectYoungerObjects) {
  const uint64_t boundary = kNow - kHourMs;
  const EloqPurger::PurgerAllFiles all_files{
      {"000001.sst-epochDead", MakeInfo(boundary)},
      {"000002.sst-epochDead", MakeInfo(boundary + 1)},
      {"MANIFEST-epochDeadBoundary", MakeInfo(boundary)},
      {"MANIFEST-epochDeadYoung", MakeInfo(boundary + 1)},
      {"smallest_new_file_number-epochDeadBoundary", MakeInfo(boundary)},
      {"smallest_new_file_number-epochDeadYoung", MakeInfo(boundary + 1)},
  };

  std::vector<std::string> obsolete;
  purger_.SelectObsoleteSSTFilesWithThreshold(all_files, {}, {}, kNow,
                                              &obsolete);
  purger_.SelectObsoleteManifestFiles(all_files, {}, kNow, &obsolete);
  purger_.SelectObsoleteFileNumberMarkers(all_files, {}, kNow, &obsolete);
  ASSERT_EQ(obsolete,
            std::vector<std::string>({"000001.sst-epochDead",
                                      "MANIFEST-epochDeadBoundary",
                                      "smallest_new_file_number-epochDeadBoundary"}));
}

// Full cycle over real serialized metadata. It simultaneously checks:
//   * live files from both current and historical epochs survive;
//   * current-epoch garbage below the guard is reclaimed;
//   * an in-flight boundary file (number == guard) survives;
//   * dead-epoch garbage obeys its age delay; and
//   * current metadata and unrelated objects are never selected.
TEST(EloqPurgerCycleTest, DeletesOnlyObjectsProvenUnreachable) {
  auto provider = std::make_shared<RecordingPurgerStorageProvider>(
      EloqPurger::PurgerAllFiles{
          {"CLOUDMANIFEST-db-2", MakeInfo(kNow - 2 * kHourMs)},
          {"MANIFEST-epochNew", MakeInfo(kNow - 2 * kHourMs)},
          {"000050.sst-epochOld", MakeInfo(kNow - 10 * kHourMs)},
          {"000060.sst-epochOld", MakeInfo(kNow - 10 * kHourMs)},
          {"000150.sst-epochNew", MakeInfo(kNow - 10 * kHourMs)},
          {"000130.sst-epochNew", MakeInfo(kNow - 10 * kHourMs)},
          {"000140.sst-epochNew", MakeInfo(kNow - 10 * kHourMs)},
          {"000007.sst-epochDead", MakeInfo(kNow - 2 * kHourMs)},
          {"000008.sst-epochOpening", MakeInfo(kNow - kHourMs + 1)},
          {"MANIFEST-epochDead", MakeInfo(kNow - 2 * kHourMs)},
          {"smallest_new_file_number-epochNew",
           MakeInfo(kNow - 2 * kHourMs)},
          {"smallest_new_file_number-epochDead",
           MakeInfo(kNow - 2 * kHourMs)},
          {"OPTIONS-000001", MakeInfo(kNow - 10 * kHourMs)},
      });
  provider->SetObjectContents(
      "dbpath/CLOUDMANIFEST-db-2",
      MakeCloudManifestContents("epochOld", {{100, "epochNew"}}));
  provider->SetObjectContents("dbpath/MANIFEST-epochNew",
                              MakeManifestContents({50, 150}));
  provider->SetObjectContents("dbpath/smallest_new_file_number-epochNew",
                              "140");
  auto cfs = MakeCloudFileSystem(provider);
  EloqPurger purger(cfs.get(), "test-bucket", "dbpath", false /*dry_run*/,
                    kHourMs, kHourMs, 10000);

  ASSERT_TRUE(purger.RunSinglePurgeCycle());
  ASSERT_EQ(provider->delete_attempts(),
            std::vector<std::string>({
                "dbpath/000060.sst-epochOld",
                "dbpath/000130.sst-epochNew",
                "dbpath/000007.sst-epochDead",
                "dbpath/MANIFEST-epochDead",
                "dbpath/smallest_new_file_number-epochDead",
            }));
}

// Failover is deliberately two-phase. During the cycle that retires the old
// CLOUDMANIFEST, its guard and MANIFEST are still loaded and must protect old
// epoch files. Only a subsequent fresh cycle may classify that epoch as dead.
TEST(EloqPurgerCycleTest, FailoverRetiresProtectionBeforeReclaimingOldEpoch) {
  auto provider = std::make_shared<RecordingPurgerStorageProvider>(
      EloqPurger::PurgerAllFiles{
          {"CLOUDMANIFEST-db-1", MakeInfo(kNow - 10 * kHourMs)},
          {"CLOUDMANIFEST-db-2", MakeInfo(kNow - 2 * kHourMs)},
          {"MANIFEST-epochOld", MakeInfo(kNow - 10 * kHourMs)},
          {"MANIFEST-epochNew", MakeInfo(kNow - 2 * kHourMs)},
          {"000040.sst-epochOld", MakeInfo(kNow - 10 * kHourMs)},
          {"000060.sst-epochOld", MakeInfo(kNow - 10 * kHourMs)},
          {"smallest_new_file_number-epochOld",
           MakeInfo(kNow - 10 * kHourMs)},
          {"smallest_new_file_number-epochNew",
           MakeInfo(kNow - 2 * kHourMs)},
      });
  provider->SetObjectContents("dbpath/CLOUDMANIFEST-db-1",
                              MakeCloudManifestContents("epochOld"));
  provider->SetObjectContents(
      "dbpath/CLOUDMANIFEST-db-2",
      MakeCloudManifestContents("epochOld", {{100, "epochNew"}}));
  provider->SetObjectContents("dbpath/MANIFEST-epochOld",
                              MakeManifestContents({40}));
  provider->SetObjectContents("dbpath/MANIFEST-epochNew",
                              MakeManifestContents({40}));
  provider->SetObjectContents("dbpath/smallest_new_file_number-epochOld",
                              "50");
  provider->SetObjectContents("dbpath/smallest_new_file_number-epochNew",
                              "100");
  auto cfs = MakeCloudFileSystem(provider);
  EloqPurger purger(cfs.get(), "test-bucket", "dbpath", false /*dry_run*/,
                    kHourMs, kHourMs, 10000);

  ASSERT_TRUE(purger.RunSinglePurgeCycle());
  ASSERT_EQ(provider->delete_attempts(),
            std::vector<std::string>{"dbpath/CLOUDMANIFEST-db-1"});

  ASSERT_TRUE(purger.RunSinglePurgeCycle());
  ASSERT_EQ(provider->delete_attempts(),
            std::vector<std::string>({
                "dbpath/CLOUDMANIFEST-db-1",
                "dbpath/000060.sst-epochOld",
                "dbpath/MANIFEST-epochOld",
                "dbpath/smallest_new_file_number-epochOld",
            }));
}

// Every observation required to prove unreachability happens before the first
// delete. Inject failures at each phase and require the entire cycle to be
// side-effect free.
TEST(EloqPurgerCycleTest, EveryPreDeletionFailureFailsClosed) {
  const std::vector<std::string> phases = {
      "list all",          "list cloud manifests", "read cloud manifest",
      "corrupt cloud manifest", "read guard",      "corrupt guard",
      "manifest metadata", "read manifest",        "upload clock",
      "clock metadata",
  };

  for (size_t i = 0; i < phases.size(); ++i) {
    auto provider = MakeValidCycleProvider();
    switch (i) {
      case 0:
        provider->SetListAllStatus(IOStatus::IOError("injected list failure"));
        break;
      case 1:
        provider->SetListPrefixStatus(
            IOStatus::IOError("injected prefix-list failure"));
        break;
      case 2:
        provider->SetGetStatus("dbpath/CLOUDMANIFEST-db-1",
                               IOStatus::IOError("injected CM read failure"));
        break;
      case 3:
        provider->SetObjectContents("dbpath/CLOUDMANIFEST-db-1", "corrupt");
        break;
      case 4:
        provider->SetGetStatus(
            "dbpath/smallest_new_file_number-epochA",
            IOStatus::IOError("injected guard read failure"));
        break;
      case 5:
        provider->SetObjectContents(
            "dbpath/smallest_new_file_number-epochA", "-1");
        break;
      case 6:
        provider->SetMetadataStatus(
            "dbpath/MANIFEST-epochA",
            IOStatus::IOError("injected manifest metadata failure"));
        break;
      case 7:
        provider->SetGetStatus(
            "dbpath/MANIFEST-epochA",
            IOStatus::IOError("injected manifest read failure"));
        break;
      case 8:
        provider->SetPutStatus(IOStatus::IOError("injected clock put failure"));
        break;
      case 9:
        provider->SetClockMetadataStatus(
            IOStatus::IOError("injected clock metadata failure"));
        break;
    }

    auto cfs = MakeCloudFileSystem(provider);
    EloqPurger purger(cfs.get(), "test-bucket", "dbpath", false /*dry_run*/,
                      kHourMs, kHourMs, 10000);
    SCOPED_TRACE(phases[i]);
    ASSERT_FALSE(purger.RunSinglePurgeCycle());
    ASSERT_TRUE(provider->delete_attempts().empty());
  }
}

TEST(EloqPurgerCycleTest, DryRunExecutesProofButNeverDeletes) {
  auto provider = MakeValidCycleProvider();
  auto cfs = MakeCloudFileSystem(provider);
  EloqPurger purger(cfs.get(), "test-bucket", "dbpath", true /*dry_run*/,
                    kHourMs, kHourMs, 10000);

  ASSERT_TRUE(purger.RunSinglePurgeCycle());
  ASSERT_TRUE(provider->delete_attempts().empty());
}

// ---- Strict parsing of numeric control objects (guard marker) ----

// std::stoull would accept all of these; a permissive parse of "-1" yields
// UINT64_MAX, a threshold that authorizes deleting every non-live SST of a
// live epoch.
TEST(EloqPurgerGuardParseTest, RejectsMalformedMarkerContents) {
  const std::string key = "dbpath/smallest_new_file_number-epochA";
  for (const std::string &bad :
       {std::string("-1"), std::string("+1"), std::string(" 1"),
        std::string("1 "), std::string("12abc"), std::string("0x10"),
        std::string(""), std::string("\n"), std::string("1.5"),
        std::string("99999999999999999999999"),
        std::string(70, '7') /* oversized object */}) {
    auto provider = std::make_shared<RecordingPurgerStorageProvider>(
        EloqPurger::PurgerAllFiles{});
    provider->SetObjectContents(key, bad);
    auto cfs = MakeCloudFileSystem(provider);
    S3FileNumberReader reader("test-bucket", "dbpath", "epochA", cfs.get());

    uint64_t threshold = 12345;
    Status s = reader.ReadSmallestFileNumber(&threshold);
    ASSERT_TRUE(s.IsCorruption()) << "input '" << bad << "' -> " << s.ToString();
    ASSERT_EQ(threshold, std::numeric_limits<uint64_t>::min());
  }
}

TEST(EloqPurgerGuardParseTest, AcceptsWellFormedMarkerContents) {
  const std::string key = "dbpath/smallest_new_file_number-epochA";
  const std::vector<std::pair<std::string, uint64_t>> cases = {
      {"0", 0},
      {"123", 123},
      {"123\n", 123},
      {"123\r\n", 123},
      {"18446744073709551615", std::numeric_limits<uint64_t>::max()},
  };
  for (const auto &[content, expected] : cases) {
    auto provider = std::make_shared<RecordingPurgerStorageProvider>(
        EloqPurger::PurgerAllFiles{});
    provider->SetObjectContents(key, content);
    auto cfs = MakeCloudFileSystem(provider);
    S3FileNumberReader reader("test-bucket", "dbpath", "epochA", cfs.get());

    uint64_t threshold = 0;
    ASSERT_OK(reader.ReadSmallestFileNumber(&threshold)) << content;
    ASSERT_EQ(threshold, expected) << content;
  }
}

// ---- Missing marker must fail closed ----

TEST(EloqPurgerCycleTest, MissingGuardMarkerAbortsCycle) {
  const std::string cloud_manifest_name = "CLOUDMANIFEST-db-1";
  auto provider = std::make_shared<RecordingPurgerStorageProvider>(
      EloqPurger::PurgerAllFiles{
          {cloud_manifest_name, MakeInfo(kNow - 2 * kHourMs)},
          {"000001.sst-epochA", MakeInfo(kNow - 2 * kHourMs)}});
  provider->SetObjectContents("dbpath/" + cloud_manifest_name,
                              MakeCloudManifestContents("epochA"));
  // No smallest_new_file_number-epochA object at all.
  provider->SetGetStatus("dbpath/smallest_new_file_number-epochA",
                         IOStatus::NotFound());
  auto cfs = MakeCloudFileSystem(provider);

  EloqPurger purger(cfs.get(), "test-bucket", "dbpath", false /*dry_run*/,
                    kHourMs, kHourMs, 10000, true /*require_guard_marker*/);
  ASSERT_FALSE(purger.RunSinglePurgeCycle());
  ASSERT_TRUE(provider->delete_attempts().empty());
}

// ---- Malformed CLOUDMANIFEST term must fail closed ----

// A permissive parse of "1x" reads as 1; a bogus high term could make the
// authoritative CLOUDMANIFEST look superseded.
TEST(EloqPurgerCycleTest, MalformedCloudManifestTermAbortsCycle) {
  const std::string bad_name = "CLOUDMANIFEST-db-1x";
  auto provider = std::make_shared<RecordingPurgerStorageProvider>(
      EloqPurger::PurgerAllFiles{{bad_name, MakeInfo(kNow - 2 * kHourMs)},
                                 {"MANIFEST-epochA",
                                  MakeInfo(kNow - 2 * kHourMs)},
                                 {"000001.sst-epochDead",
                                  MakeInfo(kNow - 2 * kHourMs)}});
  provider->SetObjectContents("dbpath/" + bad_name,
                              MakeCloudManifestContents("epochA"));
  provider->SetObjectContents("dbpath/MANIFEST-epochA",
                              MakeManifestContents({}));
  provider->SetObjectContents("dbpath/smallest_new_file_number-epochA", "5");
  auto cfs = MakeCloudFileSystem(provider);

  EloqPurger purger(cfs.get(), "test-bucket", "dbpath", false /*dry_run*/,
                    kHourMs, kHourMs, 10000);
  ASSERT_FALSE(purger.RunSinglePurgeCycle());
  ASSERT_TRUE(provider->delete_attempts().empty());
}

// ---- MANIFEST corruption must fail closed for the purger ----

// A torn tail is tolerated by the DB-open reader (so recovery can proceed)
// but must be an error for a scanner that deletes data: a skipped tail
// containing a file addition would make a live SST look unreferenced.
TEST(EloqPurgerManifestScanTest, StrictModeRejectsTornManifestTail) {
  const std::string path = test::TmpDir() + "/purger_torn_manifest_" +
                           std::to_string(Env::Default()->NowMicros());
  {
    std::unique_ptr<WritableFileWriter> writer;
    ASSERT_OK(WritableFileWriter::Create(FileSystem::Default(), path,
                                         FileOptions(), &writer, nullptr));
    log::Writer log_writer(std::move(writer), 0, false);
    VersionEdit edit;
    edit.SetNextFile(42);
    std::string record;
    ASSERT_TRUE(edit.EncodeTo(&record));
    ASSERT_OK(log_writer.AddRecord(WriteOptions(), record));
    ASSERT_OK(log_writer.file()->Sync(IOOptions(), false));
  }
  // Append a torn record: a partial header is exactly what a truncated
  // upload or a corrupted tail looks like.
  {
    std::ofstream out(path, std::ios::binary | std::ios::app);
    const std::string garbage(5, '\xab');
    out.write(garbage.data(), static_cast<std::streamsize>(garbage.size()));
  }

  uint64_t max_file_number = 0;
  // DB-open behavior: tolerate the tail so recovery can proceed.
  ASSERT_OK(ManifestReader::GetMaxFileNumberFromManifest(
      FileSystem::Default().get(), path, &max_file_number,
      kManifestScanDefaultMode));
  ASSERT_EQ(max_file_number, 42u);

  // Purger behavior: refuse.
  max_file_number = 0;
  Status strict = ManifestReader::GetMaxFileNumberFromManifest(
      FileSystem::Default().get(), path, &max_file_number,
      kManifestScanStrictMode);
  ASSERT_FALSE(strict.ok()) << strict.ToString();

  ASSERT_EQ(std::remove(path.c_str()), 0);
}

}  //  namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
