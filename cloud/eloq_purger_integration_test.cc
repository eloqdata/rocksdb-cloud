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

// End-to-end integration test for the purger against a real S3-compatible
// endpoint (Minio). It creates a real DBCloud, restarts it to mint dead
// epochs, compacts to create garbage, runs real purge cycles, and then
// reopens the database and reads every key back.
//
// The test is skipped unless these environment variables are set:
//   ELOQ_PURGER_TEST_S3_ENDPOINT   e.g. http://127.0.0.1:9900
//   ELOQ_PURGER_TEST_ACCESS_KEY
//   ELOQ_PURGER_TEST_SECRET_KEY
// Optional:
//   ELOQ_PURGER_TEST_S3_BUCKET     default: eloq-purger-it

#ifndef USE_AWS

#include <cstdio>
int main() {
  fprintf(stderr,
          "SKIPPED: eloq_purger_integration_test requires USE_AWS=1\n");
  return 0;
}

#else

#include <unistd.h>

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>

#include "cloud/eloq_purger.h"
#include "cloud/file_number_guard.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/stderr_logger.h"

namespace ROCKSDB_NAMESPACE {

namespace {

std::string GetEnvOr(const char *name, const std::string &def) {
  const char *v = getenv(name);
  return (v != nullptr && *v != '\0') ? std::string(v) : def;
}

// Path-style S3 client against a custom endpoint. Minio does not support
// virtual-host addressing without wildcard DNS, so unlike
// eloq_purger_command's factory this one forces path-style.
S3ClientFactory BuildPathStyleS3ClientFactory(const std::string &endpoint,
                                              bool https) {
  return [endpoint, https](
             const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>
                 &credentialsProvider,
             const Aws::Client::ClientConfiguration &baseConfig)
             -> std::shared_ptr<Aws::S3::S3Client> {
    Aws::Client::ClientConfiguration config = baseConfig;
    config.endpointOverride = endpoint;
    config.scheme = https ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;
    if (credentialsProvider) {
      return std::make_shared<Aws::S3::S3Client>(
          credentialsProvider, config,
          Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
          false /* useVirtualAddressing: path-style for Minio */);
    }
    return std::make_shared<Aws::S3::S3Client>(config);
  };
}

}  // namespace

class EloqPurgerIntegrationTest : public testing::Test {
 public:
  EloqPurgerIntegrationTest() {
    endpoint_url_ = GetEnvOr("ELOQ_PURGER_TEST_S3_ENDPOINT", "");
    access_key_ = GetEnvOr("ELOQ_PURGER_TEST_ACCESS_KEY", "");
    secret_key_ = GetEnvOr("ELOQ_PURGER_TEST_SECRET_KEY", "");
    bucket_ = GetEnvOr("ELOQ_PURGER_TEST_S3_BUCKET", "eloq-purger-it");

    // Strip the scheme; the client factory carries it separately.
    https_ = endpoint_url_.rfind("https://", 0) == 0;
    endpoint_ = endpoint_url_;
    auto scheme_pos = endpoint_.find("://");
    if (scheme_pos != std::string::npos) {
      endpoint_ = endpoint_.substr(scheme_pos + 3);
    }

    // Unique object path per run so reruns never collide. The local dbpath
    // must be unique alongside it: a leftover local dir from a previous run
    // records that run's dest path, and rocksdb-cloud refuses to pair it
    // with a different one ("NeedsReinitialization: bad dest path").
    object_path_ = "purger-it-" + std::to_string(getpid()) + "-" +
                   std::to_string(Env::Default()->NowMicros());
    local_dbpath_ = test::TmpDir() + "/" + object_path_;
  }

  bool Configured() const {
    return !endpoint_url_.empty() && !access_key_.empty() &&
           !secret_key_.empty();
  }

 protected:
  // One "process lifetime" of the database or the purger: a cloud FS plus
  // the composite Env the DB runs on.
  struct CloudSession {
    std::shared_ptr<FileSystem> cloud_fs;
    std::unique_ptr<Env> env;
    CloudFileSystemImpl *cfs_impl = nullptr;
  };

  CloudFileSystemOptions MakeCfsOptions() const {
    CloudFileSystemOptions cfs_options;
    cfs_options.publish_file_number_guard = guard_enabled_;
    cfs_options.guard_publish_interval = guard_publish_interval_;
    cfs_options.guard_entry_duration = guard_entry_duration_;
    cfs_options.credentials.InitializeSimple(access_key_, secret_key_);
    cfs_options.src_bucket.SetBucketName(bucket_);
    cfs_options.src_bucket.SetBucketPrefix("");
    cfs_options.src_bucket.SetObjectPath(object_path_);
    cfs_options.src_bucket.SetRegion("us-east-1");
    cfs_options.dest_bucket.SetBucketName(bucket_);
    cfs_options.dest_bucket.SetBucketPrefix("");
    cfs_options.dest_bucket.SetObjectPath(object_path_);
    cfs_options.dest_bucket.SetRegion("us-east-1");
    cfs_options.s3_client_factory =
        BuildPathStyleS3ClientFactory(endpoint_, https_);
    cfs_options.use_aws_transfer_manager = false;
    return cfs_options;
  }

  Status OpenSession(CloudSession *session, bool verbose_logging = false) {
    CloudFileSystem *cfs = nullptr;
    Status s = CloudFileSystemEnv::NewAwsFileSystem(
        FileSystem::Default(), MakeCfsOptions(), nullptr /*logger*/, &cfs);
    if (!s.ok()) {
      return s;
    }
    session->cloud_fs.reset(cfs);
    session->cfs_impl = dynamic_cast<CloudFileSystemImpl *>(cfs);
    session->env =
        CloudFileSystemEnv::NewCompositeEnv(Env::Default(), session->cloud_fs);
    if (verbose_logging && getenv("ELOQ_PURGER_TEST_VERBOSE") != nullptr) {
      session->cfs_impl->info_log_ =
          std::make_shared<StderrLogger>(InfoLogLevel::INFO_LEVEL);
    }
    return Status::OK();
  }

  // Opens the DB, hands it to `body`, closes it again. Each call is one
  // database generation: reopening rolls a new epoch.
  void WithDb(const std::function<void(DBCloud *)> &body) {
    CloudSession session;
    ASSERT_OK(OpenSession(&session));

    Options options;
    options.env = session.env.get();
    options.create_if_missing = true;
    options.disable_auto_compactions = true;

    DBCloud *db = nullptr;
    ASSERT_OK(DBCloud::Open(options, local_dbpath_,
                            "" /*persistent_cache_path*/, 0, &db));
    body(db);
    ASSERT_OK(db->Flush(FlushOptions()));
    delete db;
  }

  std::vector<std::string> ListObjects(CloudSession *session) {
    std::vector<std::string> names;
    IOStatus s = session->cfs_impl->GetStorageProvider()->ListCloudObjects(
        bucket_, object_path_, &names);
    EXPECT_TRUE(s.ok()) << s.ToString();
    return names;
  }

  static size_t CountWithPrefix(const std::vector<std::string> &names,
                                const std::string &prefix) {
    size_t n = 0;
    for (const auto &name : names) {
      if (name.rfind(prefix, 0) == 0) {
        ++n;
      }
    }
    return n;
  }

  static size_t CountSst(const std::vector<std::string> &names) {
    size_t n = 0;
    for (const auto &name : names) {
      if (name.find(".sst-") != std::string::npos) {
        ++n;
      }
    }
    return n;
  }

  // Reads the smallest_new_file_number-<epoch> guard object; empty string
  // when absent or unreadable.
  std::string ReadGuardMarker(CloudSession *session,
                              const std::string &epoch) {
    std::string key = SmallestFileNumberObjectKey(object_path_, epoch);
    std::string local = test::TmpDir() + "/guard_marker_" +
                        std::to_string(Env::Default()->NowMicros());
    IOStatus s = session->cfs_impl->GetStorageProvider()->GetCloudObject(
        bucket_, key, local);
    if (!s.ok()) {
      return "";
    }
    std::string content;
    ReadFileToString(Env::Default(), local, &content);
    Env::Default()->DeleteFile(local);
    return content;
  }

  std::string endpoint_url_;
  std::string endpoint_;
  bool https_ = false;
  std::string access_key_;
  std::string secret_key_;
  std::string bucket_;
  std::string object_path_;
  std::string local_dbpath_;
  bool guard_enabled_ = false;
  std::chrono::milliseconds guard_publish_interval_{std::chrono::seconds(30)};
  std::chrono::milliseconds guard_entry_duration_{std::chrono::seconds(15)};
};

TEST_F(EloqPurgerIntegrationTest, PurgeEndToEnd) {
  if (!Configured()) {
    ROCKSDB_GTEST_SKIP(
        "set ELOQ_PURGER_TEST_S3_ENDPOINT, ELOQ_PURGER_TEST_ACCESS_KEY, "
        "ELOQ_PURGER_TEST_SECRET_KEY to run this test");
    return;
  }

  WriteOptions wopt;
  wopt.disableWAL = true;

  auto Key = [](int i) {
    char buf[16];
    snprintf(buf, sizeof(buf), "key%04d", i);
    return std::string(buf);
  };

  // Generation 1: 200 keys across several SSTs.
  WithDb([&](DBCloud *db) {
    for (int i = 0; i < 200; i++) {
      ASSERT_OK(db->Put(wopt, Key(i), "v1-" + Key(i)));
      if ((i + 1) % 50 == 0) {
        ASSERT_OK(db->Flush(FlushOptions()));
      }
    }
  });

  // Generation 2 (epoch rolled; generation 1's epoch is now history):
  // overwrite half the keys, add new ones, then compact everything. The
  // compaction obsoletes the old-epoch SSTs -- exactly the files the
  // dead-epoch leak used to strand forever.
  WithDb([&](DBCloud *db) {
    for (int i = 0; i < 100; i++) {
      ASSERT_OK(db->Put(wopt, Key(i), "v2-" + Key(i)));
    }
    for (int i = 200; i < 300; i++) {
      ASSERT_OK(db->Put(wopt, Key(i), "v2-" + Key(i)));
    }
    ASSERT_OK(db->Flush(FlushOptions()));
    ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  });

  // Generation 3: open/close only, so generation 2's epoch also goes dead
  // while its compaction output stays live.
  WithDb([&](DBCloud *) {});

  // Snapshot the bucket before purging.
  CloudSession purge_session;
  ASSERT_OK(OpenSession(&purge_session));
  auto before = ListObjects(&purge_session);
  size_t sst_before = CountSst(before);
  size_t manifest_before = CountWithPrefix(before, "MANIFEST-");
  ASSERT_GT(sst_before, 0u);
  // Three generations => at least three MANIFEST-<epoch> objects.
  ASSERT_GE(manifest_before, 3u);

  // Let everything age past the (shortened) guards, then purge. Two cycles,
  // mirroring production where selection is recomputed per cycle.
  const uint64_t kGuardMs = 2000;
  std::this_thread::sleep_for(std::chrono::milliseconds(3000));
  EloqPurger purger(purge_session.cfs_impl, bucket_, object_path_,
                    false /*dry_run*/, kGuardMs /*cloudmanifest_retention_ms*/,
                    kGuardMs /*dead_epoch_file_age_ms*/,
                    100000 /*max_deletions_per_cycle*/);
  ASSERT_TRUE(purger.RunSinglePurgeCycle());
  ASSERT_TRUE(purger.RunSinglePurgeCycle());

  auto after = ListObjects(&purge_session);
  size_t sst_after = CountSst(after);
  size_t manifest_after = CountWithPrefix(after, "MANIFEST-");
  fprintf(stderr,
          "purge result: objects %zu -> %zu, sst %zu -> %zu, "
          "manifests %zu -> %zu\n",
          before.size(), after.size(), sst_before, sst_after, manifest_before,
          manifest_after);

  // Garbage was actually collected...
  ASSERT_LT(sst_after, sst_before);
  // ...dead-epoch MANIFESTs are gone, the live generation's remains...
  ASSERT_EQ(manifest_after, 1u);
  // ...and the CLOUDMANIFEST survived.
  ASSERT_EQ(CountWithPrefix(after, "CLOUDMANIFEST"), 1u);

  // The decisive check: reopen the database and read every key back.
  WithDb([&](DBCloud *db) {
    std::string value;
    for (int i = 0; i < 100; i++) {
      ASSERT_OK(db->Get(ReadOptions(), Key(i), &value)) << Key(i);
      ASSERT_EQ(value, "v2-" + Key(i));
    }
    for (int i = 100; i < 200; i++) {
      ASSERT_OK(db->Get(ReadOptions(), Key(i), &value)) << Key(i);
      ASSERT_EQ(value, "v1-" + Key(i));
    }
    for (int i = 200; i < 300; i++) {
      ASSERT_OK(db->Get(ReadOptions(), Key(i), &value)) << Key(i);
      ASSERT_EQ(value, "v2-" + Key(i));
    }
  });

  // Best-effort cleanup of the run's unique object path.
  purge_session.cfs_impl->GetStorageProvider()->EmptyBucket(bucket_,
                                                            object_path_);
}

// End-to-end test of the smallest_new_file_number guard: the open-time
// sentinel is replaced before Open returns, flush publishes downward before
// its SST reaches the cloud, and the purger honors the threshold.
TEST_F(EloqPurgerIntegrationTest, FileNumberGuardEndToEnd) {
  if (!Configured()) {
    ROCKSDB_GTEST_SKIP(
        "set ELOQ_PURGER_TEST_S3_ENDPOINT, ELOQ_PURGER_TEST_ACCESS_KEY, "
        "ELOQ_PURGER_TEST_SECRET_KEY to run this test");
    return;
  }

  guard_enabled_ = true;
  // Keep the background refresh and idle-decay loops reasonably quick.
  guard_publish_interval_ = std::chrono::seconds(3);
  guard_entry_duration_ = std::chrono::seconds(2);
  const std::string kMaxStr =
      std::to_string(std::numeric_limits<uint64_t>::max());

  WriteOptions wopt;
  wopt.disableWAL = true;

  CloudSession db_session;
  ASSERT_OK(OpenSession(&db_session));
  Options options;
  options.env = db_session.env.get();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;

  DBCloud *db = nullptr;
  ASSERT_OK(
      DBCloud::Open(options, local_dbpath_, "" /*persistent_cache*/, 0, &db));
  std::string epoch =
      db_session.cfs_impl->GetCloudManifest()->GetCurrentEpoch();
  ASSERT_FALSE(epoch.empty());

  // A separate session for observing the bucket (like a standalone purger).
  CloudSession observe_session;
  ASSERT_OK(OpenSession(&observe_session, /*verbose_logging=*/true));

  // (1) Open replaces its temporary 0 sentinel with the idle watermark before
  // returning, so the epoch does not remain unnecessarily blocked.
  ASSERT_EQ(ReadGuardMarker(&observe_session, epoch), kMaxStr);

  // (2) Flush: the downward publish lands a real value <= the flushed SST's
  // file number, synchronously within the flush.
  ASSERT_OK(db->Put(wopt, "k1", "v1"));
  ASSERT_OK(db->Flush(FlushOptions()));
  {
    std::string marker = ReadGuardMarker(&observe_session, epoch);
    ASSERT_FALSE(marker.empty());
    uint64_t marker_value = std::stoull(marker);
    ASSERT_GT(marker_value, 0u);
    std::vector<LiveFileMetaData> files;
    db->GetLiveFilesMetaData(&files);
    ASSERT_EQ(files.size(), 1u);
    uint64_t sst_number = std::stoull(files[0].name.substr(1));  // "/000123.sst"
    ASSERT_LE(marker_value, sst_number);
    ASSERT_LT(marker_value, std::numeric_limits<uint64_t>::max());
  }

  // (3) Idle: past linger + one publish interval, the marker decays to
  // UINT64_MAX.
  for (int i = 0; i < 20 && ReadGuardMarker(&observe_session, epoch) != kMaxStr;
       i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  ASSERT_EQ(ReadGuardMarker(&observe_session, epoch), kMaxStr);

  // (4) A new flush after idle drops the marker BEFORE the flushed SST
  // appears in cloud storage: capture the bucket listing at downward-publish
  // time via the sync point, then check the new SST wasn't there yet.
  std::vector<std::string> listing_at_publish;
  std::atomic<bool> capture_armed{false};
  SyncPoint::GetInstance()->SetCallBack(
      "FileNumberGuard::PutSmallestFileNumberObject:Status", [&](void *) {
        bool expected = true;
        if (capture_armed.compare_exchange_strong(expected, false)) {
          listing_at_publish = ListObjects(&observe_session);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::vector<LiveFileMetaData> before_files;
  db->GetLiveFilesMetaData(&before_files);
  capture_armed = true;
  ASSERT_OK(db->Put(wopt, "k2", "v2"));
  ASSERT_OK(db->Flush(FlushOptions()));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_FALSE(capture_armed.load());  // the downward publish fired

  std::vector<LiveFileMetaData> after_files;
  db->GetLiveFilesMetaData(&after_files);
  ASSERT_EQ(after_files.size(), before_files.size() + 1);
  // Find the new SST's cloud object name and assert it was absent from the
  // listing taken at publish time.
  std::string new_sst_cloud_name;
  for (const auto &f : after_files) {
    bool existed = false;
    for (const auto &b : before_files) {
      if (f.name == b.name) {
        existed = true;
        break;
      }
    }
    if (!existed) {
      new_sst_cloud_name =
          db_session.cfs_impl->RemapFilename(f.name.substr(1));
    }
  }
  ASSERT_FALSE(new_sst_cloud_name.empty());
  for (const auto &name : listing_at_publish) {
    ASSERT_NE(name, new_sst_cloud_name);
  }

  // (5) Purger honors the threshold. Create current-epoch garbage, then pin
  // the marker low with a synthetic in-flight job and verify a purge cycle
  // deletes nothing (file_number >= threshold is never deleted); release the
  // pin, let the marker decay to MAX, and verify the garbage then goes while
  // every live file survives.
  for (int i = 0; i < 4; i++) {
    // Overlapping key ranges in every SST, so CompactRange must rewrite the
    // inputs (a trivial move of non-overlapping files would create no
    // garbage).
    ASSERT_OK(db->Put(wopt, "a", "v" + std::to_string(i)));
    ASSERT_OK(db->Put(wopt, "z", "v" + std::to_string(i)));
    ASSERT_OK(db->Put(wopt, "bulk" + std::to_string(i), "v"));
    ASSERT_OK(db->Flush(FlushOptions()));
  }
  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  auto publisher = db_session.cfs_impl->GetFileNumberGuardPublisher();
  ASSERT_TRUE(publisher != nullptr);
  publisher->OnJobBegin(1, /*thread*/ 424242, /*job*/ 1);
  publisher->PeriodicPublish();
  ASSERT_EQ(ReadGuardMarker(&observe_session, epoch), "1");

  EloqPurger purger(observe_session.cfs_impl, bucket_, object_path_,
                    false /*dry_run*/, 3600 * 1000 /*retention*/,
                    1 /*dead_epoch_file_age_ms*/, 100000 /*cap*/);
  size_t sst_before = CountSst(ListObjects(&observe_session));
  ASSERT_TRUE(purger.RunSinglePurgeCycle());
  // Threshold 1: every non-live SST has file_number >= 1, so nothing may be
  // deleted.
  ASSERT_EQ(CountSst(ListObjects(&observe_session)), sst_before);

  // Release the pin and wait for the marker to decay to MAX.
  publisher->OnJobEnd(424242, 1);
  for (int i = 0; i < 20 && ReadGuardMarker(&observe_session, epoch) != kMaxStr;
       i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  ASSERT_EQ(ReadGuardMarker(&observe_session, epoch), kMaxStr);

  ASSERT_TRUE(purger.RunSinglePurgeCycle());
  size_t sst_after = CountSst(ListObjects(&observe_session));
  ASSERT_LT(sst_after, sst_before);

  // Every file the manifest references must have survived: read everything
  // back through the still-open DB.
  std::string value;
  ASSERT_OK(db->Get(ReadOptions(), "k1", &value));
  ASSERT_EQ(value, "v1");
  ASSERT_OK(db->Get(ReadOptions(), "k2", &value));
  ASSERT_EQ(value, "v2");
  for (int i = 0; i < 4; i++) {
    ASSERT_OK(db->Get(ReadOptions(), "bulk" + std::to_string(i), &value));
  }

  delete db;
  observe_session.cfs_impl->GetStorageProvider()->EmptyBucket(bucket_,
                                                              object_path_);
}

}  //  namespace ROCKSDB_NAMESPACE

int main(int argc, char **argv) {
  Aws::SDKOptions aws_options;
  Aws::InitAPI(aws_options);
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  Aws::ShutdownAPI(aws_options);
  return ret;
}

#endif  // USE_AWS
