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

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "cloud/cloud_manifest.h"
#include "rocksdb/cloud/cloud_file_system.h"
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

}  //  namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
