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

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cloud/cloud_manifest.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {
/**
 * Check if prerequisites are met for the purger to run
 * @param cfs The cloud file system to check
 * @return true if prerequisites are met, false otherwise
 */
bool PrerequisitesMet(const CloudFileSystemImpl &cfs);

/**
 * @brief S3 file updater for writing smallest file number to S3
 */
class S3FileNumberReader {
 public:
  S3FileNumberReader(const std::string &bucket_name,
                     const std::string &s3_object_path,
                     const std::string &epoch, CloudFileSystemImpl *cfs);

  ~S3FileNumberReader() = default;

  /**
   * @brief Read the smallest file number from S3
   * @return The smallest file number, or UINT64_MIN if not found
   */
  Status ReadSmallestFileNumber(uint64_t *file_number);

 private:
  std::string bucket_name_;
  std::string s3_object_path_;
  std::string epoch_;
  CloudFileSystemImpl *cfs_;

  std::string GetS3ObjectKey() const;
};

/**
 * @brief Enhanced purger with file number threshold support
 */
class EloqPurger {
 public:
  // Type aliases
  using PurgerAllFiles =
      std::vector<std::pair<std::string, CloudObjectInformation>>;
  using PurgerCloudManifestMap =
      std::unordered_map<std::string, std::unique_ptr<CloudManifest>>;
  using PurgerLiveFileSet = std::unordered_set<std::string>;
  using PurgerEpochManifestMap =
      std::unordered_map<std::string, CloudObjectInformation>;
  using PurgerFileNumberThresholds =
      std::unordered_map<std::string, uint64_t>;  // epoch -> threshold

  struct PurgerCycleState {
    PurgerAllFiles all_files;
    std::vector<std::string> cloud_manifest_files;
    PurgerCloudManifestMap cloudmanifests;
    PurgerLiveFileSet live_file_names;
    PurgerEpochManifestMap current_epoch_manifest_files;
    PurgerFileNumberThresholds
        file_number_thresholds;  // NEW: epoch -> min file number
    std::vector<std::string> obsolete_files;
  };

  EloqPurger(CloudFileSystemImpl *cfs, const std::string &bucket_name,
                 const std::string &object_path, bool dry_run,
                 uint64_t cloudmanifest_retention_ms = 3600 * 1000);

  /**
   * @brief Run a single purge cycle with improved file number checking
   */
  bool RunSinglePurgeCycle();

 private:
  CloudFileSystemImpl *cfs_;
  std::string bucket_name_;
  std::string object_path_;
  bool dry_run_;
  uint64_t cloudmanifest_retention_ms_;

  Status ListAllFiles(PurgerAllFiles *all_files);
  Status ListCloudManifests(std::vector<std::string> *cloud_manifest_files);
  Status LoadCloudManifests(
      const std::vector<std::string> &cloud_manifest_files,
      PurgerCloudManifestMap *manifests);
  Status CollectLiveFiles(const PurgerCloudManifestMap &cloudmanifests,
                          PurgerLiveFileSet *live_files,
                          PurgerEpochManifestMap *current_epoch_manifest_infos);
  Status LoadFileNumberThresholds(const PurgerCloudManifestMap &cloudmanifests,
                                  PurgerFileNumberThresholds *thresholds);
  void SelectObsoleteSSTFilesWithThreshold(
      const PurgerAllFiles &all_files, const PurgerLiveFileSet &live_files,
      const PurgerFileNumberThresholds &thresholds,
      std::vector<std::string> *obsolete_files);
  void SelectObsoleteManifestFiles(
      const PurgerAllFiles &all_files,
      const PurgerEpochManifestMap &current_epoch_manifest_infos,
      std::vector<std::string> *obsolete_files);
  void SelectObsoleteCloudManifestFiles(
      const PurgerAllFiles &all_files,
      const PurgerCloudManifestMap &cloudmanifests,
      const PurgerEpochManifestMap &current_epoch_manifest_infos,
      std::vector<std::string> *obsolete_files);
  Status GetS3CurrentTime(uint64_t *current_time);
  void DeleteObsoleteFiles(const std::vector<std::string> &obsolete_files);
};

}  // namespace ROCKSDB_NAMESPACE
