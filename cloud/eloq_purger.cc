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

#include <gflags/gflags.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <limits>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cloud/cloud_manifest.h"
#include "cloud/eloq_purger.h"
#include "cloud/file_number_guard.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "file/filename.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

namespace {

bool HasReachedAge(uint64_t now, uint64_t mtime, uint64_t threshold) {
  return now > mtime && now - mtime >= threshold;
}

}  // namespace

S3FileNumberReader::S3FileNumberReader(const std::string &bucket_name,
                                       const std::string &s3_object_path,
                                       const std::string &epoch,
                                       CloudFileSystemImpl *cfs)
    : bucket_name_(bucket_name),
      s3_object_path_(s3_object_path),
      epoch_(epoch),
      cfs_(cfs) {}

Status S3FileNumberReader::ReadSmallestFileNumber(uint64_t *file_number) {
  std::string object_key = GetS3ObjectKey();

  // Write to temp local file at first
  char tmp_template[] =
      "/tmp/smallest_file_number_download_XXXXXX";  // Xs will be replaced
  int fd = mkstemp(tmp_template);
  if (fd == -1) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "Failed to create temp file for reading smallest file number from S3: "
        "%s, object_key: %s",
        strerror(errno), object_key.c_str());
    *file_number = std::numeric_limits<uint64_t>::min();
    return Status::IOError("Failed to create temp file");
  }
  // Ensure restricted permissions (owner read/write only)
  if (fchmod(fd, S_IRUSR | S_IWUSR) != 0) {
    Log(InfoLogLevel::WARN_LEVEL, cfs_->info_log_,
        "Failed to set restricted permissions on temp file: %s",
        strerror(errno));
  }
  close(fd);  // We will open it later for reading
  std::string temp_file_path = tmp_template;

  rocksdb::IOStatus s = cfs_->GetStorageProvider()->GetCloudObject(
      bucket_name_, object_key, temp_file_path);

  if (!s.ok()) {
    std::remove(temp_file_path.c_str());
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "Failed to read smallest file number from S3: %s, object_key: %s, ",
        s.ToString().c_str(), object_key.c_str());
    if (!s.IsNotFound()) {
      // Transient/unknown failure. Do NOT fall back to the MANIFEST-derived
      // number: that is a high watermark of allocated file numbers and can
      // exceed in-flight compaction outputs, so substituting it for the
      // published low watermark risks deleting a file that is about to be
      // committed. Propagate the error so the purge cycle aborts.
      *file_number = std::numeric_limits<uint64_t>::min();
      return Status::IOError(s.ToString());
    }
    // NotFound is safe only under the deployment contract that every writable
    // DBCloud publishes this guard before uploading SSTs. Marker absence then
    // identifies a snapshot/branch epoch with no active writer, so the maximum
    // file number from its MANIFEST is a safe threshold.
    uint64_t manifest_max_file_number = 0;
    const std::string manifest_file_name = ManifestFileWithEpoch(epoch_);
    Status status = ManifestReader::GetMaxFileNumberFromManifest(
        cfs_, manifest_file_name, &manifest_max_file_number);
    if (status.ok()) {
      if (manifest_max_file_number > 0) {
        manifest_max_file_number -= 1;
      }
      *file_number = manifest_max_file_number;
      Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
          "Using max file number - 1 from MANIFEST as smallest file number: "
          "%llu, "
          "object_key: %s",
          static_cast<unsigned long long>(*file_number), object_key.c_str());
      return Status::OK();
    } else {
      Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
          "Failed to read max file number from MANIFEST: %s, returning "
          "UINT64_MIN",
          status.ToString().c_str());
    }

    *file_number = std::numeric_limits<uint64_t>::min();
    return Status::NotFound("Smallest file number object not found");
  }

  // Read the content of the temp file
  std::ifstream temp_file(temp_file_path);
  if (!temp_file.is_open()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "Failed to open temp file for reading smallest file number: %s, "
        "object_key: %s",
        temp_file_path.c_str(), object_key.c_str());
    *file_number = std::numeric_limits<uint64_t>::min();
    std::remove(temp_file_path.c_str());
    return Status::IOError("Failed to open temp file");
  }

  std::string content((std::istreambuf_iterator<char>(temp_file)),
                      std::istreambuf_iterator<char>());

  temp_file.close();
  // Remove the temp file
  if (std::remove(temp_file_path.c_str()) != 0) {
    Log(InfoLogLevel::WARN_LEVEL, cfs_->info_log_,
        "Warning: Failed to remove temp file %s", temp_file_path.c_str());
  }

  try {
    *file_number = std::stoull(content);
    Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
        "Read smallest file number from S3: %llu, object_key: %s",
        static_cast<unsigned long long>(*file_number), object_key.c_str());
    return Status::OK();
  } catch (const std::exception &e) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "Failed to parse smallest file number from S3 content: '%s', "
        "returning UINT64_MIN",
        content.c_str());
    *file_number = std::numeric_limits<uint64_t>::min();
    return Status::Corruption("Failed to parse smallest file number: %s",
                              e.what());
  }
}

std::string S3FileNumberReader::GetS3ObjectKey() const {
  // Shared with the writer (FileNumberGuardPublisher): one definition of the
  // guard object's key for both sides of the protocol.
  return SmallestFileNumberObjectKey(s3_object_path_, epoch_);
}

EloqPurger::EloqPurger(CloudFileSystemImpl *cfs, const std::string &bucket_name,
                       const std::string &object_path, bool dry_run,
                       uint64_t cloudmanifest_retention_ms,
                       uint64_t dead_epoch_file_age_ms,
                       uint64_t max_deletions_per_cycle)
    : cfs_(cfs),
      bucket_name_(bucket_name),
      object_path_(object_path),
      dry_run_(dry_run),
      cloudmanifest_retention_ms_(cloudmanifest_retention_ms),
      dead_epoch_file_age_ms_(dead_epoch_file_age_ms),
      max_deletions_per_cycle_(max_deletions_per_cycle) {}

bool EloqPurger::RunSinglePurgeCycle() {
  PurgerCycleState state;

  Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
      "[pg] Starting purge cycle for %s/%s", bucket_name_.c_str(),
      object_path_.c_str());

  // list all files in the object path, for fetch all obsolete files and live
  // files
  if (!ListAllFiles(&state.all_files).ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "[pg] Failed to list all files, aborting purge cycle");
    return false;
  }

  // list all cloud manifests in the object path
  // it's safe to purge the obsolete files that are get by above step
  // any new files generated after above step fetching will be kept
  if (!ListCloudManifests(&state.cloud_manifest_files).ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "[pg] Failed to list cloud manifests, aborting purge cycle");
    return false;
  }

  if (!LoadCloudManifests(state.cloud_manifest_files, &state.cloudmanifests)
           .ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "[pg] Failed to load cloud manifests, aborting purge cycle");
    return false;
  }

  // NEW: Load file number thresholds from S3
  // before collecting live files
  if (!LoadFileNumberThresholds(state.cloudmanifests,
                                &state.file_number_thresholds)
           .ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "[pg] Failed to load file number thresholds, aborting purge cycle");
    return false;
  }

  if (!CollectLiveFiles(state.cloudmanifests, &state.live_file_names,
                        &state.current_epoch_manifest_files)
           .ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "[pg] Failed to collect live files, aborting purge cycle");
    return false;
  }

  // Read the S3-derived clock once; it serves every age-based decision this
  // cycle (dead-epoch reclamation and CLOUDMANIFEST retention).
  if (!GetS3CurrentTime(&state.s3_current_time).ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "[pg] Failed to get S3 current time, aborting purge cycle");
    return false;
  }

  // The cap consumes a deterministic SST -> MANIFEST -> CLOUDMANIFEST ->
  // marker prefix. Metadata can wait behind an SST backlog, but the remainder
  // is re-selected and converges on later cycles.
  SelectObsoleteSSTFilesWithThreshold(state.all_files, state.live_file_names,
                                      state.file_number_thresholds,
                                      state.s3_current_time,
                                      &state.obsolete_files);

  // Select obsolete manifest files
  SelectObsoleteManifestFiles(state.all_files,
                              state.current_epoch_manifest_files,
                              state.s3_current_time, &state.obsolete_files);

  // Select obsolete CLOUDMANIFEST files
  SelectObsoleteCloudManifestFiles(state.all_files, state.cloudmanifests,
                                   state.current_epoch_manifest_files,
                                   state.s3_current_time,
                                   &state.obsolete_files);

  // Select smallest_new_file_number markers of dead epochs
  SelectObsoleteFileNumberMarkers(state.all_files,
                                  state.file_number_thresholds,
                                  state.s3_current_time,
                                  &state.obsolete_files);

  Status deletion_status;
  size_t deleted = 0;
  size_t failures = 0;
  if (dry_run_) {
    Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
        "[pg] DRY RUN: Would delete %zu files", state.obsolete_files.size());
    for (const auto &file : state.obsolete_files) {
      Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
          "[pg] DRY RUN: Would delete %s", file.c_str());
    }
  } else {
    deletion_status =
        DeleteObsoleteFiles(state.obsolete_files, &deleted, &failures);
  }

  Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
      "[pg] Purge cycle summary: total_files=%zu manifests=%zu "
      "live_files=%zu obsolete_selected=%zu deleted=%zu failed=%zu "
      "thresholds_loaded=%zu",
      state.all_files.size(), state.cloudmanifests.size(),
      state.live_file_names.size(), state.obsolete_files.size(), deleted,
      failures,
      state.file_number_thresholds.size());

  return deletion_status.ok();
}

Status EloqPurger::ListAllFiles(PurgerAllFiles *all_files) {
  IOStatus s = cfs_->GetStorageProvider()->ListCloudObjects(
      bucket_name_, object_path_, all_files);

  if (!s.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "[pg] Failed to list files in destination object path %s: %s",
        object_path_.c_str(), s.ToString().c_str());
    return Status::IOError(s.ToString());
  }

  Log(InfoLogLevel::DEBUG_LEVEL, cfs_->info_log_,
      "[pg] Total files listed: %zu", all_files->size());
  return Status::OK();
}

Status EloqPurger::ListCloudManifests(
    std::vector<std::string> *cloud_manifest_files) {
  IOStatus s = cfs_->GetStorageProvider()->ListCloudObjectsWithPrefix(
      bucket_name_, object_path_, "CLOUDMANIFEST", cloud_manifest_files);

  if (!s.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "[pg] Failed to list cloud manifest files in bucket %s: %s",
        bucket_name_.c_str(), s.ToString().c_str());
    return Status::IOError(s.ToString());
  }

  Log(InfoLogLevel::DEBUG_LEVEL, cfs_->info_log_,
      "[pg] Found %zu cloud manifest files", cloud_manifest_files->size());
  return Status::OK();
}

Status EloqPurger::LoadCloudManifests(
    const std::vector<std::string> &cloud_manifest_files,
    PurgerCloudManifestMap *manifests) {
  const FileOptions file_opts;
  IODebugContext *dbg = nullptr;

  for (const auto &cloud_manifest_file : cloud_manifest_files) {
    std::string full_path = object_path_ + "/" + cloud_manifest_file;
    std::unique_ptr<FSSequentialFile> file;

    IOStatus s = cfs_->NewSequentialFileCloud(bucket_name_, full_path,
                                              file_opts, &file, dbg);
    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
          "[pg] Failed to open cloud manifest file %s: %s",
          cloud_manifest_file.c_str(), s.ToString().c_str());
      return Status::IOError(s.ToString());
    }

    std::unique_ptr<CloudManifest> cloud_manifest;
    s = CloudManifest::LoadFromLog(
        std::unique_ptr<SequentialFileReader>(
            new SequentialFileReader(std::move(file), cloud_manifest_file)),
        &cloud_manifest);

    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
          "[pg] Failed to load cloud manifest from file %s: %s",
          cloud_manifest_file.c_str(), s.ToString().c_str());
      return Status::IOError(s.ToString());
    }

    Log(InfoLogLevel::DEBUG_LEVEL, cfs_->info_log_,
        "[pg] Loaded cloud manifest file %s with current epoch %s",
        cloud_manifest_file.c_str(), cloud_manifest->GetCurrentEpoch().c_str());

    (*manifests)[cloud_manifest_file] = std::move(cloud_manifest);
  }

  return Status::OK();
}

Status EloqPurger::CollectLiveFiles(
    const PurgerCloudManifestMap &cloudmanifests, PurgerLiveFileSet *live_files,
    PurgerEpochManifestMap *current_epoch_manifest_infos) {
  std::unique_ptr<ManifestReader> manifest_reader =
      std::make_unique<ManifestReader>(cfs_->info_log_, cfs_, bucket_name_);

  std::set<uint64_t> live_file_numbers;

  for (const auto &entry : cloudmanifests) {
    const std::string &cloud_manifest_name = entry.first;
    CloudManifest *cloud_manifest_ptr = entry.second.get();

    live_file_numbers.clear();
    std::string current_epoch = cloud_manifest_ptr->GetCurrentEpoch();
    std::string manifest_file =
        ManifestFileWithEpoch(object_path_, current_epoch);

    CloudObjectInformation manifest_file_info;
    IOStatus s = cfs_->GetStorageProvider()->GetCloudObjectMetadata(
        bucket_name_, manifest_file, &manifest_file_info);

    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
          "[pg] Failed to get metadata for manifest file %s: %s",
          manifest_file.c_str(), s.ToString().c_str());
      return Status::IOError(s.ToString());
    }

    (*current_epoch_manifest_infos)[current_epoch] = manifest_file_info;

    s = manifest_reader->GetLiveFiles(object_path_, current_epoch,
                                      &live_file_numbers);
    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
          "[pg] Failed to get live files from cloud manifest file %s: %s",
          cloud_manifest_name.c_str(), s.ToString().c_str());
      return Status::IOError(s.ToString());
    }

    for (uint64_t num : live_file_numbers) {
      std::string file_name = MakeTableFileName(num);
      file_name =
          cfs_->RemapFilenameWithCloudManifest(file_name, cloud_manifest_ptr);
      live_files->insert(file_name);
      Log(InfoLogLevel::DEBUG_LEVEL, cfs_->info_log_,
          "[pg] Live file %s found in cloud manifest %s", file_name.c_str(),
          cloud_manifest_name.c_str());
    }
  }

  return Status::OK();
}

Status EloqPurger::LoadFileNumberThresholds(
    const PurgerCloudManifestMap &cloudmanifests,
    PurgerFileNumberThresholds *thresholds) {
  for (const auto &entry : cloudmanifests) {
    CloudManifest *manifest = entry.second.get();
    std::string epoch = manifest->GetCurrentEpoch();

    // Create S3 file number updater to read threshold
    auto s3_updater = std::make_unique<S3FileNumberReader>(
        bucket_name_, object_path_, epoch, cfs_);

    uint64_t threshold;
    Status s = s3_updater->ReadSmallestFileNumber(&threshold);
    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
          "[pg] Failed to read file number threshold for epoch %s: %s",
          epoch.c_str(), s.ToString().c_str());
      return s;
    }

    (*thresholds)[epoch] = threshold;

    Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
        "[pg] Loaded file number threshold %llu for epoch %s",
        static_cast<unsigned long long>(threshold), epoch.c_str());
  }

  return Status::OK();
}

void EloqPurger::SelectObsoleteSSTFilesWithThreshold(
    const PurgerAllFiles &all_files, const PurgerLiveFileSet &live_files,
    const PurgerFileNumberThresholds &thresholds, uint64_t s3_current_time,
    std::vector<std::string> *obsolete_files) {
  for (const auto &candidate : all_files) {
    const std::string &candidate_file_path = candidate.first;

    // Skip non-SST files
    if (!IsSstFile(RemoveEpoch(candidate_file_path))) {
      continue;
    }

    // Skip live files
    if (live_files.find(candidate_file_path) != live_files.end()) {
      continue;
    }

    std::string candidate_epoch = GetEpoch(candidate_file_path);

    // NEW: Check file number threshold
    auto threshold_it = thresholds.find(candidate_epoch);
    if (threshold_it != thresholds.end()) {
      uint64_t threshold = threshold_it->second;
      // Zero is the intentional full-block sentinel for a live epoch.
      if (threshold != std::numeric_limits<uint64_t>::min()) {
        // Extract file number from candidate file name
        uint64_t file_number = 0;
        std::string base_name = RemoveEpoch(candidate_file_path);
        FileType type;
        if (ParseFileName(base_name, &file_number, &type)) {
          if (file_number < threshold) {
            obsolete_files->push_back(candidate_file_path);
            Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
                "[pg] File %s selected for deletion (file_num=%llu, "
                "threshold=%llu)",
                candidate_file_path.c_str(),
                static_cast<unsigned long long>(file_number),
                static_cast<unsigned long long>(threshold));
          } else {
            Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
                "[pg] Skipping obsolete file %s due to file number "
                "threshold (file_num=%llu, threshold=%llu)",
                candidate_file_path.c_str(),
                static_cast<unsigned long long>(file_number),
                static_cast<unsigned long long>(threshold));
          }
        }
      }
    } else {
      // No threshold entry means no loaded CLOUDMANIFEST claims this epoch
      // as its current epoch: the epoch is dead, no writer can ever add
      // files to it, so a non-live file is garbage. The age guard covers
      // the one race this reasoning misses: a node mid-open (or
      // mid-branch-creation) whose SSTs were uploaded before our listing
      // but whose CLOUDMANIFEST landed after it.
      uint64_t file_mtime = candidate.second.modification_time;
      if (HasReachedAge(s3_current_time, file_mtime, dead_epoch_file_age_ms_)) {
        obsolete_files->push_back(candidate_file_path);
        Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
            "[pg] File %s selected for deletion (dead epoch %s, "
            "file_mtime=%llu, s3_current_time=%llu)",
            candidate_file_path.c_str(), candidate_epoch.c_str(),
            static_cast<unsigned long long>(file_mtime),
            static_cast<unsigned long long>(s3_current_time));
      } else {
        Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
            "[pg] Keeping file %s: epoch %s has no threshold and file is "
            "younger than %llu ms (file_mtime=%llu, s3_current_time=%llu)",
            candidate_file_path.c_str(), candidate_epoch.c_str(),
            static_cast<unsigned long long>(dead_epoch_file_age_ms_),
            static_cast<unsigned long long>(file_mtime),
            static_cast<unsigned long long>(s3_current_time));
      }
    }
  }
}

void EloqPurger::SelectObsoleteManifestFiles(
    const PurgerAllFiles &all_files,
    const PurgerEpochManifestMap &current_epoch_manifest_infos,
    uint64_t s3_current_time, std::vector<std::string> *obsolete_files) {
  for (const auto &candidate : all_files) {
    const std::string &candidate_file_path = candidate.first;

    // Skip non-manifest files
    if (!IsManifestFile(RemoveEpoch(candidate_file_path))) {
      continue;
    }

    std::string candidate_epoch = GetEpoch(candidate_file_path);

    // Skip current epoch manifest files
    auto it = current_epoch_manifest_infos.find(candidate_epoch);
    if (it != current_epoch_manifest_infos.end()) {
      continue;
    }

    // A node rolling a new epoch uploads MANIFEST-<epoch> BEFORE the
    // CLOUDMANIFEST that makes the epoch current, so a purge cycle that
    // lists between the two uploads sees a MANIFEST with no protecting
    // CLOUDMANIFEST. The age guard keeps such a fresh MANIFEST until the
    // open either commits (epoch becomes current) or is abandoned.
    uint64_t manifest_mtime = candidate.second.modification_time;
    if (HasReachedAge(s3_current_time, manifest_mtime,
                      dead_epoch_file_age_ms_)) {
      obsolete_files->push_back(candidate_file_path);
      Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
          "[pg] Manifest file %s selected for deletion (dead epoch %s, "
          "manifest_mtime=%llu, s3_current_time=%llu)",
          candidate_file_path.c_str(), candidate_epoch.c_str(),
          static_cast<unsigned long long>(manifest_mtime),
          static_cast<unsigned long long>(s3_current_time));
    } else {
      Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
          "[pg] Keeping manifest file %s: epoch %s is not current but the "
          "manifest is younger than %llu ms (manifest_mtime=%llu, "
          "s3_current_time=%llu)",
          candidate_file_path.c_str(), candidate_epoch.c_str(),
          static_cast<unsigned long long>(dead_epoch_file_age_ms_),
          static_cast<unsigned long long>(manifest_mtime),
          static_cast<unsigned long long>(s3_current_time));
    }
  }
}

Status EloqPurger::GetS3CurrentTime(uint64_t *current_time) {
  // Create a temporary local file
  char tmp_template[] = "./purger_s3_time_XXXXXX";
  int fd = mkstemp(tmp_template);
  if (fd == -1) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "[pg] Failed to create temp file for S3 time check: %s",
        strerror(errno));
    return Status::IOError("Failed to create temp file");
  }

  // Ensure restricted permissions (owner read/write only)
  if (fchmod(fd, S_IRUSR | S_IWUSR) != 0) {
    Log(InfoLogLevel::WARN_LEVEL, cfs_->info_log_,
        "[pg] Failed to set restricted permissions on temp file: %s",
        strerror(errno));
  }

  std::string temp_local_path = tmp_template;

  // Write a small amount of data to the file
  const char *content = "time_check";
  size_t content_len = strlen(content);
  ssize_t bytes_written = write(fd, content, content_len);
  close(fd);

  if (bytes_written < 0 || static_cast<size_t>(bytes_written) != content_len) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "[pg] Failed to write to temp file for S3 time check: %s (wrote %zd of "
        "%zu bytes)",
        strerror(errno), bytes_written, content_len);
    std::remove(temp_local_path.c_str());
    return Status::IOError("Failed to write to temp file");
  }

  // Extract just the filename from the local path and use it for S3
  std::string temp_filename =
      temp_local_path.substr(temp_local_path.find_last_of('/') + 1);
  std::string temp_s3_path = object_path_ + "/" + temp_filename;

  // Upload the file to S3
  IOStatus s = cfs_->GetStorageProvider()->PutCloudObject(
      temp_local_path, bucket_name_, temp_s3_path);

  if (!s.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "[pg] Failed to upload temp file to S3 for time check: %s",
        s.ToString().c_str());
    std::remove(temp_local_path.c_str());
    return Status::IOError(s.ToString());
  }

  CloudObjectInformation file_info;
  s = cfs_->GetStorageProvider()->GetCloudObjectMetadata(
      bucket_name_, temp_s3_path, &file_info);

  if (!s.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "[pg] Failed to get metadata for temp file from S3: %s",
        s.ToString().c_str());
    // Try to delete the temp file anyway
    cfs_->GetStorageProvider()->DeleteCloudObject(bucket_name_, temp_s3_path);
    std::remove(temp_local_path.c_str());
    return Status::IOError(s.ToString());
  }

  *current_time = file_info.modification_time;

  // Delete the temporary file from S3
  s = cfs_->GetStorageProvider()->DeleteCloudObject(bucket_name_, temp_s3_path);
  if (!s.ok()) {
    Log(InfoLogLevel::WARN_LEVEL, cfs_->info_log_,
        "[pg] Failed to delete temp file from S3: %s - may require manual "
        "cleanup",
        s.ToString().c_str());
  }

  // Delete the local temporary file
  if (std::remove(temp_local_path.c_str()) != 0) {
    Log(InfoLogLevel::WARN_LEVEL, cfs_->info_log_,
        "[pg] Warning: Failed to remove local temp file %s",
        temp_local_path.c_str());
  }

  Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
      "[pg] Retrieved S3 current time: %llu",
      static_cast<unsigned long long>(*current_time));

  return Status::OK();
}

void EloqPurger::SelectObsoleteCloudManifestFiles(
    const PurgerAllFiles &all_files,
    const PurgerCloudManifestMap &cloudmanifests,
    const PurgerEpochManifestMap &current_epoch_manifest_infos,
    uint64_t s3_current_time, std::vector<std::string> *obsolete_files) {
  // Struct to represent CLOUDMANIFEST file information
  struct CloudManifestFileInfo {
    uint64_t term;
    std::string file_path;
    // The CLOUDMANIFEST object's own S3 mtime. A CLOUDMANIFEST is written
    // once, when its generation starts, so the max-term entry's mtime is
    // the moment the older terms in the group were superseded.
    uint64_t cloudmanifest_timestamp;
    std::string epoch;

    CloudManifestFileInfo(uint64_t t, const std::string &path,
                          uint64_t cloudmanifest_ts, const std::string &ep)
        : term(t),
          file_path(path),
          cloudmanifest_timestamp(cloudmanifest_ts),
          epoch(ep) {}
  };

  // Map from postfix to list of CLOUDMANIFEST file info
  std::unordered_map<std::string, std::vector<CloudManifestFileInfo>>
      grouped_manifests;

  const std::string prefix = "CLOUDMANIFEST-";

  // Parse and group CLOUDMANIFEST files
  for (const auto &candidate : all_files) {
    const std::string &candidate_file_path = candidate.first;

    // Check if it's a CLOUDMANIFEST file
    if (candidate_file_path.find(prefix) != 0) {
      continue;
    }

    // Look up in cloudmanifests to get the epoch
    auto manifest_it = cloudmanifests.find(candidate_file_path);
    if (manifest_it == cloudmanifests.end()) {
      Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
          "[pg] Skipping CLOUDMANIFEST file %s (not loaded)",
          candidate_file_path.c_str());
      continue;
    }

    std::string current_epoch = manifest_it->second->GetCurrentEpoch();

    // Preserve the current-epoch manifest lookup: missing info means this
    // CLOUDMANIFEST is not safe to consider for deletion this cycle.
    auto manifest_info_it = current_epoch_manifest_infos.find(current_epoch);
    if (manifest_info_it == current_epoch_manifest_infos.end()) {
      Log(InfoLogLevel::WARN_LEVEL, cfs_->info_log_,
          "[pg] No current manifest info found for epoch %s, skipping "
          "CLOUDMANIFEST %s",
          current_epoch.c_str(), candidate_file_path.c_str());
      continue;
    }

    // Extract the part after "CLOUDMANIFEST-"
    std::string remainder = candidate_file_path.substr(prefix.length());

    // Find the last dash to separate postfix and term
    size_t last_dash = remainder.find_last_of('-');

    std::string postfix;
    std::string term_str;

    if (last_dash == std::string::npos) {
      // Pattern: CLOUDMANIFEST-{term} (no postfix)
      postfix = "";
      term_str = remainder;
    } else {
      // Pattern: CLOUDMANIFEST-{postfix}-{term}
      postfix = remainder.substr(0, last_dash);
      term_str = remainder.substr(last_dash + 1);
    }

    // Validate that term is a number
    uint64_t term = 0;
    try {
      term = std::stoull(term_str);
    } catch (const std::exception &e) {
      // Not a valid pattern, skip this file
      Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
          "[pg] Skipping CLOUDMANIFEST file %s (invalid term: %s)",
          candidate_file_path.c_str(), term_str.c_str());
      continue;
    }

    // Group by postfix
    grouped_manifests[postfix].emplace_back(term, candidate_file_path,
                                            candidate.second.modification_time,
                                            current_epoch);

    Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
        "[pg] Found CLOUDMANIFEST file %s with postfix='%s', term=%llu, "
        "cloudmanifest_timestamp=%llu, epoch=%s",
        candidate_file_path.c_str(), postfix.c_str(),
        static_cast<unsigned long long>(term),
        static_cast<unsigned long long>(candidate.second.modification_time),
        current_epoch.c_str());
  }

  const uint64_t current_time = s3_current_time;

  // Use configurable retention time (in milliseconds)
  const uint64_t retention_threshold_ms = cloudmanifest_retention_ms_;

  // Process each postfix group
  for (auto &group : grouped_manifests) {
    const std::string &postfix = group.first;
    auto &files = group.second;

    if (files.empty()) {
      continue;
    }

    // Find the file with the largest term (current CLOUDMANIFEST)
    auto max_it = std::max_element(
        files.begin(), files.end(),
        [](const CloudManifestFileInfo &a, const CloudManifestFileInfo &b) {
          return a.term < b.term;
        });

    uint64_t max_term = max_it->term;
    // A CLOUDMANIFEST is written once, at generation start, so the max-term
    // entry's own mtime is the moment every older term in this group was
    // superseded. Retention is measured from that supersession, NOT from the
    // old generation's MANIFEST mtime: the latter is a last-write time, and
    // a write-idle (e.g. read-only) generation would look expired the
    // instant it is superseded, stripping a still-running old primary of
    // live-file and threshold protection with no grace period.
    uint64_t supersession_time = max_it->cloudmanifest_timestamp;

    Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
        "[pg] CLOUDMANIFEST group postfix='%s': largest term=%llu, "
        "superseded older terms at %llu",
        postfix.c_str(), static_cast<unsigned long long>(max_term),
        static_cast<unsigned long long>(supersession_time));

    // Check each file in the group
    for (const auto &file_info : files) {
      // Keep the file with the largest term (current CLOUDMANIFEST)
      if (file_info.term == max_term) {
        Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
            "[pg] Keeping CLOUDMANIFEST file %s (largest term in group)",
            file_info.file_path.c_str());
        continue;
      }

      // Delete the superseded CLOUDMANIFEST once the successor has existed
      // for the full retention window, giving the superseded generation a
      // guaranteed grace period regardless of how long it had been
      // write-idle before the failover.
      if (HasReachedAge(current_time, supersession_time,
                        retention_threshold_ms)) {
        obsolete_files->push_back(file_info.file_path);
        Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
            "[pg] CLOUDMANIFEST file %s selected for deletion "
            "(term=%llu, supersession_time=%llu, s3_current_time=%llu, "
            "epoch=%s, superseded_for=%llu ms)",
            file_info.file_path.c_str(),
            static_cast<unsigned long long>(file_info.term),
            static_cast<unsigned long long>(supersession_time),
            static_cast<unsigned long long>(current_time),
            file_info.epoch.c_str(),
            static_cast<unsigned long long>(current_time - supersession_time));
      } else {
        uint64_t time_diff = (current_time > supersession_time)
                                 ? (current_time - supersession_time)
                                 : 0;
        Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
            "[pg] Keeping CLOUDMANIFEST file %s "
            "(term=%llu, supersession_time=%llu, s3_current_time=%llu, "
            "epoch=%s, superseded_for=%llu ms < retention threshold %llu "
            "ms)",
            file_info.file_path.c_str(),
            static_cast<unsigned long long>(file_info.term),
            static_cast<unsigned long long>(supersession_time),
            static_cast<unsigned long long>(current_time),
            file_info.epoch.c_str(), static_cast<unsigned long long>(time_diff),
            static_cast<unsigned long long>(retention_threshold_ms));
      }
    }
  }
}

void EloqPurger::SelectObsoleteFileNumberMarkers(
    const PurgerAllFiles &all_files,
    const PurgerFileNumberThresholds &thresholds, uint64_t s3_current_time,
    std::vector<std::string> *obsolete_files) {
  // The writer publishes one smallest_new_file_number-<epoch> object per
  // epoch it writes in. Markers of living epochs (present in `thresholds`,
  // which holds one entry per loaded CLOUDMANIFEST's current epoch) are in
  // use; markers of dead epochs are garbage. The age guard covers a node
  // mid-open that published its marker before its CLOUDMANIFEST landed.
  const std::string marker_prefix = kSmallestFileNumberFilePrefix;
  for (const auto &candidate : all_files) {
    const std::string &candidate_file_path = candidate.first;

    if (candidate_file_path.compare(0, marker_prefix.size(), marker_prefix) !=
        0) {
      continue;
    }

    std::string epoch = candidate_file_path.substr(marker_prefix.size());
    if (thresholds.find(epoch) != thresholds.end()) {
      continue;  // living epoch, marker still in use
    }

    uint64_t marker_mtime = candidate.second.modification_time;
    if (HasReachedAge(s3_current_time, marker_mtime, dead_epoch_file_age_ms_)) {
      obsolete_files->push_back(candidate_file_path);
      Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
          "[pg] File number marker %s selected for deletion (dead epoch %s)",
          candidate_file_path.c_str(), epoch.c_str());
    } else {
      Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
          "[pg] Keeping file number marker %s: epoch %s has no threshold but "
          "marker is younger than %llu ms",
          candidate_file_path.c_str(), epoch.c_str(),
          static_cast<unsigned long long>(dead_epoch_file_age_ms_));
    }
  }
}

Status EloqPurger::DeleteObsoleteFiles(
    const std::vector<std::string> &obsolete_files, size_t *deleted,
    size_t *failures) {

  size_t to_delete = obsolete_files.size();
  if (max_deletions_per_cycle_ > 0 && to_delete > max_deletions_per_cycle_) {
    to_delete = max_deletions_per_cycle_;
    Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
        "[pg] Deletion cap reached: deleting %zu of %zu selected files this "
        "cycle; the remainder will be re-selected next cycle",
        to_delete, obsolete_files.size());
  }

  std::vector<std::string> paths_to_delete;
  paths_to_delete.reserve(to_delete);
  for (size_t i = 0; i < to_delete; ++i) {
    Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
        "[pg] Deleting obsolete file %s from destination bucket",
        obsolete_files[i].c_str());
    paths_to_delete.push_back(object_path_ + "/" + obsolete_files[i]);
  }

  IOStatus s = cfs_->GetStorageProvider()->DeleteCloudObjects(
      bucket_name_, paths_to_delete, deleted, failures);
  if (!s.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "[pg] Obsolete deletion failed: selected=%zu requested=%zu "
        "deleted=%zu failures=%zu: %s",
        obsolete_files.size(), to_delete, *deleted, *failures,
        s.ToString().c_str());
    return s;
  }

  Log(InfoLogLevel::DEBUG_LEVEL, cfs_->info_log_,
      "[pg] Obsolete deletion summary: selected=%zu requested=%zu "
      "deleted=%zu failures=%zu",
      obsolete_files.size(), to_delete, *deleted, *failures);
  return Status::OK();
}

// ------------- Main purger thread ------------- //
bool PrerequisitesMet(const CloudFileSystemImpl &cfs) {
  const CloudFileSystemOptions &cfs_opts = cfs.GetCloudFileSystemOptions();
  if (cfs_opts.src_bucket.IsValid() &&
      !cfs_opts.src_bucket.GetObjectPath().empty() &&
      cfs_opts.dest_bucket.IsValid() &&
      !cfs_opts.dest_bucket.GetObjectPath().empty() &&
      cfs_opts.src_bucket != cfs_opts.dest_bucket) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs.info_log_,
        "[pg] Single Object Path Purger is not running because the "
        "prerequisites are not met.");
    return false;
  }
  return true;
}

void CloudFileSystemImpl::Purger() {
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[pg] Single Object Path Purger thread started");

  if (!PrerequisitesMet(*this)) {
    return;
  }

  const auto periodicity_ms =
      GetCloudFileSystemOptions().purger_periodicity_millis;

  auto purger = std::make_unique<EloqPurger>(
      this, GetDestBucketName(), GetDestObjectPath(), false /*dry_run*/);

  while (true) {
    // Wait for next cycle or termination request
    std::unique_lock<std::mutex> lk(purger_lock_);
    purger_cv_.wait_for(lk, std::chrono::milliseconds(periodicity_ms),
                        [&]() { return !purger_is_running_; });
    if (!purger_is_running_) {
      break;  // shutdown requested
    }
    lk.unlock();  // release lock during IO work

    purger->RunSinglePurgeCycle();
  }

  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[pg] Single Object Path Purger thread exiting");
}

IOStatus CloudFileSystemImpl::FindObsoleteFiles(
    const std::string & /*bucket_name_prefix*/,
    std::vector<std::string> * /*pathnames*/) {
  return IOStatus::NotSupported(
      "Single Object Path Purger does not support FindObsoleteFiles");
}
IOStatus CloudFileSystemImpl::FindObsoleteDbid(
    const std::string & /*bucket_name_prefix*/,
    std::vector<std::string> * /*to_delete_list*/) {
  return IOStatus::NotSupported(
      "Single Object Path Purger does not support FindObsoleteDbid");
}

IOStatus CloudFileSystemImpl::extractParents(
    const std::string & /*bucket_name_prefix*/, const DbidList & /*dbid_list*/,
    DbidParents * /*parents*/) {
  return IOStatus::NotSupported(
      "Single Object Path Purger does not support extractParents");
}
}  // namespace ROCKSDB_NAMESPACE
