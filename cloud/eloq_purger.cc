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
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "file/filename.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

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
  close(fd);  // We will open it later for reading
  std::string temp_file_path = tmp_template;

  rocksdb::IOStatus s = cfs_->GetStorageProvider()->GetCloudObject(
      bucket_name_, object_key, temp_file_path);

  if (!s.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "Failed to read smallest file number from S3: %s, object_key: %s, ",
        s.ToString().c_str(), object_key.c_str());
    *file_number = std::numeric_limits<uint64_t>::min();
    if (s.IsNotFound()) {
      return Status::NotFound("Smallest file number object not found");
    }
    return Status::IOError(s.ToString());
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
  std::ostringstream oss;
  oss << s3_object_path_;
  if (!s3_object_path_.empty() && s3_object_path_.back() != '/') {
    oss << "/";
  }
  oss << "smallest_new_file_number-" << epoch_;
  return oss.str();
}

EloqPurger::EloqPurger(CloudFileSystemImpl *cfs, const std::string &bucket_name,
                       const std::string &object_path, bool dry_run,
                       uint64_t cloudmanifest_retention_ms)
    : cfs_(cfs),
      bucket_name_(bucket_name),
      object_path_(object_path),
      dry_run_(dry_run),
      cloudmanifest_retention_ms_(cloudmanifest_retention_ms) {}

bool EloqPurger::RunSinglePurgeCycle() {
  PurgerCycleState state;

  Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
      "[pg] Starting purge cycle for %s/%s", bucket_name_.c_str(),
      object_path_.c_str());

  // list all files in the object path, for fetch all obsolete files and live files
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

  // Enhanced selection with file number checking
  SelectObsoleteSSTFilesWithThreshold(state.all_files, state.live_file_names,
                                      state.file_number_thresholds,
                                      &state.obsolete_files);

  // Select obsolete manifest files
  SelectObsoleteManifestFiles(state.all_files,
                              state.current_epoch_manifest_files,
                              &state.obsolete_files);

  // Select obsolete CLOUDMANIFEST files
  SelectObsoleteCloudManifetFiles(state.all_files, state.cloudmanifests,
                                  state.current_epoch_manifest_files,
                                  &state.obsolete_files);

  if (dry_run_) {
    Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
        "[pg] DRY RUN: Would delete %zu files", state.obsolete_files.size());
    for (const auto &file : state.obsolete_files) {
      Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
          "[pg] DRY RUN: Would delete %s", file.c_str());
    }
  } else {
    DeleteObsoleteFiles(state.obsolete_files);
  }

  Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
      "[pg] Purge cycle summary: total_files=%zu manifests=%zu "
      "live_files=%zu obsolete_selected=%zu thresholds_loaded=%zu",
      state.all_files.size(), state.cloudmanifests.size(),
      state.live_file_names.size(), state.obsolete_files.size(),
      state.file_number_thresholds.size());

  return true;
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
    const PurgerFileNumberThresholds &thresholds,
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
      Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
          "[pg] threshold is 0 for epoch %s, purge is blocked intentionally. "
          "%s is skipped.",
          candidate_epoch.c_str(), candidate_file_path.c_str());
    }
  }
}

void EloqPurger::SelectObsoleteManifestFiles(
    const PurgerAllFiles &all_files,
    const PurgerEpochManifestMap &current_epoch_manifest_infos,
    std::vector<std::string> *obsolete_files) {
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

    obsolete_files->push_back(candidate_file_path);
    Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
        "[pg] Manifest file %s selected for deletion",
        candidate_file_path.c_str());
  }
}

Status EloqPurger::GetS3CurrentTime(uint64_t *current_time) {
  // Create a temporary local file
  char tmp_template[] = "/tmp/purger_s3_time_XXXXXX";
  int fd = mkstemp(tmp_template);
  if (fd == -1) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "[pg] Failed to create temp file for S3 time check: %s",
        strerror(errno));
    return Status::IOError("Failed to create temp file");
  }

  std::string temp_local_path = tmp_template;

  // Write a small amount of data to the file
  const char *content = "time_check";
  ssize_t bytes_written = write(fd, content, strlen(content));
  close(fd);

  if (bytes_written < 0) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "[pg] Failed to write to temp file for S3 time check: %s",
        strerror(errno));
    std::remove(temp_local_path.c_str());
    return Status::IOError("Failed to write to temp file");
  }

  // Extract just the filename from the local path and use it for S3
  std::string temp_filename = temp_local_path.substr(temp_local_path.find_last_of('/') + 1);
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

  // Get the metadata to read the timestamp
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
        "[pg] Failed to delete temp file from S3: %s (non-fatal)",
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

void EloqPurger::SelectObsoleteCloudManifetFiles(
    const PurgerAllFiles &all_files,
    const PurgerCloudManifestMap &cloudmanifests,
    const PurgerEpochManifestMap &current_epoch_manifest_infos,
    std::vector<std::string> *obsolete_files) {
  // Struct to represent CLOUDMANIFEST file information
  struct CloudManifestFileInfo {
    uint64_t term;
    std::string file_path;
    uint64_t current_manifest_timestamp;
    std::string epoch;

    CloudManifestFileInfo(uint64_t t, const std::string& path,
                          uint64_t manifest_ts, const std::string& ep)
        : term(t), file_path(path),
          current_manifest_timestamp(manifest_ts), epoch(ep) {}
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

    // Look up the current manifest file timestamp for this epoch
    auto manifest_info_it = current_epoch_manifest_infos.find(current_epoch);
    if (manifest_info_it == current_epoch_manifest_infos.end()) {
      Log(InfoLogLevel::WARN_LEVEL, cfs_->info_log_,
          "[pg] No current manifest info found for epoch %s, skipping CLOUDMANIFEST %s",
          current_epoch.c_str(), candidate_file_path.c_str());
      continue;
    }

    uint64_t current_manifest_timestamp = manifest_info_it->second.modification_time;

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
                                            current_manifest_timestamp,
                                            current_epoch);

    Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
        "[pg] Found CLOUDMANIFEST file %s with postfix='%s', term=%llu, "
        "manifest_timestamp=%llu, epoch=%s",
        candidate_file_path.c_str(), postfix.c_str(),
        static_cast<unsigned long long>(term),
        static_cast<unsigned long long>(current_manifest_timestamp),
        current_epoch.c_str());
  }

  // Get current timestamp from S3
  uint64_t current_time = 0;
  Status time_status = GetS3CurrentTime(&current_time);
  if (!time_status.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
        "[pg] Failed to get S3 current time, aborting CLOUDMANIFEST cleanup: %s",
        time_status.ToString().c_str());
    return;
  }

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
    auto max_it = std::max_element(files.begin(), files.end(),
        [](const CloudManifestFileInfo &a, const CloudManifestFileInfo &b) {
          return a.term < b.term;
        });

    uint64_t max_term = max_it->term;

    Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
        "[pg] CLOUDMANIFEST group postfix='%s': largest term=%llu",
        postfix.c_str(), static_cast<unsigned long long>(max_term));

    // Check each file in the group
    for (const auto &file_info : files) {
      // Keep the file with the largest term (current CLOUDMANIFEST)
      if (file_info.term == max_term) {
        Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
            "[pg] Keeping CLOUDMANIFEST file %s (largest term in group)",
            file_info.file_path.c_str());
        continue;
      }

      // Compare current MANIFEST file timestamp with S3 current time
      // If MANIFEST timestamp is earlier than S3 current time by the retention threshold or more, delete the CLOUDMANIFEST
      if (current_time > file_info.current_manifest_timestamp &&
          (current_time - file_info.current_manifest_timestamp) >= retention_threshold_ms) {
        obsolete_files->push_back(file_info.file_path);
        Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
            "[pg] CLOUDMANIFEST file %s selected for deletion "
            "(term=%llu, manifest_timestamp=%llu, s3_current_time=%llu, "
            "epoch=%s, time_diff=%llu ms)",
            file_info.file_path.c_str(),
            static_cast<unsigned long long>(file_info.term),
            static_cast<unsigned long long>(file_info.current_manifest_timestamp),
            static_cast<unsigned long long>(current_time),
            file_info.epoch.c_str(),
            static_cast<unsigned long long>(current_time - file_info.current_manifest_timestamp));
      } else {
        uint64_t time_diff = (current_time > file_info.current_manifest_timestamp)
            ? (current_time - file_info.current_manifest_timestamp) : 0;
        Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
            "[pg] Keeping CLOUDMANIFEST file %s "
            "(term=%llu, manifest_timestamp=%llu, s3_current_time=%llu, "
            "epoch=%s, time_diff=%llu ms < retention threshold %llu ms)",
            file_info.file_path.c_str(),
            static_cast<unsigned long long>(file_info.term),
            static_cast<unsigned long long>(file_info.current_manifest_timestamp),
            static_cast<unsigned long long>(current_time),
            file_info.epoch.c_str(),
            static_cast<unsigned long long>(time_diff),
            static_cast<unsigned long long>(retention_threshold_ms));
      }
    }
  }
}

void EloqPurger::DeleteObsoleteFiles(
    const std::vector<std::string> &obsolete_files) {
  size_t deleted = 0;
  size_t failures = 0;

  for (const auto &file_to_delete : obsolete_files) {
    std::string file_path = object_path_ + "/" + file_to_delete;
    Log(InfoLogLevel::INFO_LEVEL, cfs_->info_log_,
        "[pg] Deleting obsolete file %s from destination bucket",
        file_to_delete.c_str());

    IOStatus s =
        cfs_->GetStorageProvider()->DeleteCloudObject(bucket_name_, file_path);
    if (!s.ok()) {
      ++failures;
      Log(InfoLogLevel::ERROR_LEVEL, cfs_->info_log_,
          "[pg] Failed to delete obsolete file %s: %s", file_path.c_str(),
          s.ToString().c_str());
    } else {
      ++deleted;
    }
  }

  Log(InfoLogLevel::DEBUG_LEVEL, cfs_->info_log_,
      "[pg] Obsolete deletion summary: requested=%zu deleted=%zu "
      "failures=%zu",
      obsolete_files.size(), deleted, failures);
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
