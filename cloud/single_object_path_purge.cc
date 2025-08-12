// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include <chrono>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cloud/cloud_manifest.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "cloud/purge.h"
#include "file/filename.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_storage_provider.h"

/**
 * This Purge implementation is just working for a very specific use case
 * where the source bucket and destination bucket are the same, as well as
 * the object path.
 */
namespace ROCKSDB_NAMESPACE {

void CloudFileSystemImpl::Purger() {
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[pg] Single Object Path Purger started");

  // verify the prerequisites of this purger
  const CloudFileSystemOptions &cfs_opts = GetCloudFileSystemOptions();
  if (cfs_opts.src_bucket.IsValid() &&
      !cfs_opts.src_bucket.GetObjectPath().empty() &&
      cfs_opts.dest_bucket.IsValid() &&
      !cfs_opts.dest_bucket.GetObjectPath().empty() &&
      cfs_opts.src_bucket != cfs_opts.dest_bucket) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[pg] Single Object Path Purger is not running because the "
        "prerequisites are not met.");
    return;
  }

  const std::string &dest_object_path = cfs_opts.dest_bucket.GetObjectPath();
  std::vector<std::string> files_to_delete;
  std::vector<std::string> cloud_manifest_files;
  std::vector<std::pair<std::string, CloudObjectInformation>> all_files;
  std::unordered_map<std::string, CloudObjectInformation>
      current_epoch_manifest_files;
  // all live files in the destination object path
  std::set<std::string> live_file_names;
  // Get live file names for a all cloud manifest files
  std::set<uint64_t> live_file_nums;

  while (true) {
    std::unique_lock<std::mutex> lk(purger_lock_);
    purger_cv_.wait_for(
        lk,
        std::chrono::milliseconds(
            GetCloudFileSystemOptions().purger_periodicity_millis),
        [&]() { return !purger_is_running_; });
    if (!purger_is_running_) {
      break;
    }

    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[pg] Single Object Path Purger started a new cycle");

    // Clear the file lists for the new cycle
    files_to_delete.clear();
    cloud_manifest_files.clear();
    all_files.clear();
    current_epoch_manifest_files.clear();
    live_file_names.clear();
    live_file_nums.clear();

    // Step 1: List all files in the destination object path
    // This must be the 1st step to ensure the vector of live files
    // being listed in the next steps is a supper set of live files included in
    // the all_files vector. So, we can safely delete files that are not in the
    // live files list.
    IOStatus s = GetStorageProvider()->ListCloudObjects(
        GetDestBucketName(), dest_object_path, &all_files);

    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[pg] Failed to list files in destination object path %s: %s",
          dest_object_path.c_str(), s.ToString().c_str());
      continue;
    }

    // Step 2: List all CLOUDMANIFEST files in the destination bucket
    s = GetStorageProvider()->ListCloudObjectsWithPrefix(
        GetDestBucketName(), GetDestObjectPath(), "CLOUDMANIFEST",
        &cloud_manifest_files);

    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[pg] Failed to list cloud manifest files in bucket %s: %s",
          GetDestBucketName().c_str(), s.ToString().c_str());
      continue;
    }

    for (const auto &f : cloud_manifest_files) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[pg] Found cloud manifest file %s", f.c_str());
    }

    // Step 3: Load all CLOUDMANIFEST files
    std::unordered_map<std::string, std::unique_ptr<CloudManifest>>
        cloud_manifests;

    const FileOptions file_opts;
    IODebugContext *dbg = nullptr;
    for (const auto &cloud_manifest_file : cloud_manifest_files) {
      std::string cloud_manifest_file_path =
          GetDestObjectPath() + pathsep + cloud_manifest_file;
      std::unique_ptr<FSSequentialFile> file;
      s = NewSequentialFileCloud(GetDestBucketName(), cloud_manifest_file_path,
                                 file_opts, &file, dbg);
      if (!s.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[pg] Failed to open cloud manifest file %s: %s",
            cloud_manifest_file.c_str(), s.ToString().c_str());
        continue;
      }

      std::unique_ptr<CloudManifest> cloud_manifest;
      s = CloudManifest::LoadFromLog(
          std::unique_ptr<SequentialFileReader>(
              new SequentialFileReader(std::move(file), cloud_manifest_file)),
          &cloud_manifest);
      if (!s.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[pg] Failed to load cloud manifest from file %s: %s",
            cloud_manifest_file.c_str(), s.ToString().c_str());
        continue;
      }

      cloud_manifests[cloud_manifest_file] = std::move(cloud_manifest);
    }

    for (const auto &cloud_manifest : cloud_manifests) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[pg] Loaded cloud manifest file %s with current epoch %s",
          cloud_manifest.first.c_str(),
          cloud_manifest.second->GetCurrentEpoch().c_str());
    }

    // Step 4: Compile a list of all live files associated with these
    // cloud_manifest_files
    std::unique_ptr<ManifestReader> manifest_reader(new ManifestReader(
        info_log_, this, cfs_opts.dest_bucket.GetBucketName()));

    for (auto &cloud_manifest : cloud_manifests) {
      live_file_nums.clear();

      const std::string &cloud_manifest_file = cloud_manifest.first;
      const auto &cloud_manifest_ptr = cloud_manifest.second;

      // Read current epoch manifest file information
      std::string current_epoch = cloud_manifest_ptr->GetCurrentEpoch();
      auto manifest_file =
          ManifestFileWithEpoch(dest_object_path, current_epoch);

      CloudObjectInformation manifest_file_info;
      s = GetStorageProvider()->GetCloudObjectMetadata(
          GetDestBucketName(), manifest_file, &manifest_file_info);

      if (!s.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[pg] Failed to get metadata for manifest file %s: %s",
            manifest_file.c_str(), s.ToString().c_str());
        continue;
      }

      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[pg] Current epoch Manifest file %s of CloudManifest %s has size "
          "%lu and content hash %s and timestamp %lu",
          manifest_file.c_str(), cloud_manifest_file.c_str(),
          manifest_file_info.size, manifest_file_info.content_hash.c_str(),
          manifest_file_info.modification_time);
      current_epoch_manifest_files[current_epoch] = manifest_file_info;

      s = manifest_reader->GetLiveFiles(dest_object_path, current_epoch,
                                        &live_file_nums);
      if (!s.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[pg] Failed to get live files from cloud manifest file %s: %s",
            cloud_manifest_file.c_str(), s.ToString().c_str());
        continue;
      }

      // map live file numbers to live file names
      for (const auto &num : live_file_nums) {
        std::string file_name = MakeTableFileName(num);
        file_name = this->RemapFilenameWithCloudManifest(
            file_name, cloud_manifest_ptr.get());
        live_file_names.insert(file_name);
        Log(InfoLogLevel::INFO_LEVEL, info_log_,
            "[pg] Live file %s found in cloud manifest %s", file_name.c_str(),
            cloud_manifest_file.c_str());
      }
    }

    // Step 4: Identify obsolete files that are not in the live files list
    for (const auto &candiate : all_files) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[pg] Checking candidate file %s", candiate.first.c_str());
      const std::string &candiate_file_path = candiate.first;
      const std::string candiate_file_epoch = GetEpoch(candiate_file_path);
      const CloudObjectInformation &candiate_file_info = candiate.second;
      uint64_t candidate_modification_time =
          candiate_file_info.modification_time;
      uint64_t current_epoch_manifest_modification_time = UINT64_MAX;
      // find the epoch of the candidate file
      if (current_epoch_manifest_files.find(candiate_file_epoch) !=
          current_epoch_manifest_files.end()) {
        // This file is part of the current epoch manifest, so it is not
        // obsolete
        current_epoch_manifest_modification_time =
            current_epoch_manifest_files[candiate_file_epoch].modification_time;
      }

      // Check if the candidate file is a live file
      // If not, and it is a sst file, and its modification time is
      // earlier than the current epoch manifest file's modification time
      if (live_file_names.find(candiate_file_path) == live_file_names.end() &&
          ends_with(RemoveEpoch(candiate_file_path), ".sst")) {
        if (candidate_modification_time <
            current_epoch_manifest_modification_time) {
          files_to_delete.push_back(candiate_file_path);
          Log(InfoLogLevel::INFO_LEVEL, info_log_,
              "[pg] Candidate file %s is obsolete and will be deleted",
              candiate_file_path.c_str());
        } else {
          Log(InfoLogLevel::INFO_LEVEL, info_log_,
              "[pg] Candidate file %s is not obsolete because its modification "
              "time %lu is later than the current epoch manifest file's "
              "modification time %lu",
              candiate_file_path.c_str(), candidate_modification_time,
              current_epoch_manifest_modification_time);
        }
      }
    }

    // Step 5: Delete obsolete files
    for (const auto &file_to_delete : files_to_delete) {
      std::string file_path = dest_object_path + pathsep + file_to_delete;
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[pg] Deleting obsolete file %s from destination bucket",
          file_to_delete.c_str());
      s = GetStorageProvider()->DeleteCloudObject(GetDestBucketName(),
                                                  file_path);
      if (!s.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[pg] Failed to delete obsolete file %s: %s",
            file_path.c_str(), s.ToString().c_str());
      }
    }
  }
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
#endif  // ROCKSDB_LITE
