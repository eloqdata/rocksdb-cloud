// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include "cloud/purge.h"

#include <chrono>
#include <string>
#include <utility>
#include <vector>

#include "cloud/cloud_manifest.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
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

    // Purge the single object path

    // Step 1: List all CLOUDMANIFEST files in the destination bucket
    std::vector<std::string> cloud_manifest_files;
    IOStatus s = GetStorageProvider()->ListCloudObjectsWithPrefix(
        GetDestBucketName(), GetDestObjectPath(), "CLOUDMANIFEST",
        &cloud_manifest_files);

    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[pg] Failed to list cloud manifest files in bucket %s: %s",
          GetDestBucketName().c_str(), s.ToString().c_str());
      continue;
    }

    std::unordered_map<std::string, std::unique_ptr<CloudManifest>>
        cloud_manifests;

    const FileOptions file_opts;
    IODebugContext *dbg = nullptr;
    for (const auto &cloud_manifest_file : cloud_manifest_files) {
      std::unique_ptr<FSSequentialFile> file;
      s = NewSequentialFileCloud(GetDestBucketName(), cloud_manifest_file,
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

    // Step 2: Compile a list of all live files associated with these
    // cloud_manifest_files
    std::unique_ptr<ManifestReader> manifest_reader(new ManifestReader(
        info_log_, this, cfs_opts.dest_bucket.GetBucketName()));
    const std::string &dest_object_path = cfs_opts.dest_bucket.GetObjectPath();

    // all live files in the destination object path
    std::set<std::string> live_file_names;

    // live file numbers for a given cloud manifest file
    std::set<uint64_t> live_file_nums;
    for (auto &cloud_manifest : cloud_manifests) {
      live_file_nums.clear();

      const std::string &cloud_manifest_file = cloud_manifest.first;
      const auto &cloud_manifest_ptr = cloud_manifest.second;

      s = manifest_reader->GetLiveFiles(dest_object_path,
                                        cloud_manifest_ptr->GetCurrentEpoch(),
                                        &live_file_nums);
      if (!s.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[pg] Failed to get live files from cloud manifest file %s: %s",
            cloud_manifest_file.c_str(), s.ToString().c_str());
        continue;
      }

      for (const auto &num : live_file_nums) {
        std::string file_name = MakeTableFileName(dest_object_path, num);
        file_name = this->RemapFilename(file_name);
        // If the file name is already in the live_file_names,
        // assert that it is a fault
        assert(live_file_names.find(file_name) == live_file_names.end() &&
               "Duplicate file name found in live files");
        live_file_names.insert(file_name);
      }

      // Step 3: List all files in the destination object path
      std::vector<std::string> all_files;

      s = GetStorageProvider()->ListCloudObjects(GetDestBucketName(),
                                                 dest_object_path, &all_files);

      if (!s.ok()) {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[pg] Failed to list files in destination object path %s: %s",
            dest_object_path.c_str(), s.ToString().c_str());
        continue;
      }

      // Step 4: Identify files that are not in the live files list
      std::vector<std::string> files_to_delete;
      for (const auto &candiate : all_files) {
        if (live_file_names.find(candiate) == live_file_names.end() &&
            ends_with(candiate, ".sst")) {
          Log(InfoLogLevel::WARN_LEVEL, info_log_,
              "[pg] File %s marked for deletion", candiate.c_str());
          files_to_delete.push_back(candiate);
        }
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

IOStatus
CloudFileSystemImpl::extractParents(const std::string & /*bucket_name_prefix*/,
                                    const DbidList & /*dbid_list*/,
                                    DbidParents * /*parents*/) {
  return IOStatus::NotSupported(
      "Single Object Path Purger does not support extractParents");
}

} // namespace ROCKSDB_NAMESPACE
#endif // ROCKSDB_LITE
