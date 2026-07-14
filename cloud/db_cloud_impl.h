// Copyright (c) 2017 Rockset

#pragma once

#ifndef ROCKSDB_LITE
#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/db.h"
#include "port/port_posix.h"
#include "monitoring/instrumented_mutex.h"

namespace ROCKSDB_NAMESPACE {

class Env;
class CloudFileSystemImpl;
class FileNumberGuardPublisher;

//
// All writes to this DB can be configured to be persisted
// in cloud storage.
//
class DBCloudImpl : public DBCloud {
  friend DBCloud;

 public:
  virtual ~DBCloudImpl();
  Status Savepoint() override;

  Status CheckpointToCloud(const BucketOptions& destination,
                           const CheckpointToCloudOptions& options) override;

  Status WarmUp(size_t max_warmup_threads) override;

  Status GetCurrentEpoch(std::string *epoch) const override;

 protected:
  // Non-owning; the environment keeps the CloudFileSystem alive through DB
  // teardown.
  CloudFileSystemImpl* cfs_;

 private:
  Status DoCheckpointToCloud(const BucketOptions& destination,
                             const CheckpointToCloudOptions& options);

  // Maximum manifest file size
  static const uint64_t max_manifest_file_size = 4 * 1024L * 1024L;

  DBCloudImpl(DB *db, std::unique_ptr<Env> local_env, CloudFileSystemImpl *cfs,
              std::shared_ptr<FileNumberGuardPublisher> guard_publisher);

  std::unique_ptr<Env> local_env_;
  // Identifies the publisher installed by this DB instance. Teardown must not
  // stop a publisher installed by a later DBCloud::Open on the same CFS.
  std::shared_ptr<FileNumberGuardPublisher> guard_publisher_;

  std::atomic<bool> warm_up_is_running_{false};
  std::vector<port::Thread> warm_up_threads_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
