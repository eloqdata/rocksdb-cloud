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

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run eloq purger\n");
  return 1;
}
#else
#include <gflags/gflags.h>

#include <chrono>
#include <algorithm>
#include <cctype>
#include <iostream>
#include <memory>
#include <regex>
#include <string>

#include "cloud/eloq_purger.h"

#ifndef ROCKSDB_LITE
#ifdef USE_AWS
#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>

/**
 * @brief RAII wrapper for AWS SDK initialization and shutdown
 *
 * This class initializes the AWS SDK in its constructor and
 * shuts it down in its destructor, ensuring that shutdown happens
 * regardless of how the function exits.
 */
class AwsSdkManager {
 public:
  AwsSdkManager() {
    aws_options_.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Debug;
    Aws::InitAPI(aws_options_);
    initialized_ = true;
  }

  ~AwsSdkManager() {
    if (initialized_) {
      Aws::ShutdownAPI(aws_options_);
    }
  }

  // Prevent copying and moving
  AwsSdkManager(const AwsSdkManager &) = delete;
  AwsSdkManager &operator=(const AwsSdkManager &) = delete;
  AwsSdkManager(AwsSdkManager &&) = delete;
  AwsSdkManager &operator=(AwsSdkManager &&) = delete;

  Aws::SDKOptions aws_options_;
  bool initialized_ = false;
};
#endif
#endif

// Command line flags
DEFINE_string(s3_url, "", "S3 URL in format s3://bucket/path (required)");
DEFINE_bool(dry_run, false,
            "Dry run mode - list obsolete files but don't delete them");
DEFINE_string(aws_region, "ap-northeast-1", "AWS region (default: \"ap-northeast-1\")");
DEFINE_string(aws_access_key, "", "AWS Access Key ID");
DEFINE_string(aws_secret_key, "", "AWS Secret Access Key");
DEFINE_uint64(cloudmanifest_retention_ms, 3600 * 1000,
              "Time threshold in milliseconds for CLOUDMANIFEST file retention "
              "(default: 3600000 ms = 1 hour)");

/**
 * @brief Parse URL into bucket and object path components
 * @param url URL in format s3://bucket/path or
 * http(s)://server:port/bucket/path
 * @param bucket_name Output bucket name
 * @param object_path Output object path
 * @return true if parsing succeeded, false otherwise
 */
bool ParseS3Url(const std::string &url, std::string *endpoint,
                std::string *bucket_name, std::string *object_path) {
  // Regex for standard S3 URLs: s3://bucket-name/path
  std::regex s3_regex(R"(s3://([^/]+)(/.*)?)", std::regex_constants::icase);

  // Regex for HTTP/HTTPS Minio URLs: http(s)://server:port/bucket-name/path
  std::regex http_regex(R"((https?://[^/]+)/([^/]+)(/.*)?)",
                        std::regex_constants::icase);

  std::smatch matches;

  // Try to match as S3 URL
  if (std::regex_match(url, matches, s3_regex)) {
    *bucket_name = matches[1].str();
    *object_path = matches.size() > 2 ? matches[2].str() : "";
  }
  // Try to match as HTTP/HTTPS URL
  else if (std::regex_match(url, matches, http_regex)) {
    *endpoint = matches[1].str();
    *bucket_name = matches[2].str();
    *object_path = matches.size() > 3 ? matches[3].str() : "";
  }
  // No match
  else {
    return false;
  }

  // Remove leading slash from object path
  if (!object_path->empty() && (*object_path)[0] == '/') {
    *object_path = object_path->substr(1);
  }

  // Remove trailing slash from object path
  if (!object_path->empty() && object_path->back() == '/') {
    object_path->pop_back();
  }

  return true;
}

inline rocksdb::Status NewCloudFileSystem(
    const rocksdb::CloudFileSystemOptions &cfs_options,
    rocksdb::CloudFileSystem **cfs) {
  rocksdb::Status status;
  // Create a cloud file system
#if defined(USE_AWS)
  // AWS s3 file system
  status = rocksdb::CloudFileSystemEnv::NewAwsFileSystem(
      rocksdb::FileSystem::Default(), cfs_options, nullptr, cfs);
#elif defined(USE_GCP)
  // Google cloud storage file system
  status = rocksdb::CloudFileSystemEnv::NewGcpFileSystem(
      rocksdb::FileSystem::Default(), cfs_options, nullptr, cfs);
#else
  status = rocksdb::Status::NotSupported(
      "RocksDB Cloud not compiled with AWS or GCP support");
  *cfs = nullptr;
#endif
  return status;
};

std::string toLower(const std::string &str) {
  std::string lowerStr = str;
  std::transform(lowerStr.begin(), lowerStr.end(), lowerStr.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return lowerStr;
}

#ifdef USE_AWS
rocksdb::S3ClientFactory BuildS3ClientFactory(const std::string &endpoint) {
  return [endpoint](const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>
                        &credentialsProvider,
                    const Aws::Client::ClientConfiguration &baseConfig)
             -> std::shared_ptr<Aws::S3::S3Client> {
    // Check endpoint url start with http or https
    if (endpoint.empty()) {
      return nullptr;
    }

    std::string endpoint_url = toLower(endpoint);

    bool secured_url = false;
    if (endpoint_url.rfind("http://", 0) == 0) {
      secured_url = false;
    } else if (endpoint_url.rfind("https://", 0) == 0) {
      secured_url = true;
    } else {
      std::cerr << "Invalid S3 endpoint url" << std::endl;
      std::abort();
    }

    // Create a new configuration based on the base config
    Aws::Client::ClientConfiguration config = baseConfig;
    config.endpointOverride = endpoint_url;
    if (secured_url) {
      config.scheme = Aws::Http::Scheme::HTTPS;
    } else {
      config.scheme = Aws::Http::Scheme::HTTP;
    }

    // Create and return the S3 client
    if (credentialsProvider) {
      return std::make_shared<Aws::S3::S3Client>(
          credentialsProvider, config,
          Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
          true /* useVirtualAddressing */);
    } else {
      return std::make_shared<Aws::S3::S3Client>(config);
    }
  };
}
#endif

int main(int argc, char **argv) {
  // Initialize gflags and glog
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_s3_url.empty()) {
    std::cerr << "Error: --s3_url is required\n";
    std::cerr << "Usage: " << argv[0]
              << " --s3_url=s3://bucket/path [options]\n";
    std::cerr << "Options:\n";
    std::cerr << "  --dry_run=false                        Dry run mode - don't "
                 "actually delete files\n";
    std::cerr << "  --aws_region=ap-northeast-1            AWS region\n";
    std::cerr << "  --cloudmanifest_retention_ms=3600000   CLOUDMANIFEST retention time in milliseconds\n";
    return 1;
  }

  std::string endpoint, bucket_name, object_path;
  if (!ParseS3Url(FLAGS_s3_url, &endpoint, &bucket_name, &object_path)) {
    std::cerr << "Error: Invalid S3 URL format. Expected: s3://bucket/path\n";
    return 1;
  }

  std::cout << "Parsed S3 URL - Endpoint: " << endpoint
            << ", Bucket: " << bucket_name
            << ", Object Path: " << object_path << std::endl;

  std::cout << "Starting improved purger for S3 URL: " << FLAGS_s3_url
            << std::endl;
  std::cout << "Configuration - Dry Run: " << (FLAGS_dry_run ? "true" : "false")
            << ", AWS Region: " << FLAGS_aws_region << std::endl;

#ifndef ROCKSDB_LITE
#ifdef USE_AWS
  // Use RAII pattern for AWS SDK management
  AwsSdkManager aws_sdk_manager;
#endif
#endif

  bool res = true;
  try {
    // Create CloudFileSystemOptions
    ROCKSDB_NAMESPACE::CloudFileSystemOptions cfs_options;
#ifndef ROCKSDB_LITE
#ifdef USE_AWS
    if (FLAGS_aws_access_key.empty() || FLAGS_aws_secret_key.empty()) {
      std::cout << "No AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
                   "provided, use default credential provider"
                << std::endl;
      cfs_options.credentials.type = rocksdb::AwsAccessType::kUndefined;
    } else {
      cfs_options.credentials.InitializeSimple(FLAGS_aws_access_key,
                                               FLAGS_aws_secret_key);
    }

    rocksdb::Status status = cfs_options.credentials.HasValid();
    if (!status.ok()) {
      std::cerr << "Valid AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
                   "is required, error: "
                << status.ToString() << std::endl;
      return 1;
    }

#endif
#endif
    cfs_options.src_bucket.SetBucketName(bucket_name);
    // Clear prefix to replace the default prefix of "rockset."
    cfs_options.src_bucket.SetBucketPrefix("");
    cfs_options.src_bucket.SetObjectPath(object_path);
    cfs_options.src_bucket.SetRegion(FLAGS_aws_region);
    cfs_options.dest_bucket.SetBucketName(bucket_name);
    // Clear prefix to replace the default prefix of "rockset."
    cfs_options.dest_bucket.SetBucketPrefix("");
    cfs_options.dest_bucket.SetObjectPath(object_path);
    cfs_options.dest_bucket.SetRegion(FLAGS_aws_region);

#ifndef ROCKSDB_LITE
#ifdef USE_AWS
    if (!endpoint.empty()) {
      cfs_options.s3_client_factory = BuildS3ClientFactory(endpoint);
      cfs_options.use_aws_transfer_manager = false;
    }
#endif
#endif

    // Create CloudFileSystem
    rocksdb::CloudFileSystem *cfs;
    ROCKSDB_NAMESPACE::Status s = NewCloudFileSystem(cfs_options, &cfs);

    if (!s.ok()) {
      std::cerr << "Error: Failed to create CloudFileSystem: " << s.ToString()
                << std::endl;
      return 1;
    }

    std::unique_ptr<ROCKSDB_NAMESPACE::CloudFileSystem> cloud_fs;
    cloud_fs.reset(cfs);

    auto *cfs_impl =
        dynamic_cast<ROCKSDB_NAMESPACE::CloudFileSystemImpl *>(cloud_fs.get());
    if (!cfs_impl) {
      std::cerr << "Error: CloudFileSystem is not of expected type"
                << std::endl;
      return 1;
    }

    rocksdb::Options options;
    s = ROCKSDB_NAMESPACE::CreateLoggerFromOptions("eloq_purger_log", options,
                                                   &options.info_log);
    if (!s.ok()) {
      std::cerr << "Error: Failed to create logger: " << s.ToString()
                << std::endl;
      return 1;
    }
    cfs_impl->info_log_ = options.info_log;

    // Create and run improved purger
    ROCKSDB_NAMESPACE::EloqPurger purger(cfs_impl, bucket_name, object_path,
                                         FLAGS_dry_run,
                                         FLAGS_cloudmanifest_retention_ms);

    if (!ROCKSDB_NAMESPACE::PrerequisitesMet(*cfs_impl)) {
      std::cerr << "Error: Prerequisites for purger not met" << std::endl;
      return 1;
    }

    // Run purge cycles
    auto start_time = std::chrono::steady_clock::now();

    res = purger.RunSinglePurgeCycle();

    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);
    rocksdb::Log(rocksdb::InfoLogLevel::INFO_LEVEL, cfs_impl->info_log_,
                 "[pg] Purge cycle completed in %lld ms",
                 static_cast<long long>(duration.count()));
  } catch (const std::exception &e) {
    Log(rocksdb::InfoLogLevel::ERROR_LEVEL, nullptr, "Exception: %s", e.what());
    return 1;
  }

  // AWS SDK shutdown is handled by AwsSdkManager destructor
  return res ? 0 : 1;
}
#endif  // GFLAGS
