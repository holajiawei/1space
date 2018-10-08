## 0.1.47 (2018-10-08)

Improvements:

- Support the updated ContainerCrawler library.
- Added the ability to configure syslog for the 1space daemons
  (swift-s3-sync and swift-s3-migrator).

## 0.1.46 (2018-09-25)

Improvements:

- The segment container names are preserved during Swift-\>Swift sync or
  lifecycle data movements. This resolves an issue where a static large
  object could have its segments copied twice during full account data
  movement (once to place segments into a new container and one more time
  when copying the original segments container).
- When uploading SLOs, check whether a segment has already been uploaded.
  This reduces the amount of duplicated network traffic.
- When using the `remote_delete_after` option, segments are now set to
  expire 1 day after the manifest (to make sure manifests are not
  prematurely invalidated). A new configurable option
  `remote_delete_after_addition` can be used to change the 24 hours value to
  a different one.

Bug fixes:

- The shunt now returns the multi-part object from S3 even if the manifest
  is missing (the object is unable to be restored, however).

## 0.1.45.1 (2018-09-14)

Bug fixes:

- When uploading an SLO to S3 as a multi-part upload, cloud connector needs
  to reserve the S3 connection before making a Swift GET request, as
  otherwise the upload may encounter a Timeout and fail the entire MPU.

## 0.1.45 (2018-09-07)

Features:

- Added a new configuration option: `remote_delete_after`. This will cause
  the x-delete-after header to be set when uploading objects via
  swift-s3-sync. As it uses the x-delete-at header, it only works with Swift
  (and not with AWS S3, Google, or an S3 clone).
  THIS SHOULD BE USED WITH EXTREME CAUTION AS IT CAN RESULT IN DATA LOSS.

Bug fixes:

- Correctly detects if a Swift Static Large Object (SLO) has already been
  uploaded. Previously, SLO would always be re-uploaded if the remote
  segments container does not match the origin segments container.

## 0.1.44 (2018-08-27)

Features:

- Support for ContainerCrawler 0.0.14 (parallel enumeration of containers).

Bug fixes:

- Fixed a regression which caused `ChunkWriteTimeout` errors after reading
  an object from Swift.
- Properly support account overrides with Keystone (previously, the auth
  URL -- Keystone -- would be used as the storage URL).

## 0.1.43 (2018-08-14)

Features:

- Allow keystone credentials to be used (NOTE: does not work with storage
  URLs which do not use the same account - i.e. specifying `remote_account`).
- Add --prefix to verify.

Bug fixes:

- Fixed build\_docker\_image.py for cloud-connector.

## 0.1.42 (2018-08-03)

Bug fixes:

- Fixed an issue where a DLO that has a manifest that refers to the DLO
  itself would result in an infinite loop.

## 0.1.41 (2018-07-24)

Features:

- Allow migrations from a ProxyFS account. The migrator will ignore ProxyFS
  non-content specific, opaque ETags during migrations and the operator
  should validate content hashes of the migrated objects.
- Migrator will report the total size of objects copied during each pass as
  `bytes_count` field in the status file (and the corresponding
  `last_bytes_count`).

Bug fixes:

- A non-ASCII character in the `custom_prefix` option would result in a
  unicode error.
- Security: Secret key was previously logged at debug level in the Swift
  proxy server logs.
- Quiesced the shunt middleware to no longer log a notice that it is not
  configured on every Swift request to the proxy server.
- Migrator now uses the source object's X-Timestamp (if available), as
  opposed to the Last-Modified date. This ensures the exact match between
  the dates during migrations.
- If the migrator status file is corrupted, the migrator previously would
  not start. As of 0.1.41, the migrator will move the corrupted files and
  will restart its scan. The migrator also attempts to avoid corruption by
  using a temporary file, as opposed to writing to the status file directly.

## 0.1.40 (2018-06-29)

Bug fixes:

- Migrator shunt would double PUT objects in the destination cluster if the
  container already exists.
- Metadata selectors should be case-insensitive, as the HTTP headers are.

## 0.1.39 (2018-06-28)

Bug fixes:

- Fixed an issue with metadata keys that contain non-ASCII characters and
  are used for selecting objects to migrate.

## 0.1.38 (2018-06-27)

Features:

- 1space can now migrate objects based on their metadata. The metadata
  conditions can be a combination of AND, NOT, OR of metadata keys and
  values.

Bug fixes:

- Removed an extra GET request when migrating SLOs/DLOs.
- Fixed migrator statistics handling for source containers that were emptied
  and containers that were added or removed (causing a different migrator
  process to handle them).

## 0.1.37 (2018-06-12)

Features:

- Added a "cloud connector" feature. It allows for setting up a docker
  container in AWS that can serve S3 requests from S3, but fall back to the
  on-premises Swift cluster when necessary.

Bug fixes:

- The migrator honors the `poll_interval` setting set in the
  `migrator_settings` portion of the configuration file.

## 0.1.36 (2018-06-11)

Features:

- `merge_namespaces` flag now controls shunt behavior as opposed to just
  looking at the `propagate_delete` flag. This means that configuration
  MUST BE UPDATED to maintain same behavior.
- Migrator can now propagate account metadata from a swift source,
  including account ACL's.
- The shunt will now automatically detect changed configuration file and
  reload configuration.

Improvements:

- The migrator now initializes the provider loggers correctly for better/
  more logging.
- Some improvements and changes to the test container management.

Bug fixes:

- Migrator will not fail out on failed deletion of source object that is
  already deleted.

## 0.1.35 (2018-05-16)

Feature:

- Migrations can be configured to copy objects only older than a specified
  number of seconds. If this configuration option is not set, objects are
  copied immediately as before.

Bug fixes:

- A container with numerous dynamic large objects will no longer stall when
  attempting to copy its segments.
- The migrator will not stall when encountering a static large object with
  numerous segments.
- Workers are correctly passed to the migrator instance. Previously, the
  configuration option was ignored and we always defaulted to 10 workers.

## 0.1.34 (2018-05-11)

Bug fixes:

- The migrator never processes more than one page of objects. This bug was
  due to the fact that the status files would be overwritten every time the
  migrator completes a pass.
- Objects that have been copied as part of the migration may be removed if
  the listings are paginated. This is an issue with the marker not being set
  when listing objects in the destination blob store.

## 0.1.33 (2018-05-08)

Improvement:

- The migrator now tags and keeps track of containers that have been copied.
  If a container is removed from the source blob store, it will be removed
  from the destination (assuming it only contains objects copied from the
  source and no metadata has been changed).

Bug fixes:

- The migrator may remove objects previously copied when the paginated
  listings from the two blob stores do not align.
- Container and object metadata updates were not always propagated, as the
  migrator was considering the X-Timestamp date (created-at time), rather
  than the last-modified date.

## 0.1.32 (2018-04-26)

Bug fixes:

- The swift-s3-sync shunt no longer fails to load on older Swift (< 2.9).
- The migrator propagates the versioning headers on container metadata
  changes.

## 0.1.31 (2018-04-25)

Improvement:

- The swift-s3-sync migrator can migrate objects out of older (< 2.8) Swift
  clusters. Previously, there would be an error reported about a missing
  last-modified header.
- swift-s3-migrator will remove migrated objects if they have been deleted
  from the source cluster. This is done by tagging every object with
  internal metadata. If an object is mutated (via POST) or overwritten on
  the destination cluster, it will not be removed.
- Container metadata changes are propagated from the source to destination
  even after the initial creation of the container during a migration.

## 0.1.30 (2018-04-11)

Bug fixes:

- Migrations can now process accounts with more than 10000 containers (the
  default list limit in Swift).
- Large object manifests (both static and dynamic) are properly copied on
  migrations. Previously (in 0.1.29), the upload would result in a 422
  error, due to an ETag mismatch.
- Migration shunt supports HEAD and PUT against containers that have not yet
  been copied. In the case of HEAD, the headers from the source container
  are returned. In the latter, the container is create when the first PUT
  request against it is made.

## 0.1.29 (2018-04-09)

Features:

- Configuring a per-account migration (/\*) now propagates container
  listings (which allows calling GET on the account to get containers that
  may not have been yet migrated).

Bug fixes:

- Fixed unicode character handling in object metadata and container names
  for the migrator.
- Fixed handling of not-yet migrated containers when issuing GET requests
  against them.

## 0.1.28 (2018-04-02)

Features:

- Added the ability to change a container's name during migration.
- Handle Swift object versioning in migrations.
- Allow a custom prefix to be used when interacting with S3, instead of
  a hash of the local account and container followed by the account and
  container.

Bug fixes:

- Improved unicode support in user and account names.
- Properly use ETag to add data-integrity checks when uploading to Swift.
- Propagate POST in Swift-to-Swift mappings, both when syncing and migrating.
- Propagate DELETE requests back to origin when migrating. This prevents deleted
  objects from reappearing in listings.
- Fixed shunting migrations that map to all containers.

## 0.1.27 (2018-03-14)

Features:

- Implement support for migrating Dynamic Large Objects. This is done as a
  best-effort migration, where we list and copy all segments.

Bug fixes:

- Fixed a bug in the migrator, where a connection could be reused before all
  of the bytes have been read from the prior response, resulting in
  corruption.
- Ensure to close all connections to the remote providers after each
  migrator pass. When there are no objects to migrate, not closing
  connections may lead to exhausting the listening socket's queue.
- Static large objects are no longer considered different after the
  migrations if the manifests have the keys in a different order.

Improvement:

- Improved error reporting for missing containers in the migrator. A missing
  container no longer results in a traceback and prints a more informative
  message.

## 0.1.26 (2018-02-23)

Features:

- Status records generated from migrations configured for all buckets
  within a single account now include an `all_buckets` flag. Collecting
  agents may use it to perform aggregation.

Bug fixes:

- Fix a bug in migration status reporting which resulted in an unbounded
  growth of status files.

## 0.1.25 (2018-02-21)

Features:

- swift-s3-verify now makes assertions about the responses received, rather
  relying on tracebacks.
- swift-s3-verify now accepts a `--account` override when using the Swift
  protocol.
- The shunt now supports ProxyFS. Note that this requires two copies of
  the middleware in normal proxy pipeline: the first handles all
  non-ProxyFS accounts while the second handles *only* ProxyFS accounts.
  Further, the middleware is required in proxyfsd's no-auth pipeline.
- The shunt can now restore `206 Partial Content` responses that in fact
  contain the entire content.
- Keep migrator scan and moved counts for last run in status file
- The shunt now supports configured migrations.
- Swift Container ACLs are propagated to created containers during whole
  account migrations.

Bug fixes:

- Make progress even when other nodes are down.
- Prevent busy-loops on small, mostly-empty clusters.
- swift-s3-verify now works against AWS.
- Do translate headers twice from the remote to local. In the case of S3,
  this would mangle the ETag, causing the PUT to fail.
- Do not display objects twice in shunted listings for migrations or
  archive sync mappings after restore.
- Do not duplicate secrets in status file.
- Stale status entries for migrations are removed for unconfigured
  migrations startup.


## 0.1.24 (2018-02-01)

Bug fixes:

- Fixed shunted S3 listings to return Last-Modified date in the same format
  as Swift.
- Migration out of S3 buckets sets the X-Timestamp header from Last-Modified
  date (as X-Timestamp is absent).
- List entire S3 bucket contents when performing migration out of S3 (as
  opposed to assuming a namespace keyed off the hash).

## 0.1.23 (2018-01-31)

Features:

- Added a swift-s3-verify utility, which allows for validating a provider's
  credentials required by swift-s3-sync by performing
  PUT/GET/HEAD/COPY/DELETE requests against a user-supplied bucket
  (container).
- Added a swift-s3-migrator daemon, which allows for migrating objects from
  a given Swift cluster into the Swift cluster which has swift-s3-migrator
  deployed. The migration follows a pull model where the remote accounts and
  containers are periodically scanned for new content. The object metadata
  and timestamps are preserved in this process. Some limitations currently
  exist:
  - Dynamic Large Objects are not migrated
  - container ACLs are not propagated
  The migrator can be used against AWS S3 and S3-clones, as well. However,
  that functionality is not well tested.

Bug fixes:

- Resolved a possible issue where on a GET request through the swift-s3-sync
  shunt the underlying connection may be prematurely re-used.

## 0.1.22 (2017-12-05)

Improvement:

- Removed the dependency on the `container_crawler` library in the
  `sync_swift` module.

## 0.1.21 (2017-12-05)

Bug fixes:

- Fix the retries of uploads into Swift by adding support for the `reset()`
  method in the FilePutWrapper and SLOPutWrapper. Previously, Swift would
  never retry a failed upload.
- No longer issues a PUT object request if the segments container was
  missing and had to be created, but instead we wait until the following
  iteration to retry segment upload.

## 0.1.20 (2017-10-09)

Bug fixes:

- Update the integration test container dependencies (botocore and
  container-crawler).
- Improved error handling, by relying on ResponseMetadata:HTTPStatusCode in
  boto errors (as opposed to Error:Code, which may not always be present).
- Make Content-Type propagation work correctly. The prior attempt included
  it as a user metadata header, which is not what we should be doing.
- Fix the SLO upload against Google to include the SLO manifest.

## 0.1.19 (2017-10-04)

Features:

- Support restoring static large objects (SLO) from the remote store (which
  are stored there either as the result of a multipart upload or static
  large objects). The change requires the SLO manifest to be preserved and
  is now uploaded to S3 (and S3 clones) in the .manifests namespace (for
  that account and container).

Bug fixes:

- If an object is removed from the remote store, no longer fail with 404 Not
  Found (and continue to make progress).
- Propagate the Content-Type header to the remote store on upload.
- Fix up for the Swift 2.15.3 release (which repatriated a function we use).

Improvements:

- Small improvement to the testing container, which will no longer install
  recommended packages.

## 0.1.18 (2017-09-11)

Improvements:

- Reset the status row when the container policy changes.

## 0.1.17 (2017-09-06)

Features:

- Support restoring objects from the archive on a GET request. This only
  applies to regular objects. SLO (or multipart objects in S3) are not
  restored, as we do not have the object manifest.

Improvements:

- Added a docker container to be used for functional testing.

## 0.1.16 (2017-08-23)

Bug fixes:

- Fix invalid arguments in the call to `get_object_metadata`, which
  manifests during SLO metadata updates (when the object is not changed, but
  the metadata is).

Improvement:

- Lazy initialize public cloud sessions. This is useful when cloud sync
  reaches the steady state of checking for changes on an infrequently
  changed container. If there are no new objects to upload, no connections
  are created.

## 0.1.15 (2017-08-07)

Bug fixes:

- Fix listings where the last object has a unicode name.

## 0.1.14 (2017-08-01)

Bug fixes:

- Handle the "Accept" header correctly when constructing response listings.

## 0.1.13 (2017-07-13)

Bug fixes:

- Convert container names in the shunt to unicode strings. Otherwise, we
  fail with unicode containers, as they will be (unexpectedly) UTF-8
  encoded.

## 0.1.12 (2017-07-12)

Features:

- Added "content\_location" to JSON listings, which indicate where the object
  is stored (if not local).
- Support for HTTP/HTTPS proxy.
- Allowed log-level to be set through the config.

Bug fixes:

- Unicode characters are properly handled in account and container names when
  syncing to S3.
- Fixed paginated listings of archived objects from S3, where previously missing
  hashed prefix could cause the listing to never terminate.
- Worked around an issue with Google Cloud Storage, where encoding-type has been
  dropped as a valid parameter.
- Swift to Swift sync is properly supported in the "per-account" case now.
  Containers are auto-created in the remote store and the "cloud container" is
  used as the prefix for the container names.

## 0.1.11 (2017-06-22)

Bug fixes:

- When returning S3 objects or their metadata, we should unquote the ETag,
  as that would match the expected output from Swift.

## 0.1.10 (2017-06-21)

Bug fixes:

- The shunt was incorrectly referencing an exception attribute when
  encountering errors from Swift (e.http_status_code vs e.http_status).

## 0.1.9 (2017-06-21)

Bug fixes:

- The shunt should propagate errors encountered from S3 (e.g. 404) to the
  client, as opposed to always returning 502.

## 0.1.8 (2017-06-21)

Bug fixes:

- When syncing *all* containers in an account, the middleware needs to use
  the requested container when looking up the object in S3.

## 0.1.7 (2017-06-20)

Features:

- When uploading data to Amazon S3, AES256 Server-Side encryption will be
  used by default.
- Added middleware to allow for LIST and GET of objects that may have been
  archived to the remote bucket.

Bug fixes:

- Supply content-length with Swift objects on PUT. This ensures that we can
  upload a 0-sized object.
- Fixed Swift DELETE propagation. Previously, DELETE requests would fail due
  to a missing parameter.

Known issues:

- Sync all containers is currently not working as intended with Swift. It
  places all of the objects in one container. Will address in a subsequent
  release.

## 0.1.6 (2017-06-02)

Bug fixes:

- Fix an issue that prevents SLO uploads where opening a Swift connection
  before acquiring the S3 client may cause the Swift connection to be closed
  before any bytes are read.
- Do not serialize on a single Boto session.

## 0.1.5 (2017-06-01)

Bug fixes:

- Handle deleted objects when DELETE propagation is turned off correctly
  (should be a NOOP, but previously fell through to an attempted upload).
- Handle "409 Conflict" if attempting to DELETE an object, but it was
  actually already replaced with a new Timestamp.

## 0.1.4 (2017-05-30)

Features:

- Allow fine(r) grained control of object movement through `copy_after`,
  `retain_local`, and `propagate_delete` options. `copy_after` defers action
  on the rows until after a specified number of seconds has expired since
  the last object update; `retain_local` determines whether the object
  should be removed after copying to the remote store; `propagate_delete`
  controls whether DELETE requests against the cluster should show up on the
  remote endpoint. For example, one could configure Cloud Sync in archive
  mode, by turning off DELETE propagation and local copy retention, while
  defering the copy action for a set number of days until the archival date.

Bug fixes:

- A missing object should not generate an exception -- and stop Cloud Sync
  -- when attempting to upload. The exception will now be ignored.

## 0.1.3 (2017-05-08)

Improvement:

- Support new version of the ContainerCrawler (0.0.3).

## 0.1.2 (2017-04-19)

Features:

- Implemented support for syncing to Swift. Does not support DLO, but does
  have parity with S3 sync (propagates PUT, POST, DELETE, and supports
  SLOs). Swift can be enabled by passing the option "protocol" with the
  value "swift" in the configuration for a mapping.

Bug fixes:

- Fixed a broken import, which prevented the daemon from starting.
- Restricted the requests Sessions to be used once per worker (as opposed to
  being shared across workers).

## 0.1.1 (2017-03-22)

Improvements:

- Add boto3/botocore logging. This is particularly useful at debug level to
  observe the submitted requests/responses.
- Added a user agent string for the Google Partner Network.

## 0.1.0 (2017-03-20)

Features:

- Added SLO support in AWS S3 and Google Cloud Storage. For AWS S3 (and
  clones), SLO is converted to an MPU. Ranges are not supported in SLO
  manifest. If there is a mismatch between the smallest S3 part and Swift,
  i.e. allowing for a segment size < 5MB in Swift, the manifest will fail to
  upload. GCS uploads are converted to a single object, as it has a 5TB
  upload limit.

Improvements:

- Move s3-sync to using the ContainerCrawler framework.

## 0.0.9 (2016-12-12)

Bug fixes:

- Fix error handling, where some workers could quit without indicating
  completion of a task, causing the main process to hang.
- More unicode support fixes.

## 0.0.8 (2016-10-19)

Bug fixes:

- Properly encode unicode characters in object names and metadata.
- The `--once` option runs exactly once now.

## 0.0.7 (2016-09-28)

Features:

- Added support for non-Amazon providers, where we fall back to v2 signer.
- Glacier integration: objects are re-uploaded if their metadata changes, as
  the metadata is immutable in Glacier.

Bug fixes:

- Fixed object deletion. Previously, deletes would repeatedly fail and the
  daemon would not make progress.
- Fixed a bug where `upload_object()` would be called after
  `delete_object()` (even though the object does not exist)

## 0.0.6 (2016-09-05)

Features:

- Added concurrent uploads, through green threads.

Bug fixes:

- Avoid extra seeks when attempting to rewind the object which has not been
  read (i.e. calling seek(0) after opening the object).
- Close the object stream at the end of transfers.

## 0.0.5 (2016-08-16)

Features:

- Add support for AWS-v4 chunked transfers.

Improvements:

- Track the database ID and bucket name. If the DB drive crashes and it is
  rebuilt, this will cause the node to re-validate the data already
  uploaded.

- Exit with status "0" if the config file does not exist. This is important,
  as otherwise a process monitoring system may restart the daemon, on the
  assumption that it encountered an error.

Bug fixes:

- Configuring the cloud sync daemon for a new bucket resets the sync
  progress.

## 0.0.4 (2016-07-29)

Bug fixes:

- Account for S3 quoting etags when comparing to the Swift etag (which would
  previously result in repeated uploads).

## 0.0.3 (2016-07-26)

Improvements:

- Only use the account/container when computing the bucket prefix.
- Add retry on errors (as opposed to exiting).
- Early termination if there are no containers to sync.
