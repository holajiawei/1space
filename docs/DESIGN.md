## 1space Overview

### Components

1space is roughly composed of four parts:

1. Sync/Lifecycle
2. Migrator (and Metadata Migrator)
3. Shunt (Swift middleware)
4. Cloud Connector

At the high level, Sync/Lifecycle is used to push objects from the Swift cluster
into a remote blob store. The Migrator is used to copy objects from a remote
blob store into the Swift cluster. The Shunt is a piece of Swift middleware
which allows 1space to merge namespaces across blob stores. The Cloud Connector
is used to present an endpoint in a public cloud (e.g. AWS), which can be used
to transparently access the Swift cluster.

### Common abstractions

All components rely on the common *Provider* abstraction. The parent (abstract)
class is
[`BaseSync`](https://github.com/swiftstack/1space/blob/master/s3_sync/base_sync.py).
Each child class implements the following methods:
* `delete_object()`
* `get_object()`
* `head_account()`
* `head_bucket()`
* `head_object()`
* `list_buckets()`
* `list_objects()`
* `post_object()`
* `put_object()`
* `shunt_delete()`
* `shunt_object()`
* `shunt_post()`
* `update_metadata()`
* `upload_object()`

These roughly map onto the common blob store operations (HEAD, GET, PUT, POST,
DELETE). The `shunt_*` methods are for implementing the shunt requests for
merged namespaces.

Currently, there are two classes inheriting from this abstract class:
* `SyncS3`
* `SyncSwift`

They are used for interacting with S3 and Swift blob stores, respectively.

### Sync/Lifecycle

Sync/Lifecycle refers to either implementing a sync or lifecycle (archive) of
Swift objects into a remote blob store (e.g. AWS S3, Google Cloud Storage, or
another Swift cluster). The daemon implementing these policies is called
`swift-s3-sync` and leverages the
[ContainerCrawler](https://github.com/swiftstack/container-crawler) library. The
library trawls the specified container databases, looking for new objects or
metadata updates that have not yet been processed.

The handling of object changes (create, update, or delete) is implemented in
[`sync_container.py`](https://github.com/swiftstack/1space/blob/master/s3_sync/sync_container.py).

### Migrator

The Migrator runs in the Swift cluster and copies objects from a remote blob
store. As such, it has to rely on public APIs and currently enumerates the
contents of the remote blob store, comparing them to the Swift contents.

The migrator is implemented in
[`migrator.py`](https://github.com/swiftstack/1space/blob/master/s3_sync/sync_container.py).

#### Metadata Migrator

The Metadata Migrator peforms a similar function to the migrator, but instead
of copying remote objects to a local blob store, it indexes remote object
metadata to a local Elasticsearch cluster.

It shares much of its implementation with the migrator. You can find it in
[`metadata_migrator.py`](https://github.com/swiftstack/1space/blob/master/s3_sync/metadata_migrator.py).

### Shunt (Swift middleware)

The shunt is invoked on requests that cannot be satisfied from the Swift
cluster, but have an associated remote blob store bucket/container. It is also
invoked when listing a Swift container.

For container GET requests, the shunt submits a corresponding GET to the remote
blob store, splicing the responses in an alpha-numeric sorted order. The
`content_location` field in the JSON response indicates whether an object
exists in one, the other, or both locations.

For object HEAD/GET requests, if the object is not found in the Swift cluster,
the Shunt attempts to retrieve the object from the remote blob store (optionally
issuing a PUT into Swift while returning content to the client).

The object PUT path is only utilized during migrations when a container may not
yet exist in the Swift cluster.

The code for shunt exists primarily in:
[`shunt.py`](https://github.com/swiftstack/1space/blob/master/s3_sync/shunt.py).

### Integration testing

We use Docker containers for integration tests. Namely, we build a `swift-s3-sync`
container based on `docker-swift` and a `cloud-connector` container, based on
Alpine Linux. More details are in the
[README](https://github.com/swiftstack/1space/blob/master/README.md#integration-tests).

### Code Organization

The 1space code has fairly flat directory structure. The top level has a few
convenience scripts (`run_tests`, for instance), `requirements.txt`, and the
`setup.py` files.  The Docker containers needed for testing are in
`./containers`.  Note that `containers/swift-s3-sync` includes several config
files used by the integration tests in addition to the launch scripts and
patches. The bulk of the code is in the `s3_sync` directory.  `scripts` contains
a couple of convenience scripts used by the `run_*` scripts in the root of the
project. The unit and integration test suites can be found under the test
directory.
