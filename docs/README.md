1space
------

1space is a way to share data between on-premises [OpenStack
Swift](https://github.com/openstack/swift)
deployments and [Amazon S3](https://aws.amazon.com/s3) (or S3-clones). The
project initially allowed for propagating any changes from Swift to S3 -- PUT,
DELETE, or POST -- in an asynchronous fashion. Since then, it has evolved to
support a limited set of data policy options to express the life cycle of the
data and transparent access to data stored in S3.

Notable features:

- asynchronously propagates object operations to Amazon S3, Google Cloud
  Storage&#185;, S3-clones, and other Swift Clusters
- allows for an "archival" mode after set time period
- on-line access to archived data through the Swift interface

&#185;Google Cloud Storage requires [interoperability
access](https://cloud.google.com/storage/docs/migrating#keys) to be enabled.

### Design overview

`1space` runs as a standalone process, intended to be used on Swift
container nodes. The container database provides the list of changes to the
objects in Swift (whether it was a metadata update, new object, or a deletion).

To provide on-line access to archived Swift objects, there is a Swift middleware
[component](https://github.com/swiftstack/1space/blob/master/s3_sync/shunt.py).
If a Swift container was configured to be archived, the middleware will query the
destination store for contents on a GET request, as well as splice the results
of LIST requests between the two stores.

There is no explicit coordination between the `1space` daemons.
Implicitly, they coordinate through their progress in the container database.
Each daemon looks up the number of container nodes in the system (with the
assumption that each node has a running daemon). Initially, each only handles
the objects assigned to it. Afterward, each one verifies that the other objects
have been processed, as well. This means that for each operation, there are
as many requests issued against the remote store as there are container
databases for the container. For example, in a three replica policy, there would
be three HEAD requests if an object PUT was performed (but only one PUT against
the remote store in the common case).

### How to setup and use

`1space` depends on:

- [container-crawler library](https://github.com/swiftstack/container-crawler)
- [botocore](https://github.com/swiftstack/botocore/tree/1.12.23.1)
  (unfortunately, we had to use our own fork, as a number of patches were
  difficult to merge upstream)
- [boto](https://github.com/boto/boto3)
- [eventlet](https://github.com/eventlet/eventlet)

All dependencies, can be installed automatically with `pip`:
```
pip install -r requirements.txt
```

Build the package to be installed on the nodes with:
```
python ./setup.py build sdist
```

Install the tarball with:
```
pip install dist/swift-s3-sync-<version>.tar.gz
```

After that, you should have the `swift-s3-sync` and `swift-s3-migrator`
executables available in `/usr/local/bin`.

Both `swift-s3-sync` and `swift-s3-migrator` must be invoked with a
configuration file, specifying which containers to watch, where the
contents should be placed, as well as a number of global settings. A
sample configuration file is in the
[repository](https://github.com/swiftstack/1space/blob/master/sync.json-sample).

Both of these tools run in the foreground, so starting each in their own
respective screen sessions is advisable.

To configure the Swift Proxy servers to use `1space` to redirect requests
for archived objects, you have to add the following to the proxy pipeline:
```
[filter:swift_s3_shunt]
use = egg:swift-s3-sync#cloud-shunt
conf_file = <Path to swift-s3-sync config file>
```

This middleware should be in the pipeline before the DLO/SLO middleware.

### Running tests

You can run all tests (flake8, unit, and integration) by executing:
```
./run_tests
```

A code line and branch HTML coverage report for the unit tests will get
written to `.coverhtml/`, and on macOS, you can view the results with
```
open ./.coverhtml/index.html
```

#### Unit tests

You can run just the unit tests with
```
./run_unit_tests
```

You can get a shell into the integration test container to run arbitrary
commands within it like so:

```
./run_bash
```


#### Integration tests

You can run the integration tests by running
```
./run_tests
```

Non-integration test time is so low that there isn't any reason to make
another command that only runs integration tests.

The integration tests need access to a Swift cluster and some sort of an S3
provider. Currently, they use a Docker container to provide Swift and are
configured to talk to [S3Proxy](https://github.com/andrewgaul/s3proxy).

The cloud sync configuration for the tests is defined in
`containers/swift-s3-sync/swift-s3-sync.conf`. In particular, there are mappings for S3
sync and archive policies and the same for Swift. The S3 mappings point to
S3Proxy running in the swift-s3-sync container, listening on port 10080.

You can run a subset of the integration tests in the container as well:

```
docker exec -e DOCKER=true  swift-s3-sync nosetests \
    /swift-s3-sync/test/integration/test_s3_sync:TestCloudSync.test_s3_sync
```

The tests create and destroy the Swift containers and S3 buckets configured in
the `swift-s3-sync.conf` file.  If you need to examine the state of a Swift
container or S3 bucket after the tests have finished executing, you can set
`NO_TEARDOWN=1` in the environment when you run the integration tests.  This
will make the `tearDownClass` method a NOOP.  It may also introduce test
failures if different subclasses of `TestCloudSyncBase` end up operating on the
same Swift containers or S3 buckets.

If you would like to examine the logs from each of the services, all logs are in
/var/log (e.g. /var/log/swift-s3-sync.log).

For the cloud-connector service, you view its logs by executing:
```
docker logs cloud-connector
```

### Building and Deploying cloud-connector

#### Build cloud-connector Docker Image

You build docker images for cloud-connector using the
`build_docker_image.py` script.  Many options have a sane default, but here is
an example invocation specifying all options and illustrating how you can
change the GitHub repository from which Swift is pulled:

```
cd containers/cloud-connector
./build_docker_image.py --swift-repo swiftstack/swift \
    --swift-tag ss-release-2.16.0.2 --swift-s3-sync-tag DEV \
    --config-bucket default-bucket-name-to-use \
    --repository swiftstack/cloud-connector
```

#### Publish the Image

If you want to publish the image after it's built, you can include the
`--push` flag, and `build_docker_image.py` will push the built image for
you.

You can also just push a built image using the `docker push` command.

#### Deploying cloud-connector

To deploy cloud-connector, you need the following inputs:

1. A healthy Swift cluster with CloudSync configured, deployed, and happy.
1. The image repository and tag of the cloud-connector Docker image you want to
   deploy.
1. A JSON-format database of authorized cloud-connector users and their
   corresponding secret S3-API keys **from the Swift cluster that the
   cloud-connector container will be pointed at**.  See
   [here](https://github.com/swiftstack/1space/blob/master/containers/swift-s3-sync/s3-passwd.json)
   for an example.
1. A copy of the CloudSync JSON-format config file as used inside the Swift
   cluster.
1. A configuration file for cloud-connector.  See
   [here](https://github.com/swiftstack/1space/blob/master/containers/swift-s3-sync/cloud-connector.conf)
   for an example.  Of particular interest are the `swift_baseurl` setting in
   the `[DEFAULT]` section, `conf_file` setting in the `[app:proxy-server]`
   section, and the `s3_passwd_json` setting in the
   `[filter:cloud-connector-auth]` section.  This config file will determine
   what port the cloud-connector service listens on _inside_ the container.
   How client traffic is delivered to that port depends on how the container
   is run.
1. A S3-API object storage service that is "local" to where the cloud-connector
   container will be deployed.  For Amazon EC2, that would be S3.  The endpoint of
   this storage service will be `CONF_ENDPOINT` later.  If S3 is used, then
   `CONF_ENDPOINT` does not need to be specified in the container environment.
1. A bucket in the S3-API object storage service that will hold the
   configuration files for the cloud-connector container.  This bucket name
   will be the `CONF_BUCKET` value later.
1. S3-API Credentials authorized to list and read objects in `CONF_BUCKET`.
   These will be the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` values
   later.  If you are using Amazon ECS with an IAM Role to provide access to
   `CONF_BUCKET`, then you do not need to specify `AWS_ACCESS_KEY_ID` or
   `AWS_SECRET_ACCESS_KEY` in the cloud-connector container environment.

With those in hand, perform these steps:

1. Upload the S3-API user database and CloudSync config files into the
   `CONF_BUCKET`.  Note their key names and make sure they are correct in the
   `cloud-connector.conf` file.
1. Upload the `cloud-connector.conf` config file into `CONF_BUCKET`.  The key
   name of this file in the S3-API object store will be the `CONF_NAME` value
   later.
1. Run the container with the following environment variables (unless earlier
   instructions specified that they were not necessary in your circumstances):
    ```
    CONF_BUCKET
    CONF_ENDPOINT
    CONF_NAME
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    ```
   Configuring networking to deliver client traffic to the bound port inside the
   container is outside the scope of this document and depends entirely on the
   container runtime environment you use.
