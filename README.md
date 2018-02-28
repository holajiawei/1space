Swift-S3 Sync
-------------

Swift-S3 Sync is a way to share data between on-premises [OpenStack
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

`swift-s3-sync` runs as a standalone process, intended to be used on Swift
container nodes. The container database provides the list of changes to the
objects in Swift (whether it was a metadata update, new object, or a deletion).

To provide on-line access to archived Swift objects, there is a Swift middleware
[component](https://github.com/swiftstack/swift-s3-sync/blob/master/s3_sync/shunt.py). If a
Swift container was configured to be archived, the middleware will query the
destination store for contents on a GET request, as well as splice the results
of LIST requests between the two stores.

There is no explicit coordination between the `swift-s3-sync` daemons.
Implicitly, they coordinate through their progress in the container database.
Each daemon looks up the number of container nodes in the system (with the
assumption that each node has a running daemon). Initially, each only handles
the objects assigned to it. Afterward, each one verifies that the other objects
have been processed, as well. This means that for each operation, there are
as many requests issued against the remote store as there are container
databases for the container. For example, in a three replica policy, there would
be three HEAD requests if an object PUT was performed (but only one PUT against
the remote store in the common case).

`proxymc` runs as a standalone process, intended to be used in a public cloud
somewhere. It provides a Swift API endpoint with access to the shared namespace
per the same config file that `swift-s3-sync` uses.

### How to setup and use

`swift-s3-sync` depends on:

- [container-crawler library](https://github.com/swiftstack/container-crawler)
- [botocore](https://github.com/swiftstack/botocore/tree/1.4.32.5)
  (unfortunately, we had to use our own fork, as a number of patches were
  difficult to merge upstream)
- [boto](https://github.com/boto/boto3/tree/1.3.1)
- [eventlet](https://github.com/eventlet/eventlet)

Until we can merge the boto patches, you will also have to install botocore from
our fork (do this before installing swift-s3-sync):
`pip install -e git://github.com/swiftstack/botocore.git@1.4.32.5#egg=botocore`

Build the package to be installed on the nodes with:
```
python ./setup.py build sdist
```

Install the tarball with:
```
pip install swift-s3-sync-<version>.tar.gz
```

You also will need to install the `container-crawler` library from Git:
```
pip install -e git://github.com/swiftstack/container-crawler.git@0.0.12#egg=container-crawler
```

After that, you should have the `swift-s3-sync` executable available in
`/usr/local/bin`.

`swift-s3-sync` has to be invoked with a configuration file, specifying which
containers to watch, where the contents should be placed, as well as a number of
global settings. A sample configuration file is in the
[repository](https://github.com/swiftstack/swift-s3-sync/blob/master/sync.json-sample).

To configure the Swift Proxy servers to use `swift-s3-sync` to redirect requests
for archived objects, you have to add the following to the proxy pipeline:
```
[filter:swift_s3_shunt]
use = egg:swift-s3-sync#cloud-shunt
conf_file = <Path to swift-s3-sync config file>
```

This middleware should be in the pipeline before the DLO/SLO middleware.

To configure the proxymc servers, you have to have a minimal proxy-server-like
config file:
```
[DEFAULT]
...

[pipeline:main]
pipeline = healthcheck proxy-logging cache proxy-logging proxy-server

[app:proxy-server]
# Calling this "proxy-server" in the pipeline is a little white lie to keep the
# swift3 pipeline check from blowing up (if we were to make swift3 work in the
# future)
use = egg:swift-s3-sync#proxymc
conf_file = /.../swift-s3-sync.conf

[filter:healthcheck]
use = egg:swift#healthcheck

[filter:cache]
use = egg:swift#memcache

[filter:proxy-logging]
use = egg:swift#proxy_logging
```

### Trying it out

If you have docker and docker-compose already you can easily get started in the root directory:

```
docker-compose up -d
```

Our current development/test environment only defines one Docker container
named `swift-s3-sync`.  It is based on the
[bouncestorage/swift-aio](https://hub.docker.com/r/bouncestorage/swift-aio/).
The Dockerfile adds S3Proxy, backed by the file system and uses a Swift
all-in-one docker container as the base.  The Compose file maps the current
source tree into the container, so that it operates on your current state.
Port 8080 is the Swift Proxy server, port 10080 is the S3Proxy, and port 8081
is the proxymc server.

Tests pre-configure multiple
[policies](https://github.com/swiftstack/swift-s3-sync/blob/master/test/container/swift-s3-sync.conf).

Specifically, you can create containers `sync-s3` and `archive-s3` to observe
how swift-s3-sync works. Using `python-swiftclient`, that would look something
like this:

```
export ST_AUTH=http://localhost:8080/auth/v1.0
export ST_USER=test:tester
export ST_KEY=testing
swift post sync-s3
swift post archive-s3
swift upload sync-s3 README.md
swift upload archive-s3 README.md
```

In the root of the project we provide an example s3cfg file you can use with
s3cmd to talk to your S3Proxy configured and running in the container:

```
[default]
access_key=s3-sync-test
secret_key=s3-sync-test
host_base=localhost:10080
host_bucket=localhost:10080
use_https = False
```

If it makes life easier you can copy `s3cfg` to `~/.s3cfg` and s3cmd will pick
it up automatically.

> note: if you remapped your exposed ports with an custom compose file, you
> have to fix ST_AUTH and your s3cfg host options to which ever host port you
> mapped to 8080 and 10080 in the container.

After this, we can create the bucket, and shortly examine the synced data

```
s3cmd -c s3cfg mb s3://s3-sync-test
s3cmd -c s3cfg ls -r s3://s3-sync-test
```

You should see two objects in the bucket.

When you're done you can always destroy the container:

```
docker rm -sf
```

### Running tests


#### Unit tests

If you have the development environment running, it will have all the
dependencies already squared away, you can run arbitrary commands in the
pre-configured development environment, including `/bin/bash` or the test
suite:

```
docker-compose exec swift-s3-sync nosetests /swift-s3-sync/test/unit
```

If you want to try to get all the dependencies squared away on your host, start
here.  All commands below assume you're running them in the swift-s3-sync
directory.

It is recommended to setup virtualenv when working on this project:

```
virtualenv venv
```

After that, dependencies:

```
./venv/bin/pip install -r requirements.txt
./venv/bin/pip install -r requirements-test.txt
```

The remaining dependencies include swift and container-crawler, you'll need to
get their dependencies installed and add them to your `PYTHONPATH`:

```
export PYTHONPATH=$PYTHONPATH:~/swift:~/container-crawler
```

> note: if you have the swift and container-crawler sources somewhere else you
> will need to change the paths

Then just run unit tests with `nosetests test/unit`


#### Integration tests

For integration tests, we need access to a Swift cluster and some sort of an S3
provider. Currently, the tests use a Docker container to provide Swift and are
configured to talk to [S3Proxy](https://github.com/andrewgaul/s3proxy).

Start the provided development environment with:

```
docker-compose up -d
```

The container will be started in the background (`-d`) and will expose ports
8080 and 10080 to connect to Swift and S3Proxy, respectively.

> note: the services do not restart on code changes. You can either manually
> stop/start the swift-s3-sync daemon (and Swift proxy if you're working on the
> shunt), or just run `docker-compose restart`.

The cloud sync configuration for the tests is defined in
`test/container/swift-s3-sync.conf`. In particular, there are mappings for S3
sync and archive policies and the same for Swift. The S3 mappings point to
S3Proxy running on the host machine, listening on port 10080.

You can run integration tests in the container as well:

```
docker exec -e DOCKER=true swift-s3-sync nosetests /swift-s3-sync/test/integration
```

With sufficient effort you might be able to get enough dependencies installed on
your host to run the integration suite.

```
./venv/bin/nosetests test/integration
```

By default tests will look for the first running container that has been started
from an image named `swift-s3-sync`. You can override that behavior by
specifying the test container to use with the `TEST_CONTAINER` environment
variable.

The tests create and destroy the containers and buckets configured in the
`swift-s3-sync.conf` file. If you need to examine the state of a container,
consider commenting out the `tearDownClass` method to be a NOOP (TODO: add a way
to keep state).

If you would like to examine the logs from each of the services, all logs are in
/var/log (e.g. /var/log/swift-s3-sync.log).

### Working with Docker directly

## Build

To build the test container, run:
`docker build -t swift-s3-sync test/container`

To start the container, run:
`docker run -P -d -v <swift-s3-sync checkout>:/swift-s3-sync swift-s3-sync`

List running containers to inspect port mappings:
`docker ps -a`

## Override ports

Docker seems to do open ports with SO_REUSEPORT which can get confusing if you
have another service also trying to accept on the ports we use.

First copy the provided default `docker-compose.override.yml` to `docker-compose.custom.yml`

Then edit to change your port mappings:

```
version: '3'
services:
  swift-s3-sync:
    ports:
     - "8090:8080"
     - "10090:10080"
```

Here we've mapped the host port 8090 to the Swift proxy service running in the
container on 8080 and host port 10090 to the S3Proxy service listening on 10080
in the container.  You can edit your ST_AUTH env var and s3cfg as needed.

You can use the provided `example.env` to tell tell docker-compose how to
automatically overlay your customized ports:

`cp example.env .env`

Docker will automatically read the `.env` file and use your modified
`docker-compose.custom.yml` to control the host port mapping.
