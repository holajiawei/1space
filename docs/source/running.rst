Running and Deploying
=====================

Both ``swift-s3-sync`` and ``swift-s3-migrator`` must be invoked with a
configuration file, specifying which containers to watch, where the
contents should be placed, as well as a number of global settings. A
sample configuration file is in the
`repository <https://github.com/swiftstack/1space/blob/master/sync.json-sample>`_.

Both of these tools run in the foreground, so starting each in their own
respective screen sessions is advisable.

To configure the Swift Proxy servers to use ``1space`` to redirect requests
for archived objects, you have to add the following to the proxy pipeline::

    [filter:swift_s3_shunt]
    use = egg:swift-s3-sync#cloud-shunt
    conf_file = <Path to swift-s3-sync config file>

This middleware should be in the pipeline before the DLO/SLO middleware.

when configuring, it's important to notice the different roles between the
sync and the migrator tools. The Sync/Lifecycle tool is used to push objects
from the local Swift cluster out to a remote object store. The Migrator is used
to copy objects from remote object store into the local Swift cluster.

swift-s3-sync configuration 
---------------------------
Below is a sample of both a sync profile setting and the sync global settings.
A profile is a mapping between one local swift container and a remote bucket.
The sync process can handle multiple profiles, the global settings apply to
all profiles:

.. code-block:: json

   {
       "containers": [
           {
               "account": "AUTH_swift",
               "container": "local",
               "aws_endpoint": "http://192.168.22.99/auth/v1.0",
               "aws_identity": "swift",
               "aws_secret": "swift",
               "aws_bucket": "remote",
               "protocol": "swift",
               "copy_after": 0,
               "propagate_delete": false,
               "retain_local": false
           }
       ]  
       "devices": "/swift/nodes/1/node",
       "items_chunk": 1000,
       "log_file": "/var/log/swift-s3-sync.log",
       "poll_interval": 1,
       "status_dir": "/var/lib/swift-s3-sync",
       "workers": 10,
       "enumerator_workers": 10,
       "statsd_host": "localhost",
       "statsd_port": 21337
   }

Sync Profile
  - **account**: local account where data is synced from.
  - **container**: local container where data is synced from.
  - **aws_endpoint**: remote object store endpoint (supports either
    swift/s3 endpoint).
  - **aws_identity**: remote object store account/identity.
  - **aws_secret**: remote object store identity's secret/password.
  - **aws_bucket**: remote bucket where data is synced to.
  - **protocol**: remote object store API protocol: ``swift`` or ``s3``.
  - **copy_after**: Time in seconds to delay object sync (*Optional*,
    Default: 0).
  - **propagate_delete**: If False, local DELETE requests won't be propagated
    to remote container (*Optional*, Default: ``True``).
  - **retain_local**: If False, local object will be deleted after sync is
    completed (*Optional*, Default: 'True').
  - **remote_delete_after**: Delete after setting for remote objects. For Swift
    remote clusters, this is applied to each object. For S3, it is applied as a
    lifecycle policy for the prefix. Note that in both cases, the delete after
    relates to the original object date, not the date it is copied to remote.
    A value of 0 (zero) means don't apply. (*Optional*, Default: 0)

Global settings
  - **devices**: Directory Swift's container devices are mounted under.
  - **items_chunk**: Number of rows to process at a time
  - **log_file**: Path to sync process log file
  - **poll_interval**: Time interval between sync runs
  - **status_dir**: Directory to where sync process saves status data
  - **workers**: Number of internal swift clients
  - **enumerator_workers**: Number of sync workers
  - **statsd_host**: StatsD host
  - **statsd_port**: StatsD port

swift-s3-migrator configuration 
-------------------------------
Below is a sample of both a migration profile setting and the migratoin global
settings. A profile is a mapping between one (or all for a given account)
remote container and a local account or container. The migrator process
can handle multiple profiles, the global settings apply to all profiles:

.. code-block:: json

   {
       "migrations": [
           {
               "account": "AUTH_test",
               "container": "migration-s3",
               "aws_endpoint": "http://1space-s3proxy:10080",
               "aws_identity": "s3-sync-test",
               "aws_secret": "s3-sync-test",
               "aws_bucket": "migration-s3",
               "protocol": "s3"
           },
       ],
       "migrator_settings": {
           "items_chunk": 5,
           "log_file": "/var/log/swift-s3-migrator.log",
           "poll_interval": 1,
           "status_file": "/var/lib/swift-s3-sync/migrator.status",
           "workers": 5,
           "processes": 1,
           "process": 0,
           "log_level": "debug"
       },
   }

Sync Profile
  - **account**: local account where data is migrated to.
  - **container**: local container where data is migrated to.
  - **aws_endpoint**: remote object store endpoint (supports either
    swift/s3 endpoint).
  - **aws_identity**: remote object store account/identity.
  - **aws_secret**: remote object store identity's secret/password.
  - **aws_bucket**: remote bucket where data is migrated from.
  - **protocol**: remote object store API protocol: ``swift`` or ``s3``.

Global settings
  - **items_chunk**: Number of items to process at a time
  - **log_file**: Path to sync process log file
  - **poll_interval**: Time interval between sync runs
  - **status_dir**: Directory to where sync process saves status data
  - **workers**: Number of internal swift clients
  - **processes**: Number of total migrator processes
  - **process**: index id of migrator process
  - **log_level**: Log level
