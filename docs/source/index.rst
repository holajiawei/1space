1space
======

.. toctree::
   :hidden:
   :maxdepth: 2
   :caption: Contents:

   installation
   running
   design
   contributing
   changelog
   modules

1space enables a single object namespace between object storage locations.
This enables application portability and freedom to share data between 
public and on-premises `OpenStack Swift <https://github.com/openstack/swift>`_
clouds. Additionally, 1space enables data lifecycling, migration and data
protection capabilities between clouds. The source code to 1space is hosted
at: `github.com/swiftstack/1space <https://github.com/swiftstack/1space>`_

Notable features:
-----------------

Data Lifecycle
  What data lifecycling does is move data, based on policy (such as age),
  to another storage location. This enables an operator to use the public
  cloud as part of their pool of capacity. The single namespace is
  established so applications (or users) don’t need to change as data moves
  from one location to another.

Live Migration
  Live migration is the act of taking an application and moving it from one
  cloud to another. With a single namespace, that means that application
  migration can be done *live*. This is super-challenging to pull off,
  but is *essential* for active applications with a large storage
  footprint.

Cloud Bursting
  Cloud bursting is about leveraging either the native services in the
  cloud, or bursting up compute to do data processing in the public cloud.
  With a single namespace – this provides data *access* to on-premises data.

Design overview
---------------

1space runs as a standalone process, intended to be used on Swift
container nodes. The container database provides the list of changes to the
objects in Swift (whether it was a metadata update, new object, or a deletion).

To provide on-line access to archived Swift objects, there is a Swift middleware
`component <https://github.com/swiftstack/1space/blob/master/s3_sync/shunt.py>`_.
If a Swift container was configured to be archived, the middleware will query the
destination store for contents on a GET request, as well as splice the results
of LIST requests between the two stores.

There is no explicit coordination between the 1space daemons.
Implicitly, they coordinate through their progress in the container database.
Each daemon looks up the number of container nodes in the system (with the
assumption that each node has a running daemon). Initially, each only handles
the objects assigned to it. Afterward, each one verifies that the other objects
have been processed, as well. This means that for each operation, there are
as many requests issued against the remote store as there are container
databases for the container. For example, in a three replica policy, there would
be three HEAD requests if an object PUT was performed (but only one PUT against
the remote store in the common case).

License
-------
1space is licensed under the Apache2 license.
