Installation
============

1space depends on:

- `container-crawler library <https://github.com/swiftstack/container-crawler>`_
- `botocore <https://github.com/swiftstack/botocore/tree/1.12.23.1>`_
  (unfortunately, we had to use our own fork, as a number of patches were
  difficult to merge upstream)
- `boto <https://github.com/boto/boto3>`_
- `eventlet <https://github.com/eventlet/eventlet>`_

All dependencies, can be installed automatically with ``pip``::

    pip install -r requirements.txt

Build the package to be installed on the nodes with::

    python ./setup.py build sdist

Install the tarball with::

    pip install dist/swift-s3-sync-<version>.tar.gz

After that, you should have the ``swift-s3-sync`` and ``swift-s3-migrator``
executables available in ``/usr/local/bin``.

