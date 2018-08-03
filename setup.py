#!/usr/bin/python

from setuptools import setup

setup(name='swift-s3-sync',
      version='0.1.42',
      author='SwiftStack',
      test_suite='nose.collector',
      url='https://github.com/swiftstack/swift-s3-sync',
      packages=['s3_sync', 's3_sync.cloud_connector'],
      dependency_links=[
          'git://github.com/swiftstack/botocore.git@1.4.32.6#egg=botocore',
          'git://github.com/swiftstack/container-crawler.git@0.0.12'
          '#egg=container-crawler',
      ],
      install_requires=['boto3==1.3.1'],
      entry_points={
          'console_scripts': [
              'swift-s3-sync = s3_sync.__main__:main',
              'swift-s3-verify = s3_sync.verify:main',
              'swift-s3-migrator = s3_sync.migrator:main',
              'cloud-connector = s3_sync.cloud_connector.app:main',
          ],
          'paste.filter_factory': [
              'cloud-shunt = s3_sync.shunt:filter_factory',
              'cloud-connector-auth = '
              's3_sync.cloud_connector.auth:filter_factory',
              'cloud-connector-slo = '
              's3_sync.cloud_connector.slo_cc:filter_factory',
          ],
          'paste.app_factory': [
              'cloud-connector = s3_sync.cloud_connector.app:app_factory',
          ],
      })
