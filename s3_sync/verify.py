"""
Copyright 2018 SwiftStack

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import argparse
import urlparse

from .provider_factory import create_provider


def validate_bucket(provider, swift_key, create_bucket):
    if create_bucket:
        # This should only be necessary on Swift; reach down to the client
        with provider.client_pool.get_client() as client:
            result = client.put_container(provider.aws_bucket)
        if result is not None:
            return result

    result = provider.put_object(
        swift_key, {'content-type': 'text/plain'}, '1space-test')
    if not result.success:
        return 'Unexpected status code checking PUT: %s' % result.wsgi_status

    result = provider.update_metadata(swift_key, {
        'X-Object-Meta-Cloud-Sync': 'fabcab',
        'content-type': 'text/plain'})
    if result and not result.success:
        return result.wsgi_status

    result = provider.head_object(swift_key)
    if not result.success:
        return 'Unexpected status code checking write: %s' % result.wsgi_status
    if result.headers['x-object-meta-cloud-sync'] != 'fabcab':
        return 'Unexpected headers after setting metadata: %s' % result.headers

    result = provider.list_objects(
        marker='', limit=1, prefix='', delimiter='')
    if not result.success:
        return 'Unexpected status code listing bucket: %s' % result.wsgi_status

    result = provider.delete_object(swift_key)
    if result is not None and not result.success:
        return 'Unexpected status code deleting obj: %s' % result.wsgi_status

    if create_bucket:
        # Clean up after ourselves
        with provider.client_pool.get_client() as client:
            result = client.delete_container(provider.aws_bucket)
        if result is not None:
            return result


def main(args=None):
    keystone_v3_args = ['--project-name', '--project-domain-name',
                        '--user-domain-name']

    parser = argparse.ArgumentParser()
    parser.add_argument('--protocol', required=True, choices=('s3', 'swift'))
    parser.add_argument('--endpoint', required=True)
    parser.add_argument('--username', required=True)
    parser.add_argument('--password', required=True)
    parser.add_argument('--account')
    parser.add_argument('--bucket')
    parser.add_argument('--prefix')
    parser.add_argument('--auth-type', choices=('keystone_v2', 'keystone_v3'))
    # Required arg, if --auth-type=keystone_v2 provided:
    parser.add_argument('--tenant-name')
    # Required args if --auth-type=keystone_v3 provided:
    for v3_arg in keystone_v3_args:
        parser.add_argument(v3_arg)

    args = parser.parse_args(args)
    # We normalize the conf to Unicode strings since that's how they come out
    # of the JSON file.
    conf = {
        'protocol': args.protocol,
        'account': 'verify-auth',
        'container': u'testing-\U0001f44d',
        'aws_endpoint': args.endpoint,
        'aws_identity': args.username.decode('utf8'),
        'aws_secret': args.password.decode('utf8'),
        'remote_account':
        args.account.decode('utf8') if args.account else args.account,
        'aws_bucket': args.bucket.decode('utf8') if args.bucket
        else args.bucket,
        'custom_prefix': args.prefix.decode('utf8') if args.prefix else
        args.prefix,
    }
    if args.account and args.protocol != 'swift':
        return 'Invalid argument: account is only valid with swift protocol'
    if args.bucket == '/*':
        conf['aws_bucket'] = u'.cloudsync_test_container-\U0001f44d'
    if urlparse.urlparse(args.endpoint).hostname.endswith('.amazonaws.com'):
        conf['aws_endpoint'] = None  # let Boto sort it out

    if args.auth_type in ('keystone_v2', 'keystone_v3'):
        if args.protocol != 'swift':
            return 'Keystone auth requires swift protocol.'
    if args.auth_type == 'keystone_v2':
        if not args.tenant_name:
            return 'argument --tenant-name is required when ' \
                '--auth-type=keystone_v2'
        conf['auth_type'] = 'keystone_v2'
        conf['tenant_name'] = args.tenant_name.decode('utf8')
    elif args.auth_type == 'keystone_v3':
        err_strs = []
        for v3_arg in keystone_v3_args:
            attr = v3_arg[2:].replace('-', '_')
            if not getattr(args, attr):
                err_strs.append('argument %s is required when '
                                '--auth-type=keystone_v3\n' % v3_arg)
        if err_strs:
            return '\n'.join(err_strs)
        conf['auth_type'] = 'keystone_v3'
        for v3_arg in keystone_v3_args:
            attr = v3_arg[2:].replace('-', '_')
            conf[attr] = getattr(args, attr).decode('utf8')

    if conf['aws_bucket'] and '/' in conf['aws_bucket']:
        return 'Invalid argument: slash is not allowed in container name'

    provider = create_provider(conf, max_conns=1)
    if not args.bucket:
        with provider.client_pool.get_client() as client:
            if args.protocol == 's3':
                client.list_buckets()
            else:
                client.get_account()
    else:
        if args.protocol == 's3':
            if args.prefix:
                swift_key = 'cloud_sync_test'
            else:
                swift_key = 'fabcab/cloud_sync_test'
        else:
            swift_key = 'cloud_sync_test_object'
        result = validate_bucket(
            provider, swift_key,
            args.protocol == 'swift' and args.bucket == '/*')
        if result is not None:
            return result
    return 0


if __name__ == '__main__':
    exit(main())
