#!/usr/bin/env python

import argparse
from contextlib import contextmanager
import os
import shutil
import subprocess


@contextmanager
def swift_s3_sync_base_dir(args):
    if args.swift_s3_sync_tag != 'DEV':
        code_base = '.swift-s3-sync'
        shutil.rmtree(code_base, ignore_errors=True)
        subprocess.check_call([
            'git', 'clone', '-b', args.swift_s3_sync_tag,
            '--single-branch', '--depth', '1',
            'https://github.com/swiftstack/swift-s3-sync.git',
            code_base],
            cwd=args.base_dir)
        try:
            yield code_base
        finally:
            shutil.rmtree(code_base, ignore_errors=True)
    else:
        yield '.'


def build_image(args, code_base):
    dockerfile_path = os.path.join(args.my_dir, 'Dockerfile')

    if args.swift_s3_sync_tag == 'DEV':
        desc = 'DEV'
    else:
        desc = subprocess.check_output(
            ['git', 'describe', '--tags', 'HEAD'],
            cwd=os.path.join(args.base_dir, code_base)).strip()
    tag = '%s:%s' % (args.repository, desc)
    build_args = [
        '.', '-f', dockerfile_path, '-t', tag,
        '--build-arg', 'SWIFT_REPO=%s' % args.swift_repo,
        '--build-arg', 'SWIFT_TAG=%s' % args.swift_tag,
        '--build-arg', 'SWIFT_S3_SYNC_DIR=%s' % code_base,
        '--build-arg', 'CONF_BUCKET=%s' % args.config_bucket,
    ]
    subprocess.check_call(
        ['docker', 'build'] + build_args,
        cwd=args.base_dir)

    if args.push:
        try:
            stdout_err = subprocess.check_output(['docker', 'push', tag],
                                                 stderr=subprocess.STDOUT)
            print stdout_err
        except subprocess.CalledProcessError as e:
            if "Please run 'aws ecr get-login' to fetch a new one" in e.output:
                login_cmd = subprocess.check_output(
                    ['aws', 'ecr', 'get-login', '--no-include-email'])
                if "docker login -u AWS" in login_cmd:
                    subprocess.check_call(login_cmd, shell=True)
                    subprocess.check_call(['docker', 'push', tag])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='Build cloud-connector Docker images.')
    parser.add_argument('--swift-repo', default='openstack/swift',
                        help='Repo name at GitHub from which to pull '
                        'Swift code.')
    parser.add_argument('--swift-tag', default='2.17.0',
                        help='Git tag or branch name for Swift code '
                        'to use inside the Docker image.')
    parser.add_argument('--swift-s3-sync-tag', default='master',
                        help='Git tag or branch name for the '
                        'swift-s3-sync code to use inside the Docker '
                        'image; the special values "DEV" will use this '
                        'tree\'s current code.')
    parser.add_argument('--config-bucket', default='cloud-connector-conf',
                        help='The name of the S3 bucket in which the '
                        'various cloud-connector configuration files '
                        'may be found; this value may be overridden '
                        'whenenver the generated Docker image is run '
                        'via an environment variable.')
    parser.add_argument('--repository', default='swiftstack/cloud-connector',
                        help='Docker repository to use for tagging the built '
                        'image.')
    parser.add_argument('--push', action='store_true', default=False,
                        help='Push the built image to the repository?')

    args = parser.parse_args()
    args.my_dir = os.path.realpath(os.path.dirname(__file__))
    args.base_dir = os.path.realpath(os.path.join(args.my_dir, '..'))

    os.chdir(args.base_dir)

    print '''Building cloud-connector Docker image using
    swift==%s and swift-s3-sync==%s
''' % (args.swift_tag, args.swift_s3_sync_tag)

    with swift_s3_sync_base_dir(args) as s3_sync_dir:
        build_image(args, s3_sync_dir)
