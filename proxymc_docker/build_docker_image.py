#!/usr/bin/env python
import argparse
import os
import shutil
import subprocess


def reset_git_to_taggy_thing(git_dir, taggy_thing):
    subprocess.check_call(['git', 'reset', '--hard'], cwd=git_dir)
    subprocess.check_call(['git', 'clean', '-dxfq'], cwd=git_dir)
    subprocess.check_call(['git', 'fetch', 'origin'], cwd=git_dir)
    subprocess.check_call(['git', 'fetch', '--tags', 'origin'], cwd=git_dir)
    rev_parsed = subprocess.check_output(['git', 'rev-parse', taggy_thing],
                                         cwd=git_dir).strip()
    subprocess.check_call(['git', 'checkout', rev_parsed],
                          cwd=git_dir)


def get_or_reset_source_repos(args):
    # ss-swift
    if not os.path.isdir(args.swift_dir):
        subprocess.check_call([
            'git', 'clone', 'https://github.com/swiftstack/swift.git',
            args.swift_dir])
    reset_git_to_taggy_thing(args.swift_dir, args.ss_swift_tag)

    # swift-s3-sync
    if args.swift_s3_sync_tag != 'DEV':
        if not os.path.isdir(args.swift_s3_sync_dir):
            subprocess.check_call([
                'git', 'clone',
                'https://github.com/swiftstack/swift-s3-sync.git',
                args.swift_s3_sync_dir])
        reset_git_to_taggy_thing(args.swift_s3_sync_dir,
                                 args.swift_s3_sync_tag)


def prepare_code_copy_sources(args):
    # ss-swift
    shutil.rmtree(args.swift_tree, ignore_errors=True)
    subprocess.check_call([
        '/usr/bin/env', 'python', './setup.py', 'install',
        '--prefix', '/usr/local', '--root', args.swift_tree, '-O2'],
        cwd=args.swift_dir)

    # swift-s3-sync
    shutil.rmtree(args.swift_s3_sync_tree, ignore_errors=True)
    subprocess.check_call([
        '/usr/bin/env', 'python', './setup.py', 'install',
        '--prefix', '/usr/local', '--root', args.swift_s3_sync_tree, '-O2'],
        cwd=args.swift_s3_sync_dir)

    if args.s3proxy:
        shutil.copy(os.path.join(args.swift_s3_sync_dir, 's3cfg'),
                    os.path.join(args.base_dir, 'files'))
        shutil.copy(os.path.join(args.swift_s3_sync_dir, 'test', 'container',
                                 's3proxy.properties'),
                    os.path.join(args.base_dir, 'files'))
        shutil.copy(os.path.join(args.swift_s3_sync_dir,
                                 'test', 'container', 'proxymc.conf'),
                    os.path.join(args.base_dir, 'files'))
        shutil.copy(os.path.join(args.swift_s3_sync_dir,
                                 'test', 'container', 'swift-s3-sync.conf'),
                    os.path.join(args.base_dir, 'files'))


def mungify(args, src_path, dst_path):
    with open(src_path, 'rb') as src:
        with open(dst_path, 'wb') as dst:
            in_s3proxy = False
            for line in src:
                line = line.replace('__CONFBUCKET__', args.config_bucket)
                if '__S3PROXY_END__' in line:
                    in_s3proxy = False
                    continue
                elif '__S3PROXY_BEGIN__' in line:
                    in_s3proxy = True
                    continue
                if in_s3proxy and not args.s3proxy:
                    continue
                dst.write(line)


def build_image(args):
    # Make a temporary dockerfile that includes the choices made by our
    # invoker.
    src_dockerfile_path = os.path.join(args.base_dir, 'Dockerfile')
    dst_dockerfile_path = os.path.join(args.base_dir, '.dockerfile')
    mungify(args, src_dockerfile_path, dst_dockerfile_path)

    src_sup_path = os.path.join(args.base_dir, 'files', 'supervisord.conf')
    dst_sup_path = os.path.join(args.base_dir, '.supervisord.conf')
    mungify(args, src_sup_path, dst_sup_path)

    desc = subprocess.check_output(
        ['git', 'describe', '--tags', 'HEAD'],
        cwd=args.swift_s3_sync_dir).strip()
    subprocess.check_call([
        'docker', 'build', '-f', dst_dockerfile_path,
        '-t', 'swiftstack/proxymc:' + desc, '.'])

    os.unlink(dst_dockerfile_path)
    os.unlink(dst_sup_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='Build proxymc Docker images.')
    parser.add_argument('--ss-swift-tag', default='ss-release-2.16.0.2',
                        help='Argument for "git checkout" to select '
                        'the ss-swift code to use inside the Docker '
                        'image.')
    parser.add_argument('--swift-s3-sync-tag', default='master',
                        help='Argument for "git checkout" to select '
                        'the swift-s3-sync code to use inside the Docker '
                        'image; the special value "DEV" will use this '
                        'tree\'s code.')
    parser.add_argument('--config-bucket', default='dockertest',
                        help='The name of the S3 bucket in which the '
                        'service configuration file may be found. The '
                        'filename within the bucket must be '
                        '"proxymc.conf" and whatever "conf_file" '
                        'that config references must also be available '
                        'in the same bucket.')
    parser.add_argument('--s3proxy', action='store_true', default=False,
                        help='Include and use a S3Proxy server inside the '
                        'container? (Config comes from files/ directory.)')

    args = parser.parse_args()
    args.base_dir = os.path.realpath(os.path.dirname(__file__))
    args.swift_dir = os.path.join(args.base_dir, 'files', 'swift')
    if args.swift_s3_sync_tag == 'DEV':
        args.swift_s3_sync_dir = os.path.join(args.base_dir, '..')
    else:
        args.swift_s3_sync_dir = os.path.join(args.base_dir,
                                              'files', 'swift-s3-sync')
    args.swift_tree = os.path.join(args.swift_dir, 'tree')
    args.swift_s3_sync_tree = os.path.join(args.base_dir,
                                           'files', 'swift-s3-sync', 'tree')
    os.chdir(args.base_dir)

    print '''Building proxmc Docker image in %s
  (using ss-swift==%s and swift-s3-sync==%s)
''' % (args.base_dir, args.ss_swift_tag, args.swift_s3_sync_tag)

    get_or_reset_source_repos(args)
    prepare_code_copy_sources(args)
    build_image(args)
