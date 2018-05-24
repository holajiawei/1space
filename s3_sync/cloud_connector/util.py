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

import os
import pwd
import requests
import time
import traceback

from s3_sync.sync_s3 import SyncS3


class ConfigReloaderMixin(object):
    """
    Classes that need to reload one or more config files from a S3 protocol
    object store can use this mixin to automatically reload configs when they
    change.

    Using classes can define these properties:
        CHECK_PERIOD = X  # check for new config every X seconds
        CONF_FILES = [{
           'key': '...',
           'load_cb': some_fn,
        }, ...]
    """
    def reload_confs(self):
        # The using class may need to fiddle around in __init__ to determine
        # what the contents of CONF_FILES should be, so to avoid a
        # chicken-and-egg in a __init__() here, we just lazily populate
        # self.config_info in here.
        if not getattr(self, 'config_info', None):
            self.config_info = [{
                'key': cf['key'],
                'load_cb': cf['load_cb'],
                'next_check': 0,
                'last_etag': '',
            } for cf in self.CONF_FILES]

        now = time.time()
        # We have to get fresh env_options to avoid trying to use an expired
        # short-term service credential handed out by Amazon ECS.
        # TODO(darrell): store the expire time of the ECS cred somewhere and
        # use that to not hit the ECS cred endpoint any more frequently than we
        # need to.
        env_options = None
        for ci in self.config_info:
            if now >= ci['next_check']:
                if not env_options:
                    env_options = get_env_options()
                try:
                    resp = get_conf_file_from_s3(ci['key'], env_options,
                                                 if_none_match=ci['last_etag'])
                    ci['last_etag'] = resp.headers['etag']
                    ci['load_cb'](''.join(resp.body))
                except ConfigUnchanged:
                    pass
                finally:
                    ci['next_check'] = now + self.CHECK_PERIOD


def get_aws_ecs_creds():
    opts = {}

    aws_creds_relative_uri = os.environ.get(
        'AWS_CONTAINER_CREDENTIALS_RELATIVE_URI', None)
    if aws_creds_relative_uri:
        creds_uri = 'http://169.254.170.2%s' % (aws_creds_relative_uri,)
        resp = requests.get(creds_uri)
        resp.raise_for_status()
        aws_creds = resp.json()
        opts['AWS_ACCESS_KEY_ID'] = aws_creds['AccessKeyId']
        opts['AWS_SECRET_ACCESS_KEY'] = aws_creds['SecretAccessKey']
        opts['AWS_SECURITY_TOKEN_STRING'] = aws_creds['Token']
        # NOTE: this temporary key will expire in like a day.
    return opts


def get_env_options():
    """
    Reads various environment variables to determine how to load configuration
    from a S3 API endpoint:
        CONF_BUCKET: required; bucket where config(s) live
        CONF_ENDPOINT: optional; S3 API endpoint; defaults to Amazon S3
        CONF_NAME: optional; object name of main config file residing in
            the CONF_BUCKET; defaults to `cloud-connector.conf`

    For authentication/authorization into S3, one set of the following is
    required:
        AWS_CONTAINER_CREDENTIALS_RELATIVE_URI: Amazon ECS can set this env var
        to allow the container to load temporary session S3 credentials
    or:
        AWS_ACCESS_KEY_ID: S3 API key ID to use
        AWS_SECRET_ACCESS_KEY: S3 API secret access key to use

    If AWS_CONTAINER_CREDENTIALS_RELATIVE_URI is present as well as
    AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, then the latter values will be
    used, NOT the temporary credentials from
    AWS_CONTAINER_CREDENTIALS_RELATIVE_URI.

    Returns a dictionary of key/values read from the environment.
    """
    opts = {}

    opts['AWS_ACCESS_KEY_ID'] = os.environ.get('AWS_ACCESS_KEY_ID', None)
    opts['AWS_SECRET_ACCESS_KEY'] = os.environ.get('AWS_SECRET_ACCESS_KEY',
                                                   None)

    # Grabbing creds in this order and with this logic allows a container
    # deployment to overide the Amazon-ECS-configured temporary IAM role
    # session creds with a specific access key id and secret access key.
    if not (opts['AWS_ACCESS_KEY_ID'] and opts['AWS_SECRET_ACCESS_KEY']):
        opts.update(get_aws_ecs_creds())

    if not (opts['AWS_ACCESS_KEY_ID'] and opts['AWS_SECRET_ACCESS_KEY']):
        exit('Missing either AWS_CONTAINER_CREDENTIALS_RELATIVE_URI or '
             'AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env vars!')

    opts['CONF_BUCKET'] = os.environ.get('CONF_BUCKET', None)
    if not opts['CONF_BUCKET']:
        exit('Missing CONF_BUCKET env var!')

    opts['CONF_ENDPOINT'] = os.environ.get('CONF_ENDPOINT', '')
    opts['CONF_NAME'] = os.environ.get('CONF_NAME', 'cloud-connector.conf')

    return opts


class GetAndWriteFileException(Exception):
    pass


class ConfigUnchanged(Exception):
    pass


def get_conf_file_from_s3(obj_name, env_options, if_none_match=None):
    """
    Uses configuration, as read and returned by `get_env_options` to fetch an
    object specified by name, from a S3 endpoint, and return the
    ProviderResponse object for the GET.

    If the optional kwarg `if_none_match` is specified, it will be passed into
    the GET (to only GET a conf file if its ETag is different than what we got
    last time).  If the response is 304 (not modified), a ConfigUnchanged
    exception will be raised.
    """
    provider_settings = {
        'aws_identity': env_options['AWS_ACCESS_KEY_ID'],
        'aws_secret': env_options['AWS_SECRET_ACCESS_KEY'],
        'encryption': False,  # TODO: get from CONF_ENCRYPTION or something
        'custom_prefix': '',
        'account': 'notused',
        'container': 'notused',
        'aws_bucket': env_options['CONF_BUCKET'],
    }
    # To default to real Amazon S3, the aws_endpoint kwarg needs to not be
    # present.
    if env_options['CONF_ENDPOINT']:
        provider_settings['aws_endpoint'] = env_options['CONF_ENDPOINT']
    if env_options.get('AWS_SECURITY_TOKEN_STRING', None):
        provider_settings['aws_session_token'] = \
            env_options['AWS_SECURITY_TOKEN_STRING']
    provider = SyncS3(provider_settings)
    get_object_opts = {'IfNoneMatch': if_none_match} \
        if if_none_match is not None else {}
    resp = provider.get_object(obj_name, **get_object_opts)
    if not resp.success and resp.status != 304:
        raise GetAndWriteFileException('GET for %s: %s %s' % (
            obj_name, resp.status, ''.join(resp.body)))
    if resp.status == 304:
        raise ConfigUnchanged()
    return resp


def get_and_write_conf_file_from_s3(obj_name, target_path, env_options,
                                    user=None):
    """
    Uses configuration, as read and returned by `get_env_options` to fetch an
    object specified by name, from a S3 endpoint, and write it to the given
    local filesystem path.

    If a user name is specified and the current euid is 0 (running as root),
    the written file is chown'ed to the uid and primary gid of the specified
    user.
    """
    resp = get_conf_file_from_s3(obj_name, env_options)
    try:
        with open(target_path, 'wb') as fh:
            if user is not None and os.geteuid() == 0:
                user_ent = pwd.getpwnam(user)
                os.fchown(fh.fileno(), user_ent.pw_uid, user_ent.pw_gid)
            os.fchmod(fh.fileno(), 0o0640)
            for chunk in resp.body:
                fh.write(chunk)
    except Exception:
        os.unlink(target_path)
        raise GetAndWriteFileException('Writing %s: %s' % (
            obj_name, traceback.format_exc()))
