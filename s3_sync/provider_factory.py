"""
Copyright 2017 SwiftStack

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

from .providers.s3 import S3
from .providers.swift import Swift


def create_provider(sync_settings, max_conns, per_account=False, logger=None,
                    extra_headers=None):
    provider_type = sync_settings.get('protocol', None)
    if not provider_type or provider_type == 's3':
        return S3(sync_settings, max_conns, per_account, logger,
                  extra_headers=extra_headers)
    elif provider_type == 'swift':
        return Swift(sync_settings, max_conns, per_account, logger,
                     extra_headers=extra_headers)
    else:
        raise NotImplementedError()
