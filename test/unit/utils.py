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


class FakeStream(object):
    def __init__(self, size=1024, content=None):
        if content:
            self.size = len(content)
            self.content = content
        else:
            self.size = size
            self.content = None
        self.current_pos = 0
        self.closed = False

    def read(self, size=0):
        if self.closed:
            raise RuntimeError('The stream is closed')
        if self.current_pos == self.size - 1:
            return ''
        if size == -1 or self.current_pos + size > self.size:
            if self.content:
                ret = self.content[self.current_pos:]
            else:
                ret = 'A' * (self.size - self.current_pos)
            self.current_pos = self.size - 1
            return ret

        if self.content:
            ret = self.content[self.current_pos:self.current_pos + size]
        else:
            ret = 'A' * size
        self.current_pos += size
        return ret

    def next(self):
        if self.current_pos == self.size:
            raise StopIteration()
        if self.content:
            ret = self.content[self.current_pos]
        else:
            ret = 'A'
        self.current_pos += 1
        return ret

    def __iter__(self):
        return self

    def __len__(self):
        return self.size

    def close(self):
        self.closed = True
