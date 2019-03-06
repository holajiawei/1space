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


class FakeSwift(object):
    def __init__(self, responses={}):
        self.calls = []
        self.responses = responses

    def __call__(self, env, start_response):
        if env.get('wsgi.input'):
            wsgi_input = []
            chunk = env['wsgi.input'].read()
            while chunk:
                wsgi_input.append(chunk)
                chunk = env['wsgi.input'].read()
            body = ''.join(wsgi_input)
        else:
            body = ''
        call_env = dict(env)
        call_env['body'] = body
        self.calls.append(call_env)

        method = env['REQUEST_METHOD']
        # Let the tests set up the responses they want
        if env.get('__test__.response_dict'):
            resp_dict = env['__test__.response_dict']
            if method in resp_dict:
                if (isinstance(resp_dict[method], dict) and
                        env['PATH_INFO'] in resp_dict[method]):
                    responses = resp_dict[method][env['PATH_INFO']]
                else:
                    responses = resp_dict[method]

                if isinstance(responses, list):
                    entry = responses.pop(0)
                else:
                    entry = responses
                status = entry.get('status', '200 OK')
                headers = entry.get('headers', [])
                start_response(status, headers)
                return resp_dict[method].get('body', '')

        if self.responses and method in self.responses:
            resp_func = self.responses[method].pop(0)
            request_body = self.calls[-1]['body']
            status, headers, body = resp_func(
                env.get('headers', {}), request_body)
            start_response(status, headers)
            return body

        status = env.get('__test__.status', '200 OK')
        headers = env.get('__test__.headers', [])
        body = env.get('__test__.body', ['pass'])

        start_response(status, headers)
        return body


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
        self.chunk_size = self.size / 10 or 1
        self.raised_stop_iter = False

    def next(self):
        want = min(self.size - self.current_pos, self.chunk_size)
        if want <= 0:
            self.raised_stop_iter = True
            raise StopIteration()
        if self.content:
            ret = self.content[self.current_pos:self.current_pos + want]
        else:
            ret = 'A' * want
        self.current_pos += want
        return ret

    def __iter__(self):
        return self

    def __len__(self):
        return self.size

    def close(self):
        self.closed = True
