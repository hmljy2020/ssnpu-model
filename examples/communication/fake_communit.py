"""Scripted communication peer for core tests; no network model is imported.

script(request, peer) yields DataCommand objects. Each command waits for the
matching DataReply before the generator resumes. Tests choose ranges, resource
reservation times and failures explicitly. Publication precedes completion.
"""
from runtime.component import Component
from runtime.communication import CommRequest, Accepted, Completion, DataReply


class FakeCommUnit(Component):
    def __init__(self, script):
        super().__init__('communit')
        self.script = script
        self.jobs = {}
        self.history = []

    def proceed(self, token, reply=None):
        request, steps = self.jobs[token]
        try:
            command = next(steps) if reply is None else steps.send(reply)
        except StopIteration:
            self.send(request.endpoint, Completion(token, 'success'))
            del self.jobs[token]
        else:
            if command.token != token:
                raise ValueError('Script command token differs from request')
            self.send(request.endpoint, command)

    def tick(self, cycle):
        for _, message in self.inputs(cycle):
            self.history.append((cycle, message))
            if isinstance(message, CommRequest):
                if message.token in self.jobs:
                    raise ValueError('Duplicate test request')
                self.jobs[message.token] = message, iter(self.script(message, self))
                self.send(message.endpoint, Accepted(message.token))
                self.proceed(message.token)
            elif isinstance(message, DataReply):
                if message.event == 'fault':
                    request, _ = self.jobs.pop(message.token)
                    self.send(request.endpoint, Completion(message.token, 'fault', str(message.detail), False))
                else:
                    self.proceed(message.token, message)
            else:
                raise TypeError(message)

    def is_idle(self):
        return super().is_idle() and not self.jobs
