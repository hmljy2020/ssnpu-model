"""Core-side token adapter. It knows bindings and instructions, not Calendar."""
from collections import Counter

from runtime.component import Component
from runtime.protocol import (Dispatch, Complete, Consumed, Access, AccessDone, ReceivePrepare,
                       PortFence, WriteReservations, RouterWrite, PublishReceive, TransferReply, core_name)
from runtime.communication import (Accepted, Rejected, Completion, DataCommand, DataReply, request_from_dict)
from errors import ModelError


class CommPort(Component):
    def __init__(self, name, instructions):
        super().__init__(name)
        self.nodes = instructions
        self.bindings, self.by_instruction = {}, {}
        self.waiting, self.finished, self.retry = set(), {}, {}
        self.waits = Counter()
        for key, instruction in instructions.items():
            token = instruction.raw['communication']['token']
            binding = self.bindings.setdefault(token, {})
            binding[instruction.raw['ir_op'].split('.')[1]] = instruction
            binding['request'] = request_from_dict(instruction.raw['communication']['request'])
            self.by_instruction[key] = token

    def finish(self, instruction):
        self.observe(instruction.id, complete=self.cycle, state='complete')
        self.send(core_name(instruction.core, 'rob'), Complete(instruction.id))

    def tick(self, cycle):
        for _, message in self.inputs(cycle):
            if isinstance(message, Dispatch):
                instruction = self.nodes[message.instruction]
                token = self.by_instruction[instruction.id]
                self.observe(instruction.id, received=cycle, state='communication_pending')
                if instruction.raw['ir_op'] == 'comm.submit':
                    self.send('communit', self.bindings[token]['request'])
                else:
                    self.waiting.add(token)
            elif isinstance(message, Accepted):
                binding = self.bindings[message.token]
                self.retry.pop(message.token, None)
                self.observe(binding['submit'].id, accepted=cycle)
                self.finish(binding['submit'])
                # Source retention ends on data-read completion, not acceptance.
            elif isinstance(message, Rejected):
                if not message.retryable:
                    raise ModelError(f'Communication request {message.token} rejected: {message.reason}')
                self.retry[message.token] = cycle+1
                self.waits[self.bindings[message.token]['submit'].id] += 1
            elif isinstance(message, Completion):
                if message.status != 'success':
                    raise ModelError(f'Communication {message.token} fault (bindings quarantined): {message.error}')
                self.finished[message.token] = message
            elif isinstance(message, AccessDone):
                token = self.by_instruction[message.instruction]
                binding = self.bindings[token]
                self.send(core_name(binding['submit'].core, 'tile'), Consumed(message.instruction))
                self.send('communit', DataReply(token, 'source_ready'))
                self.observe(message.instruction, source_read_complete=cycle)
            elif isinstance(message, TransferReply):
                self.send('communit', DataReply(self.by_instruction[message.instruction], message.event, message.detail))
            elif isinstance(message, DataCommand):
                binding = self.bindings[message.token]
                submit, wait, request = binding['submit'], binding['wait'], binding['request']
                expected = request.source if message.kind == 'read' else request.destination
                if message.region != expected:
                    self.send('communit', DataReply(message.token, 'fault', 'Data access binding/generation mismatch'))
                    continue
                if message.kind == 'read':
                    obj, version = submit.operands[0]
                    command = Access(submit.id, message.token, version, False, message.region.nbytes, obj['dtype'])
                elif message.kind == 'prepare':
                    command = ReceivePrepare(wait.id, message.region.handle, message.detail)
                elif message.kind == 'fence':
                    command = PortFence(wait.id)
                elif message.kind == 'reserve':
                    command = WriteReservations(wait.id, message.detail)
                elif message.kind == 'write':
                    source, offset, size, _ = message.detail
                    command = RouterWrite(wait.id, message.region.handle, source, offset, size)
                elif message.kind == 'publish':
                    command = PublishReceive(wait.id)
                else:
                    self.send('communit', DataReply(message.token, 'fault', 'Unknown data command'))
                    continue
                self.send(core_name(submit.core, 'tile'), command)
            else:
                raise TypeError(message)
        for token, when in list(self.retry.items()):
            if when <= cycle:
                self.send('communit', self.bindings[token]['request'])
                del self.retry[token]
        for token in sorted(self.waiting & self.finished.keys()):
            wait = self.bindings[token]['wait']
            self.finish(wait)
            self.send(core_name(wait.core, 'tile'), Consumed(wait.id))
            self.waiting.remove(token)
            del self.finished[token]

    def next_event(self):
        return min(self.retry.values(), default=None)

    def is_idle(self):
        return super().is_idle() and not (self.retry or self.waiting or self.finished)

    def snapshot(self):
        return dict(super().snapshot(), waiting=sorted(self.waiting), completed=sorted(self.finished), retry=dict(self.retry))
