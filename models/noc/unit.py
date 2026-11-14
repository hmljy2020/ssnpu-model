"""WSE40 Calendar network; exact network time, core-clock control messages.

One object owns the fabric. Sources are staged before alignment. Shared write
slots are installed under an admission fence, then ordinary writes use gaps.
Whole-packet adapter buffers are provisioned from the plan, not assumed SRAM.
"""
import heapq
import math
from bisect import bisect_left
from collections import Counter, defaultdict
from fractions import Fraction

from errors import ModelError
from models.noc.routing import receive_offset, route_tree, transmissions
from runtime.communication import (CommRequest, Accepted, Rejected, Completion, DataCommand, DataReply, digest)
from runtime.component import Component



class CommUnit(Component):
    def __init__(self, installation, platform):
        super().__init__('communit')
        import copy
        self.platform = copy.deepcopy(platform)
        self.installation = copy.deepcopy(installation)
        body = {k: v for k, v in installation.items() if k != 'version'}
        if installation.get('schema') != 'wse-communit/1' or digest(body) != installation.get('version'):
            raise ModelError('Invalid CommUnit installation version')
        if installation['platform_hash'] != digest(platform):
            raise ModelError('CommUnit platform/config version mismatch')
        self.p = platform['network']
        self.core_ns = Fraction(10**9, self.p['core_clock_hz'])
        self.noc_ns = Fraction(10**9, self.p['noc_clock_hz'])
        self.payload = self.p['link_bytes_per_cycle'] - self.p['header_bytes']
        self.states, self.requests, self.tokens = {}, {}, {}
        self.heap, self.intervals, self.events = [], defaultdict(list), []
        self.serial = self.processed = 0
        self.links, self.buffer_required = defaultdict(Counter), Counter()
        self.active = None
        self.failed = False
        for name, compiled in self.installation['calreg'].items():
            entries = {int(core): dict(entry) for core, entry in compiled['entries'].items()}
            self.states[name] = dict(plan={'name': name}, compiled=compiled, entries=entries,
                accepted=set(), posted=set(), staged=set(), ports={}, reserved=set(),
                published=set(), committed=set(), tx_done=set(), completed=set(),
                committed_remote=Counter(), status='preparing', coverage=defaultdict(set))
            for core, entry in entries.items():
                self.buffer_required[f'core:{core}/tx'] = max(self.buffer_required[f'core:{core}/tx'], entry['nbytes'])
                remote = sum(w['nbytes'] for w in entry['expected'] if w['source'] != core)
                self.buffer_required[f'core:{core}/rx'] = max(self.buffer_required[f'core:{core}/rx'], remote)

    def command(self, name, core, kind, detail=None, *, source=False):
        request = self.requests[name, core]
        return request.endpoint, DataCommand(request.token, kind,
            request.source if source else request.destination, detail)

    def data(self, name, core, kind, detail=None, *, source=False):
        endpoint, command = self.command(name, core, kind, detail, source=source)
        self.send(endpoint, command)

    def submit(self, request):
        try:
            if self.failed:
                raise ModelError('CommUnit quarantined after fault')
            if request.version != self.installation['version']:
                raise ModelError('Request/CalReg version mismatch')
            if request.operation != 'allgather':
                raise ModelError('Only AllGather/copy semantics are supported')
            s = self.states[request.domain]
            entry = s['entries'][request.node]
            if tuple(sorted(s['entries'])) != request.members or request.epoch != s['compiled']['epoch']:
                raise ModelError('Request epoch/participant domain mismatch')
            if digest(list(request.route_bits)) != entry['route_digest']:
                raise ModelError('Request route differs from installed CalReg contract')
            for region in (request.source, request.destination):
                if not region.handle or any(type(v) is not int for v in (region.offset, region.nbytes, region.capacity)) or region.offset < 0 or region.nbytes <= 0 or region.offset+region.nbytes > region.capacity:
                    raise ModelError('Invalid source/destination data range')
            expected = sum(w['nbytes'] for w in entry['expected'])
            if (request.source.nbytes != entry['nbytes'] or request.source.offset != entry['source_offset'] or
                    request.destination.nbytes != expected or request.destination.offset != 0 or
                    request.destination_offset != entry['destination_offset']):
                raise ModelError('Request geometry differs from compiled contract')
            if request.token in self.tokens or (request.domain, request.node) in self.requests:
                raise ModelError('Duplicate token or retired epoch/member request')
            if not request.token or not request.endpoint:
                raise ModelError('Request token and endpoint must be nonempty')
        except (ModelError, KeyError) as exc:
            self.send(request.endpoint, Rejected(request.token, str(exc), False))
            return
        # One collective admission domain at a time. Capacity covers every
        # member, avoiding a queue-depth deadlock while waiting for peers.
        if self.active not in (None, request.domain):
            self.send(request.endpoint, Rejected(request.token, 'communication domain busy', True))
            self.record('backpressure', self.cycle*self.core_ns, phase=request.domain, node=request.node)
            return
        self.active = request.domain
        self.tokens[request.token] = request.domain, request.node
        self.requests[request.domain, request.node] = request
        entry['routes'] = [dict(window, route_bits=bits) for window, bits in zip(entry['windows'], request.route_bits)]
        s['accepted'].add(request.node)
        self.record('accepted', self.cycle*self.core_ns, phase=request.domain, node=request.node, token=request.token)
        self.send(request.endpoint, Accepted(request.token))
        self.data(request.domain, request.node, 'prepare', tuple((w['source'], w['offset'], w['nbytes']) for w in entry['expected']))
        self.data(request.domain, request.node, 'read', source=True)

    def local_complete(self, name, core):
        s = self.states[name]
        if core in s['published'] and core in s['tx_done'] and core not in s['completed']:
            request = self.requests[name, core]
            s['completed'].add(core)
            self.send(request.endpoint, Completion(request.token, 'success'))
            self.record('completion', self.cycle*self.core_ns, phase=name, node=core, token=request.token)

    def fail(self, reason):
        # Fail-stop quarantine: never publish success or release bindings after
        # an in-flight fault. Completion explicitly forbids reuse; reset/recovery
        # is outside this first version, and drained() remains false.
        if self.failed:
            return
        self.failed = True
        for (name, core), request in self.requests.items():
            if core not in self.states[name]['completed']:
                self.send(request.endpoint, Completion(request.token, 'fault', str(reason), False))
        self.record('fault', self.cycle*self.core_ns, error=str(reason))

    def drained(self):
        return not self.failed and not self.heap and not self.inbox and not self.outputs and self.active is None

    def record(self, kind, time, *, end=None, **fields):
        self.events.append(dict(kind=kind, time_ns=float(time), time_ns_exact=str(time),
                                end_ns=float(time if end is None else end), **fields))

    def push(self, when, kind, name, source, node, flit=-1, round_id=0):
        self.serial += 1
        heapq.heappush(self.heap, (when, self.serial, kind, name, source, node, flit, round_id))

    def reserve(self, resource, start, end, name, source, **fields):
        intervals = self.intervals[resource]
        index = bisect_left(intervals, (start,))
        if (index and intervals[index - 1][1] > start) or (index < len(intervals) and intervals[index][0] < end):
            raise ModelError(f'Calendar resource collision: {resource}, {name}/{source}, {start}')
        intervals.insert(index, (start, end))
        self.record('link' if resource.startswith('link:') else 'port', start, end=end,
                    resource=resource, phase=name, source=source, **fields)

    def bound(self, state, source, end, round_id=0):
        entry = state['entries'][source]
        limit = state['origin'] + transmissions(entry, self.p)[round_id]['end_cycle'] * self.noc_ns
        if end + self.p['guard_cycles'] * self.noc_ns > limit:
            raise ModelError(f'Calendar window exceeded: {state["plan"]["name"]}/{source}')

    def schedule_writes(self, name):
        s = self.states[name]
        # Declared endpoint round-trip bound, then an execution edge.
        earliest = max((self.cycle + self.platform['reservation_roundtrip_cycles'] + 1) * self.core_ns,
                       max(s['ports'].values()) * self.core_ns)
        s['origin'] = math.ceil(earliest / self.noc_ns) * self.noc_ns + (
            self.p['alignment_cycles'] + self.p['arm_cycles']) * self.noc_ns
        s['writes'], s['routes'], s['visibility'] = {}, {}, {}
        requests = defaultdict(list)
        engine = self.platform['write_service']
        for source, entry in sorted(s['entries'].items()):
            count = math.ceil(entry['nbytes'] / self.payload)
            for round_id, transmission in enumerate(transmissions(entry, self.p)):
                children, landing, distance = route_tree(source, transmission['route_bits'])
                s['routes'][source, round_id] = children, landing
                start = s['origin'] + transmission['start_cycle'] * self.noc_ns
                for target in landing:
                    ready = start + receive_offset(distance[target], count, self.p) * self.noc_ns
                    requests[target].append((math.ceil(ready / self.core_ns) + self.platform['endpoint_command_cycles'], source, round_id))
        for target, entries in sorted(requests.items()):
            available = s['ports'][target]
            reservations = []
            for ready, source, round_id in sorted(entries):
                work = s['entries'][source]['nbytes']
                service = max(1, math.ceil(work / engine['throughput']))
                start = max(ready, available)
                latency = engine['latency']
                self.bound(s, source, (start + service + latency) * self.core_ns, round_id)
                reservations.append((source, start, service, latency))
                s['writes'][source, target] = start
                s['visibility'][source, target] = start + service + latency
                available = start + service
                self.record('write_reservation', start * self.core_ns,
                            end=available * self.core_ns, phase=name, source=source, node=target,
                            ready_cycle=ready, start_cycle=start, service_cycles=service,
                            visible_cycle=start + service + latency, bytes=work)
            self.data(name, target, 'reserve', tuple(reservations))
        s['status'] = 'installing'

    def activate(self, name):
        s = self.states[name]
        if self.cycle * self.core_ns >= s['origin']:
            raise ModelError('Calendar admission missed the common origin')
        self.record('align', s['origin'], phase=name)
        for source, entry in sorted(s['entries'].items()):
            count = math.ceil(entry['nbytes'] / self.payload)
            drained = s['origin']
            for round_id, transmission in enumerate(transmissions(entry, self.p)):
                start = s['origin'] + transmission['start_cycle'] * self.noc_ns
                inject = start + self.p['tx_cdc_cycles'] * self.noc_ns
                children, landing = s['routes'][source, round_id]
                if source in landing:
                    self.push((s['writes'][source, source] - self.platform['endpoint_command_cycles']) * self.core_ns, 'write', name, source, source)
                if children[source]:
                    for flit in range(count):
                        self.push(inject + flit * self.noc_ns, 'inject', name, source, source, flit, round_id)
                    end = inject + count * self.noc_ns
                else:
                    end = start
                self.bound(s, source, end, round_id)
                drained = max(drained, end)
            self.push(drained, 'tx_done', name, source, source)
        s['status'] = 'running'

    def tick(self, cycle):
        for _, message in self.inputs(cycle):
            if isinstance(message, CommRequest):
                self.submit(message)
                continue
            if self.failed:
                continue
            try:
                if not isinstance(message, DataReply) or message.token not in self.tokens:
                    raise ModelError('Unknown communication data reply/token')
                name, core = self.tokens[message.token]
                s, now = self.states[name], cycle*self.core_ns
                entry = s['entries'][core]
                if message.event == 'source_ready':
                    if core in s['staged']:
                        raise ModelError('Duplicate source-read completion')
                    s['staged'].add(core)
                    entry['payload'] = message.detail
                    self.record('source_released', now, phase=name, source=core, bytes=entry['nbytes'])
                elif message.event == 'posted':
                    s['posted'].add(core)
                elif message.event == 'port_ready':
                    s['ports'][core] = message.detail
                elif message.event == 'reserved':
                    s['reserved'].add(core)
                elif message.event == 'written':
                    source, visible_cycle = message.detail
                    pair = source, core
                    expected = {(w['source'], core) for w in entry['expected']}
                    if pair not in expected or (source, core) not in s.get('writes', {}):
                        raise ModelError('Write acknowledgement outside authorized coverage')
                    if visible_cycle < s['visibility'][pair] or visible_cycle > cycle:
                        raise ModelError('Invalid data visibility time')
                    if pair in s['committed']:
                        continue
                    s['committed'].add(pair)
                    size = s['entries'][source]['nbytes']
                    if source != core:
                        s['committed_remote'][core] += size
                    self.record('write_visible', visible_cycle*self.core_ns, phase=name, source=source, node=core, bytes=size, local=source == core)
                    if expected <= s['committed']:
                        expval = sum(w['nbytes'] for w in entry['expected'] if w['source'] != core)
                        if s['committed_remote'][core] != expval:
                            raise ModelError('Receive byte count differs from expVal')
                        self.data(name, core, 'publish')
                elif message.event == 'published':
                    expected = {(w['source'], core) for w in entry['expected']}
                    if not expected <= s['committed'] or core in s['published']:
                        raise ModelError('Premature or duplicate publication')
                    s['published'].add(core)
                    self.record('receive_published', now, phase=name, node=core)
                elif message.event == 'fault':
                    raise ModelError(str(message.detail))
                else:
                    raise ModelError(f'Unknown data reply: {message.event}')
                self.local_complete(name, core)
            except ModelError as exc:
                self.fail(exc)
        if self.failed:
            return
        for name, s in self.states.items():
            members = set(s['entries'])
            try:
                if s['status'] == 'preparing' and s['accepted'] == s['posted'] == s['staged'] == members:
                    for core in sorted(members):
                        self.data(name, core, 'fence')
                    s['status'] = 'fencing'
                if s['status'] == 'fencing' and s['ports'].keys() == members:
                    self.schedule_writes(name)
                if s['status'] == 'installing' and s['reserved'] == members:
                    self.activate(name)
                for core in sorted(s['accepted']):
                    self.local_complete(name, core)
                if s['status'] == 'running' and s['completed'] == members:
                    s['status'] = 'draining'
                if s['status'] == 'draining' and not any(event[3] == name for event in self.heap):
                    s['status'] = 'done'
                    self.active = None
            except ModelError as exc:
                self.fail(exc)

    def advance(self, cycle):
        try:
            return self._advance(cycle)
        except ModelError as exc:
            self.cycle = cycle
            self.fail(exc)
            return []

    def _advance(self, cycle):
        """Return core messages due on this edge, before any core component ticks."""
        due = []
        if self.failed:
            return due
        until = cycle * self.core_ns
        while self.heap and self.heap[0][0] <= until:
            now, _, kind, name, source, node, flit, round_id = heapq.heappop(self.heap)
            self.processed += 1
            self.progress += 1
            if self.processed > 2_000_000:
                raise ModelError('NoC simulator event limit exceeded (not a hardware limit)')
            s = self.states[name]
            entry = s['entries'][source]
            children, landing = s['routes'][source, round_id]
            if kind in ('inject', 'arrive'):
                if kind == 'inject':
                    self.reserve(f'inject:{node}', now, now + self.noc_ns, name, source, flit=flit)
                elif node in landing:
                    self.reserve(f'eject:{node}', now - self.noc_ns, now, name, source, flit=flit)
                    seen = s['coverage'][source, node]
                    if flit in seen:
                        raise ModelError('Duplicate flit on scheduled network')
                    seen.add(flit)
                    if len(seen) == math.ceil(entry['nbytes'] / self.payload):
                        write_cycle = s['writes'][source, node] - self.platform['endpoint_command_cycles']
                        if now + self.p['rx_cdc_cycles'] * self.noc_ns > write_cycle * self.core_ns:
                            raise ModelError('Packet misses reserved Shared write')
                        self.push(write_cycle * self.core_ns, 'write', name, source, node)
                valid = min(self.payload, entry['nbytes'] - flit * self.payload)
                for child in children[node]:
                    resource = f'link:{node}->{child}'
                    arrival = now + self.p['hop_cycles'] * self.noc_ns
                    self.bound(s, source, arrival, round_id)
                    self.reserve(resource, now, now + self.noc_ns, name, source, flit=flit,
                                 round=round_id, payload_bytes=valid, header_bytes=self.p['header_bytes'],
                                 padding_bytes=self.payload - valid)
                    self.links[resource].update(flits=1, wire_bytes=self.p['link_bytes_per_cycle'], payload_bytes=valid)
                    self.push(arrival, 'arrive', name, source, child, flit, round_id)
            elif kind == 'write':
                endpoint, command = self.command(name, node, 'write',
                    (source, entry['destination_offset'], entry['nbytes'], entry.get('payload')))
                due.append((endpoint, command, now))
            elif kind == 'tx_done':
                s['tx_done'].add(source)
                self.record('tx_drained', now, phase=name, source=source)
                # Local completion is emitted from tick only after publication too.
        return due

    def is_idle(self):
        return not self.failed and super().is_idle() and not self.heap and all(s['status'] == 'done' for s in self.states.values())

    def next_event(self):
        return self.heap[0][0] if self.heap else None

    def snapshot(self):
        return dict(super().snapshot(), network_events=len(self.heap), phases={name: dict(
            status=s['status'], posted=sorted(s['posted']), staged=sorted(s['staged']),
            published=sorted(s['published']), tx_done=sorted(s['tx_done'])) for name, s in self.states.items()})

    def report(self):
        return dict(interface='CommUnit request/data/completion v1', version=self.installation['version'], drained=self.drained(), schedule={name: [dict(source=source, round=index, **transmission)
                                    for source, entry in sorted(s['entries'].items())
                                    for index, transmission in enumerate(transmissions(entry, self.p))]
                              for name, s in self.states.items()},
                    events=sorted(self.events, key=lambda e: (e['time_ns'], e['kind'], str(e))),
                    links={k: dict(v) for k, v in sorted(self.links.items())},
                    adapter_reserved_peak_bytes=dict(self.buffer_required),
                    phases={name: dict(status=s['status'], remote_committed_bytes={c: s['committed_remote'][c] for c in s['entries']})
                            for name, s in self.states.items()},
                    assumptions=['WSE40 mesh; one source packet per participant and phase; phases drain serially.',
                                 'One flit per NoC cycle per injection/ejection/directed link; parallel multicast.',
                                 'Automatic plans overlap trees with disjoint resource intervals; buffered receives queue for Shared writes.',
                                 'Whole-packet TX/RX buffers provisioned from the plan; hardware depth is pending.',
                                 'Shared writes reserve service windows; ordinary writes use gaps (pending).',
                                 'Simulator source/segment identities are metadata; on-wire encoding remains pending.',
                                 'Header size is a provisional baseline, not proof of complete address/identity encoding.',
                                 'One active collective domain; retryable admission backpressure; fixed plan-sized TX/RX reservation.',
                                 'Local success means TX drained and destination published; not global drain.',
                                 'Faults quarantine in-flight resources; no automatic recovery or CalReg reinstallation.'])
