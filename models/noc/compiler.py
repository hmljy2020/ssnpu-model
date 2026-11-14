"""Joint route/Calendar compiler, independent of core instructions and bindings."""
import copy
import math
from bisect import bisect_left, insort
from collections import defaultdict
from fractions import Fraction

from runtime.communication import digest
from models.noc.routing import node_id, integer, multicast_routes, route_tree, transmissions, receive_offset
from errors import ModelError


def reserve_packet(calendar, uses, flits):
    """Find the first start with space on every resource at its relative offset."""
    start = 0
    while True:
        earliest = start
        for resource, offset in uses:
            intervals = calendar[resource]
            begin = start + offset
            index = bisect_left(intervals, (begin,))
            if index and intervals[index - 1][1] > begin:
                earliest = max(earliest, intervals[index - 1][1] - offset)
            elif index < len(intervals) and intervals[index][0] < begin + flits:
                earliest = max(earliest, intervals[index][1] - offset)
        if earliest == start:
            for resource, offset in uses:
                insort(calendar[resource], (start + offset, start + offset + flits))
            return start
        start = earliest


def compile_phase(phase, platform):
    """Place packet trains on the NoC and bound the queued destination writes."""
    if phase.get('routing') != 'auto':
        if 'routing' in phase:
            raise ModelError('Unknown routing mode')
        return
    parameters = platform['network']
    payload = parameters['link_bytes_per_cycle'] - parameters['header_bytes']
    participants = {node_id(entry['core']) for entry in phase['nodes']}
    write = platform['write_service']
    core_cycle = Fraction(parameters['noc_clock_hz'], parameters['core_clock_hz'])
    tx, hop, guard = (parameters[key] for key in ('tx_cdc_cycles', 'hop_cycles', 'guard_cycles'))
    calendar, writes = defaultdict(list), defaultdict(list)
    for entry in sorted(phase['nodes'], key=lambda item: item['core']):
        if any(field in entry for field in ('routes', 'route_bits', 'window')):
            raise ModelError('Automatic routing takes destinations, not precompiled routes/windows')
        destinations = entry.get('destinations', sorted(participants))
        if not set(destinations) <= participants:
            raise ModelError('Route lands outside registered participants')
        size = integer(entry['nbytes'], 'nbytes', 1)
        flits = math.ceil(size / payload)
        service = math.ceil(size / write['throughput']) * core_cycle
        source = entry['core']
        entry['routes'] = []
        for bits in multicast_routes(source, destinations):
            children, landing, distance = route_tree(source, bits)
            uses = [(('link', node, child), tx + distance[node] * hop)
                    for node, peers in children.items() for child in peers]
            if children[source]:
                uses.append((('inject', source), tx))
            uses.extend((('eject', node), tx + distance[node] * hop - 1)
                        for node in landing if node != source)
            start = reserve_packet(calendar, uses, flits)
            hops = max(distance.values())
            network_tail = tx + flits - 1 + hops * hop if hops else 0
            route = dict(route_bits=bits, start_cycle=start, end_cycle=start + network_tail + guard)
            entry['routes'].append(route)
            for node in landing:
                ready = start + receive_offset(distance[node], flits, parameters)
                writes[node].append((ready, service, route))

    # The receiver buffers whole packets while its Shared write port is busy.
    # Round-up to the core clock adds less than one core cycle to the queue tail.
    endpoint = platform['endpoint_command_cycles'] * core_cycle
    visibility = (1 + write['latency']) * core_cycle
    for requests in writes.values():
        requests.sort(key=lambda request: request[0])
        ready_times, finishes = [], []
        finish = 0
        for ready, service, _ in requests:
            finish = max(finish, ready + endpoint) + service
            ready_times.append(ready)
            finishes.append(finish)
        for ready, _, route in requests:
            # Requests that round to the same core edge can write in either order.
            last_tie = bisect_left(ready_times, ready + core_cycle) - 1
            end = math.ceil(finishes[last_tie] + visibility) + guard
            route['end_cycle'] = max(route['end_cycle'], end)


def make_installation(phases, platform):
    """WSE-side configuration contains no instruction ids or Tile descriptors."""
    calreg = {}
    for phase in phases:
        entries = {}
        for entry in phase['nodes']:
            routes = transmissions(entry, platform['network'])
            entries[entry['core']] = dict(
                windows=[{k: route[k] for k in ('start_cycle', 'end_cycle')} for route in routes],
                route_digest=digest([route['route_bits'] for route in routes]),
                nbytes=entry['nbytes'], source_offset=entry.get('source_offset', 0),
                destination_offset=entry['destination_offset'], expected=copy.deepcopy(entry['expected']))
        calreg[phase['name']] = dict(opcode=phase['opcode'], epoch=phase['epoch'], entries=entries)
    body = dict(schema='wse-communit/1', platform_hash=digest(platform), calreg=calreg)
    return dict(body, version=digest(body))
