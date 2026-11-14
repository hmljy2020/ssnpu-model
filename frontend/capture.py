"""Capture loading, dependency validation, barriers, and version lifetimes."""
from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from pathlib import Path

from errors import ModelError
from records import Instruction, Version
from tiles import nbytes, version_key


class TraceAdapter:
    """Capture v2 / SSNPU v1 boundary; order edges only establish age."""
    def __init__(self, capture_dir, config, communication_plan=None):
        self.path, self.config = Path(capture_dir), config
        self.instructions, self.versions = {}, {}
        self.source_hashes = {}
        self.block_members = defaultdict(set)
        manifest = self.read('manifest.json')
        if manifest.get('format_version') != 2 or manifest.get('ssnpu_spec_version') != 1:
            raise ModelError('Expected capture format_version=2, ssnpu_spec_version=1')
        if manifest.get('complete') is not True:
            raise ModelError('Capture is incomplete')
        self.capture_config = self.read(manifest['config_file']) if manifest.get('config_file') else {}
        self.objects = self.read(manifest['objects_file'])
        self.tasks = self.read(manifest['tasks_file'])
        events = self.lines(manifest['events_file'])
        instances = manifest['instances']
        seen_threads = set()
        for instance in instances:
            if instance.get('status') != 'completed':
                raise ModelError(f'Incomplete instance: {instance}')
            core, pe = instance['mapping']['core_id'], instance['mapping']['pe_id']
            if not 0 <= pe < config['parameters']['pe_count']['value']:
                raise ModelError(f'PE mapping outside configuration: {pe}')
            self.block_members[instance['task_id'], instance['block_id']].add((core, pe))
            identity = tuple(instance[k] for k in ('task_id', 'block_id', 'subblock_id', 'thread_id'))
            if identity in seen_threads:
                raise ModelError(f'Duplicate thread {identity}')
            seen_threads.add(identity)
            records = self.lines(instance['trace_file'])
            if len(records) != instance['operation_count']:
                raise ModelError(f'Operation count mismatch: {identity}')
            previous = None
            for sequence, r in enumerate(records):
                if tuple(r[k] for k in ('task_id', 'block_id', 'subblock_id', 'thread_id')) != identity:
                    raise ModelError(f'Thread identity mismatch: {r["op_id"]}')
                if r['instance_op_sequence'] != sequence:
                    raise ModelError(f'Non-contiguous sequence: {r["op_id"]}')
                if r.get('order_dependencies', []) != ([] if previous is None else [previous]):
                    raise ModelError(f'Unsupported order_dependencies contract at {r["op_id"]}')
                # order_dependencies 只校验线程原始顺序，不加入 deps；否则独立 tile 指令也会串行。
                previous = r['op_id']
                r['_identity'] = identity
                self.add(Instruction(r['op_id'], core, pe, r['task_id'], r['block_id'],
                                     r['instance_op_sequence'], r,
                                     set(r['dependencies']), set(r.get('sync_dependencies', [])),
                                     alias=r['ir_op'] in {'tile.transpose_view', 'tile.reshape', 'tile.reshape_view', 'tile.view'}))
        real = list(self.instructions.values())
        self._barriers(events, instances, real)
        self.calendar_phases = []
        if communication_plan is not None:
            from frontend.communication import attach_plan
            attach_plan(self, communication_plan)
            real = [i for i in self.instructions.values() if not i.control]
        self._versions(real)
        self._tasks(real)
        self._validate_graph()

    def read(self, name):
        path = (self.path / name).resolve()
        if not path.is_relative_to(self.path.resolve()):
            raise ModelError(f'Capture path escapes directory: {name}')
        data = path.read_bytes()
        self.source_hashes[name] = hashlib.sha256(data).hexdigest()
        return json.loads(data)

    def lines(self, name):
        path = (self.path / name).resolve()
        if not path.is_relative_to(self.path.resolve()):
            raise ModelError(f'Capture path escapes directory: {name}')
        data = path.read_bytes()
        self.source_hashes[name] = hashlib.sha256(data).hexdigest()
        return [json.loads(line) for line in data.splitlines() if line.strip()]

    def add(self, i):
        if i.id in self.instructions:
            raise ModelError(f'Duplicate instruction: {i.id}')
        self.instructions[i.id] = i

    def _versions(self, real):
        # view 解析到同一版本；Local/Shared 都以动态版本建立生产者和消费者。
        # 这些是静态元数据；运行时的 allocated/ready/释放状态由 TileReg 独立维护。
        aliases = {}
        for i in real:
            if i.alias:
                if len(i.raw['inputs']) != 1:
                    raise ModelError(f'View requires one source: {i.id}')
                for o in i.raw['outputs']:
                    aliases[version_key(o)] = version_key(i.raw['inputs'][0])
        def canonical(key):
            visited = set()
            while key in aliases and aliases[key] != key:
                if key in visited:
                    raise ModelError(f'Alias cycle: {key}')
                visited.add(key)
                key = aliases[key]
            return key
        for i in real:
            if i.alias:
                continue
            for o in i.raw['outputs']:
                if o['kind'] != 'tile':
                    continue
                key = canonical(version_key(o))
                shared = o['memory_space'] == 'SharedTileReg'
                if o['memory_space'] not in ('SharedTileReg', 'LocalTileReg'):
                    raise ModelError(f'Unsupported tile memory: {o["memory_space"]}')
                pool = f'core:{i.core}/shared' if shared else f'core:{i.core}/pe:{i.pe}/local'
                if key in self.versions:
                    raise ModelError(f'Duplicate version producer: {key}')
                self.versions[key] = Version(key, o, i.id, pool, nbytes(o, o['shape']), retained=shared)
                i.outputs.add(key)
        uses = defaultdict(list)
        for i in real:
            for o in i.raw['inputs']:
                producer = o.get('producer')
                if producer is not None and producer != i.id:
                    i.deps.add(producer)
                if o.get('view_producer') is not None:
                    i.deps.add(o['view_producer'])
                if o['kind'] != 'tile':
                    continue
                key = canonical(version_key(o))
                if key not in self.versions:
                    raise ModelError(f'Missing producer for tile {key} at op {i.id}')
                v = self.versions[key]
                if v.pool.split('/')[0] != f'core:{i.core}' or ('/local' in v.pool and v.pool != f'core:{i.core}/pe:{i.pe}/local'):
                    raise ModelError(f'Tile accessed outside owning Core/PE: {key}')
                i.inputs.add(key)
                i.operands.append((o, key))
                i.deps.add(v.producer)
                # 登记所有未来读者，避免当前无人读取时就提前释放共享版本。
                v.consumers.add(i.id)
            for use in i.raw.get('ssnpu', {}).get('shared_inputs', []):
                uses[(use['use_id'], use['operand'])].append((i, use))
        terminal_uses = defaultdict(list)
        version_uses = defaultdict(list)
        for group, entries in uses.items():
            first = entries[0][1]
            if any(u != first for _, u in entries) or sorted(i.raw['thread_id'] for i, _ in entries) != sorted(first['participants']):
                raise ModelError(f'Shared-use participant mismatch: {group}')
            key = ('shared', first['version_id'])
            if key not in self.versions or any(key not in i.inputs for i, _ in entries):
                raise ModelError(f'Invalid shared-use version: {group}')
            version_uses[key].append(entries)
            if first['retention'] == 'last-use':
                # last-use 允许回收；运行时仍须等待该版本的全部消费者，而非仅最后一组。
                terminal_uses[key].append(entries)
                self.versions[key].retained = False
            elif first['retention'] != 'reuse':
                raise ModelError(f'Unknown retention: {first}')
        for key, terminals in terminal_uses.items():
            if len(terminals) != 1:
                raise ModelError(f'Multiple last-use groups: {key}')
            terminal = terminals[0]
            for entries in version_uses[key]:
                if entries is terminal:
                    continue
                for i, _ in entries:
                    if not any(i.raw['ssnpu']['phase'] < end.raw['ssnpu']['phase'] or
                               (i.raw['_identity'] == end.raw['_identity'] and i.age < end.age)
                               for end, _ in terminal):
                        raise ModelError(f'Read after last-use or unordered lifetime: {key}')

    def _barriers(self, events, instances, real):
        # 把多线程同一轮 barrier 合并为一个控制节点，交给 Sync 执行。
        # 前序 tile → barrier → 后续阶段；barrier 间也连边，空阶段不会丢失同步约束。
        groups = defaultdict(list)
        for e in events:
            if e['kind'] == 'ssnpu.barrier':
                groups[(e['task_id'], e['block_id'], e['parameters']['round'])].append(e)
        blocks = {(x['task_id'], x['block_id']) for x in instances}
        for task, block in sorted(blocks):
            threads = [x['thread_id'] for x in instances if (x['task_id'], x['block_id']) == (task, block)]
            rows = [i for i in real if (i.task, i.block) == (task, block)]
            rounds = sorted(r for t, b, r in groups if (t, b) == (task, block))
            if rounds != list(range(len(rounds))):
                raise ModelError(f'Non-contiguous barrier rounds: {task}/{block}')
            for r in rounds:
                es = groups[task, block, r]
                p = es[0]['parameters']
                if sorted(e['thread_id'] for e in es) != sorted(threads) or any(
                        e['parameters']['barrier_id'] != p['barrier_id'] or
                        sorted(e['parameters']['participants']) != sorted(threads) for e in es):
                    raise ModelError(f'Barrier participants/site mismatch: {task}/{block}/{r}')
                ids = {i.id for i in rows if i.raw['ssnpu']['phase'] <= r}
                for e in es:
                    if set(e.get('completion_dependencies', [])) != ids:
                        raise ModelError(f'Barrier completion dependencies mismatch: {e["event_id"]}')
                bid = f'barrier:{task}:{block}:{r}'
                deps = ids | ({f'barrier:{task}:{block}:{r-1}'} if r else set())
                owner = next(x for x in instances if (x['task_id'], x['block_id']) == (task, block))
                self.add(Instruction(bid, owner['mapping']['core_id'], 0, task, block, r,
                                     {'ir_op': 'barrier'}, deps=deps, control=True))
            for i in rows:
                phase = i.raw['ssnpu']['phase']
                if type(phase) is not int or not 0 <= phase <= len(rounds):
                    raise ModelError(f'Invalid/missing barrier phase at {i.id}')
                if phase:
                    i.sync.add(f'barrier:{task}:{block}:{phase-1}')

    def _tasks(self, real):
        # 每个 task 增加完成节点；跨 task 依赖最终由跨 Core 的 ROB Completed 广播唤醒。
        task_ids = {t['task_id'] for t in self.tasks}
        if any(i.task not in task_ids for i in real):
            raise ModelError('Instruction references missing task')
        for t in self.tasks:
            tid = t['task_id']
            rows = [i for i in self.instructions.values() if i.task == tid]
            if t.get('status') not in ('completed', 'skipped'):
                raise ModelError(f'Incomplete task {tid}')
            for dep in t.get('dependencies', []):
                if dep not in task_ids:
                    raise ModelError(f'Missing task dependency {dep}')
                for i in rows:
                    i.sync.add(f'task:{dep}')
            owner = next((i for i in rows), None)
            self.add(Instruction(f'task:{tid}', owner.core if owner else 0, 0, tid, -1, 0,
                                 {'ir_op': 'task_complete'},
                                 deps={i.id for i in rows} | {f'task:{d}' for d in t.get('dependencies', [])},
                                 control=True))

    def _validate_graph(self):
        edges = {k: i.deps | i.sync for k, i in self.instructions.items()}
        followers = defaultdict(list)
        counts = {}
        for key, deps in edges.items():
            missing = deps - edges.keys()
            if missing:
                raise ModelError(f'Missing dependencies at {key}: {missing}')
            counts[key] = len(deps)
            for d in deps:
                followers[d].append(key)
        ready = [k for k, n in counts.items() if not n]
        visited = 0
        while ready:
            visited += 1
            for k in followers[ready.pop()]:
                counts[k] -= 1
                if counts[k] == 0:
                    ready.append(k)
        if visited != len(edges):
            raise ModelError(f'Dependency cycle involving {[k for k, n in counts.items() if n][:20]}')
