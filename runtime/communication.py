"""Immutable request/data/completion contract shared by independent models."""
from dataclasses import dataclass
import hashlib
import json


def digest(value):
    # Normalize integer object keys before sorting, so JSON round-trips preserve
    # the digest even when nodes include both single- and two-digit ids.
    normalized = json.loads(json.dumps(value, allow_nan=False))
    encoded = json.dumps(normalized, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class DataRange:
    handle: tuple  # Opaque owner-issued binding + generation; WSE never decodes it.
    offset: int
    nbytes: int
    capacity: int


@dataclass(frozen=True)
class CommRequest:
    token: str
    endpoint: str
    node: int
    operation: str
    epoch: int
    domain: str
    members: tuple
    version: str
    route_bits: tuple
    source: DataRange
    destination: DataRange
    destination_offset: int


@dataclass(frozen=True)
class Accepted:
    token: str


@dataclass(frozen=True)
class Rejected:
    token: str
    reason: str
    retryable: bool


@dataclass(frozen=True)
class Completion:
    token: str
    status: str
    error: str = ''
    safe_to_release: bool = True


@dataclass(frozen=True)
class DataCommand:
    token: str
    kind: str
    region: DataRange
    detail: object = None


@dataclass(frozen=True)
class DataReply:
    token: str
    event: str
    detail: object = None


def request_from_dict(data):
    data = dict(data)
    for key in ('source', 'destination'):
        region = dict(data[key])
        region['handle'] = tuple(region['handle'])
        data[key] = DataRange(**region)
    data['members'], data['route_bits'] = tuple(data['members']), tuple(data['route_bits'])
    return CommRequest(**data)

