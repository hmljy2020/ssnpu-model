"""Compatibility exports; shared contracts live in runtime.communication."""
from runtime.communication import (DataRange, CommRequest, Accepted, Rejected, Completion,
                                   DataCommand, DataReply, digest, request_from_dict)
from config import platform_spec
