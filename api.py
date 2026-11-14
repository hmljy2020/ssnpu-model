"""Public simulation entry point composing configuration, trace and Simulator."""
from __future__ import annotations

import copy
import json
from pathlib import Path

from config import default_config, validate_config, wse_config
from errors import ModelError


def simulate(capture_dir, config=None, *, communication_plan=None):
    """Replay a static tile trace directory and return a JSON-serializable report."""
    from simulator import Simulator
    from frontend.capture import TraceAdapter

    if config is None:
        config = wse_config() if communication_plan is not None else default_config()
    elif isinstance(config, (str, Path)):
        config = json.loads(Path(config).read_text())
    config = validate_config(copy.deepcopy(config))
    try:
        trace = TraceAdapter(capture_dir, config, communication_plan)
        return Simulator(trace, config).run()
    except (KeyError, TypeError, OSError, json.JSONDecodeError) as exc:
        raise ModelError(f'Invalid capture: {exc}') from exc


def simulate_core(capture_dir, config=None, *, comm_backend=None):
    """Replay a core workload with an optional injected message-only backend."""
    from simulator import Simulator
    from frontend.capture import TraceAdapter
    if config is None:
        config = default_config()
    elif isinstance(config, (str, Path)):
        config = json.loads(Path(config).read_text())
    config = validate_config(copy.deepcopy(config))
    try:
        trace = TraceAdapter(capture_dir, config)
        return Simulator(trace, config, core_only=True, comm_backend=comm_backend).run()
    except (KeyError, TypeError, OSError, json.JSONDecodeError) as exc:
        raise ModelError(f'Invalid capture: {exc}') from exc
