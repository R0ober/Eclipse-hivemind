"""Real installed-library probe; no DHT network or training is started."""
import inspect
import importlib.metadata
import json
import hashlib
from pathlib import Path
import sys
from unittest.mock import Mock

sys.path.insert(0, "/app")
import hivemind
import torch
from hivemind.averaging import averager as module
from hivemind.averaging.matchmaking import Matchmaking
from shared.eclipse_averaging import install_hooks
from factorial.runtime import FactorialRuntime, AuditedAverager
from factorial.trace import Trace
from probe_factorial_shutdown import probe as probe_shutdown

assert hivemind.__version__ == "1.1.12"
native = Matchmaking
install_hooks(Mock(), {}, "probe_grad_averager.0b")
observed = module.Matchmaking
for name in ("_check_reasons_to_reject", "rpc_join_group", "_request_join_potential_leaders", "leader_disband_group"):
    assert getattr(observed, name) is getattr(native, name), name
assert "kwargs" in inspect.signature(AuditedAverager._aggregate_with_group).parameters
model = torch.nn.Linear(2, 1)
for parameter in model.parameters():
    parameter.grad = torch.ones_like(parameter)
runtime = object.__new__(FactorialRuntime)
runtime.transform_calls = 0
runtime.reverse_gradients(model)
assert all(torch.equal(parameter.grad, -torch.ones_like(parameter)) for parameter in model.parameters())
assert runtime.transform_calls == 1
trace = Trace(dict(experiment_id="probe", node_id="probe", node_type="probe", node_index=0), "http://unused")
from unittest.mock import patch
with patch("factorial.trace.client.send_eclipse_observations", side_effect=lambda **kw: {"accepted": len(kw["observations"]), "duplicates": 0}):
    trace.emit("probe")
    trace.close()
shutdown = probe_shutdown()
embedded = {"src/eclipse_hivemind/nodes/honest/runner.py": hashlib.sha256(Path("/app/honest_runner.py").read_bytes()).hexdigest()}
for folder in ("shared", "coordinated_attacker", "factorial"):
    for path in (Path("/app") / folder).glob("*.py"):
        embedded[f"src/eclipse_hivemind/nodes/{folder}/{path.name}"] = hashlib.sha256(path.read_bytes()).hexdigest()
print(json.dumps(dict(complete=True, hivemind_version=hivemind.__version__,
                     native_uncoordinated_matchmaking=True, inversion_checked=True,
                     python=sys.version, packages={d.metadata['Name']: d.version for d in importlib.metadata.distributions()},
                     transport_version=Trace.TRANSPORT_VERSION, shutdown_recovery=shutdown,
                     embedded_sources=embedded)))
