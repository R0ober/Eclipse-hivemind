"""Real installed-library probe; no DHT network or training is started."""
import inspect
import importlib.metadata
import json
import hashlib
from pathlib import Path
import sys
from unittest.mock import Mock, AsyncMock, patch
import asyncio
import os
from contextlib import redirect_stdout
from io import StringIO
from types import SimpleNamespace
from hivemind.dht.node import DHTNode
from hivemind.dht.protocol import DHTProtocol
from hivemind.dht.routing import DHTID

sys.path.insert(0, "/app")
import factorial.runtime as runtime_module
import hivemind
import torch
from hivemind.averaging import averager as module
from hivemind.averaging.matchmaking import Matchmaking
from shared.eclipse_averaging import install_hooks
from factorial.runtime import FactorialRuntime, AuditedAverager
from factorial.trace import Trace
from probe_factorial_shutdown import probe as probe_shutdown

assert hivemind.__version__ == "1.1.12"
assert Trace.DRAIN_MAX_TIMEOUT_SECONDS == 1800
assert Trace.TRANSPORT_VERSION == "process_spool_compact_v1"
assert Trace.DRAIN_IDLE_TIMEOUT_SECONDS == Trace.CLOSE_TIMEOUT_SECONDS == 120
assert inspect.signature(DHTNode.create).parameters["bucket_size"].default == 20

async def native_bucket_probe():
    # Keep the real native DHTNode/Protocol/routing/cache initialization. Only
    # disable registration of P2P handlers; no network or training is started.
    result = []
    with patch.object(DHTProtocol, "add_p2p_handlers", new=AsyncMock()), \
         patch.object(DHTProtocol, "remove_p2p_handlers", new=AsyncMock()):
        for k in (20, 5):
            node = await DHTNode.create(p2p=SimpleNamespace(peer_id="offline-probe"),
                                       node_id=DHTID(17), bucket_size=k, num_replicas=5)
            assert node.protocol.bucket_size == node.protocol.routing_table.bucket_size == k
            assert node.num_replicas == 5
            assert node.cache_locally and node.cache_on_store and node.cache_nearest == 1
            result.append(dict(bucket_size=k, replicas=node.num_replicas, native_cache_defaults=True))
            await node.shutdown()
    return result

native_buckets = asyncio.run(native_bucket_probe())
for role in ("normal", "victim", "recruiter", "support"):
    runtime_probe = object.__new__(FactorialRuntime)
    runtime_probe.settings = SimpleNamespace(replicas=5, placement="random")
    runtime_probe.trace = Mock()
    runtime_probe.owner = role in {"recruiter", "support"}
    runtime_probe.coordination = False
    runtime_probe.role = role
    dht = Mock()
    dht.run_coroutine.return_value = {"bucket_size": 5}
    with patch.dict(os.environ, {"PARAM_BUCKET_SIZE": "5"}), \
         patch.object(runtime_module, "start_dht", return_value=dht) as start:
        assert runtime_probe.start_dht() is dht
        assert start.call_args.kwargs["bucket_size"] == 5
        assert start.call_args.kwargs["num_replicas"] == 5
        assert start.call_args.kwargs["shutdown_timeout"] == 30

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
with patch("factorial.trace.client.send_eclipse_observations", side_effect=lambda **kw: {"accepted": len(kw["observations"]), "duplicates": 0}), redirect_stdout(StringIO()):
    trace.emit("probe")
    for _ in range(100_000):
        trace.emit("response_filtered", retained=[], removed=["peer"])
    trace.close()
assert trace._received_counts["response_filtered"] == 100_000
assert trace._received_summaries < 10
# Fork after the parent's collector/lock exists, as native child processes do.
import multiprocessing
fork_trace = Trace(dict(experiment_id="compact-fork-probe", node_id="support-0", node_type="support", node_index=0), "http://unused")
fork_trace.emit("dht_response", records=[])
received = []
def collect(**kwargs):
    received.extend(kwargs["observations"])
    return {"accepted": len(kwargs["observations"]), "duplicates": 0}
def producer():
    fork_trace.producer_started()
    for _ in range(1000):
        fork_trace.emit("response_filtered", retained=[], removed=["peer"])
    fork_trace.producer_finished()
with patch("factorial.trace.client.send_eclipse_observations", side_effect=collect), redirect_stdout(StringIO()):
    fork_trace.start()
    fork_trace._compact_lock.acquire()
    process = multiprocessing.get_context("fork").Process(target=producer)
    process.start()
    fork_trace._compact_lock.release()
    fork_trace.register_process(process)
    fork_trace.close()
assert process.exitcode == 0
assert received[-1]["compacted_events_by_kind"] == {"dht_response": 1, "response_filtered": 1000}
assert received[-1]["producer_count"] == 2
with redirect_stdout(StringIO()):
    shutdown = probe_shutdown()
embedded = {"src/eclipse_hivemind/nodes/honest/runner.py": hashlib.sha256(Path("/app/honest_runner.py").read_bytes()).hexdigest()}
for folder in ("shared", "coordinated_attacker", "factorial"):
    for path in (Path("/app") / folder).glob("*.py"):
        origin = "factorial2_compact" if folder == "factorial" and path.name in {"trace.py", "trace_full.py", "compact.py"} else "factorial2" if folder == "factorial" and path.name == "runtime.py" else folder
        embedded[f"src/eclipse_hivemind/nodes/{origin}/{path.name}"] = hashlib.sha256(path.read_bytes()).hexdigest()
print(json.dumps(dict(complete=True, hivemind_version=hivemind.__version__,
                     native_uncoordinated_matchmaking=True, inversion_checked=True,
                     native_bucket_configuration=native_buckets, bucket_forwarded_on_all_roles=True,
                     python=sys.version, packages={d.metadata['Name']: d.version for d in importlib.metadata.distributions()},
                     transport_version=Trace.TRANSPORT_VERSION, shutdown_recovery=shutdown,
                     embedded_sources=embedded)))
